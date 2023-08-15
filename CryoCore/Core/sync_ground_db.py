#!/usr/bin/env python
from CryoCore.Core.loggingService import getLoggingService
from CryoCore.Core import API

try:
    import psycopg2 as postgre
except:
    getLoggingService("Importing").critical("WARNING: postgres bindings not installed")

from CryoCore.Core.common import Singleton
from CryoCore.Core.dbHandler import DbHandler
import logging
import threading as threading
import _thread


class SyncDB(Singleton):
    # Units: second
    period = 60 * 5

    ignoreSubsequent = True

    onboard_con = None
    onboard_aux_con = None
    ground_con = None

    def __init__(self, stop_event=None):
        self.log = API.get_log("SyncDB")
        self.cfg = API.get_config("System.Status")

        if stop_event:
            self.stop_event = stop_event
        else:
            self.stop_event = threading.Event()

        self._connections = {}
        self._lock = threading.Lock()

        # Must connect to the local database
        self._db_conn = postgre.connect(host=self.cfg["GrounDB.db_name"],
                                        database=self.cfg["GrounDB.db_name"],
                                        port=self.cfg["GrounDB.db_name"],
                                        user=self.cfg["GroundDB.db_user"],
                                        password=self.cfg["GroundDB.db_password"])
        self._prepare_ground_db()

        self._is_onboard_prepared = False
        self._connector = threading.Thread(target=self._run_connector)
        self._connector.start()

    def _run_connector(self):
        """
        Entrypoint for the connector thread, which will loop and try to
        keep connections up.  Only connections that are up are put
        in the self._connections, and if one channel is down, it will
        try to get it back up.
        """
        last_run = 0
        while not self.stop_event.is_set():
            for conn in ["fast", "slow"]:
                if conn not in list(self._connections.keys())[:]:
                    # Try to connect!
                    try:
                        addr = (self.cfg["SyncStatus.%s_host" % conn],
                                self.cfg["SyncStatus.%s_port" % conn])

                        if addr[0] or addr[1] is None:
                            self.log.error("Missing config for connection %s" % conn)
                            continue

                        c = postgre.connect(host=addr[0],
                                            database=self.cfg["Postgres.db_name"],
                                            port=addr[1],
                                            user=self.cfg["Postgres.db_user"],
                                            password=self.cfg["Postgres.db_password"])
                        with self.lock:
                            self.log.info("Adding connection over %s link" % conn)
                            self._connections[conn] = c

                    except:
                        self.log.debug("Could not connect to database using %s connection" % conn)

            # Wait a bit until we try again
            interval = float(self.cfg["run_interval"])
            while not self.stop_event.is_set():
                now = time.time()
                if now - last_run >= interval:
                    break
                time.sleep(min(1, last_run + interval - now))
            last_run = time.time()

    def _prepare_ground_db(self):
        """
        Prepare the ground database if it isn't already present
        """
        # Check whether the ground database, the one to be populated, has been well built
        ground_cursor = self._db_conn.cursor()

        ground_cursor.execute("select table_name from information_schema.tables")
        self._db_conn.commit()

        existing_tables = []

        for row in ground_cursor.fetchall():
            existing_tables.append(row[0])

        if "status" not in existing_tables:
            ground_cursor.execute("CREATE TABLE status ( " +
                                "id SERIAL PRIMARY KEY, " +
                                "timestamp FLOAT, " +
                                "inserted TIMESTAMP, " +
                                "onboard_id INTEGER, " +
                                "channel VARCHAR(256), " +
                                "name VARCHAR(128), " +
                                "value VARCHAR(128))")
            ground_cursor.execute("CREATE INDEX stat_name ON status(name)")
            ground_cursor.execute("CREATE INDEX stat_channel ON status(channel)")
            ground_cursor.execute("CREATE INDEX stat_time ON status(timestamp)")
            self._db_conn.commit()

        ground_cursor.close()

    def _lost_connection(self, connection_name):
        """
        Lost a connection
        """
        self.log.warning("Lost connection to remote database over %s connection" % connection_name)
        with self.lock:
            del self._connections[connection_name]

    def _prepare_onboard(self):
        """
        Prepare the onboard database if it does not already exist
        """
        for conn in ["fast", "slow"]:
            if conn in self._connections:
                try:
                    onboard_cursor = self._connections[conn].cursor()
                    onboard_cursor.execute("select table_name from information_schema.tables where table_name = 'sync_status_var'")

                    if len(onboard_cursor.fetchall()) == 0:
                        onboard_cursor.execute("CREATE TABLE sync_status_var (" +
                                                   "channel VARCHAR(128), " +
                                                   "name VARCHAR(128), " +
                                                   "PRIMARY KEY (channel, name))")
                        self._connections[conn].commit()
                    else:
                        onboard_cursor.execute("delete from sync_status_var")
                        self._connections[conn].commit()

                    # TODO: It would be good if there is a list of variables to be watched on the configuration file, so it might be load into this table here
                    # Let's save the prepared sql sentence
                    onboard_cursor.execute("select name from pg_prepared_statements where name = 'last_values'")

                    if len(onboard_cursor.fetchall()) == 0:
                        onboard_cursor.execute("prepare last_values as " +
                                                "select distinct on (channel, name) " +
                                                "status.id, status.name as name, timestamp, value, " +
                                                "status.channel as channel from status " +
                                                "inner join sync_status_var on " +
                                                "status.name = sync_status_var.name and " +
                                                "status.channel = sync_status_var.channel " +
                                                "order by channel, name, timestamp desc, id desc")

                    self._connections[conn].commit()

                    onboard_cursor.execute("select name from pg_prepared_statements where name = 'IMU_Lon_and_Lat'")
                    if len(onboard_cursor.fetchall()) == 0:
                        onboard_cursor.execute("prepare IMU_Lon_and_Lat as " +
                                                   "select distinct on (name) " +
                                                   "id, name, timestamp, value " +
                                                   "from status where " +
                                                   "channel = 'IMU' and " +
                                                   "name = 'position.lon' or name = 'position.lat' " +
                                                   "order by name, timestamp desc, id desc")
                    self._connections[conn].commit()
                except:
                    self._lost_connection(conn)

        # We update the list of available variables
        self.resetVariableList()

        return True

    def resetVariableList(self):
        ground_cursor = self._db_conn.cursor()

        if self.createVariableList():
            # The table exists, we will reset it and will fill it again
            ground_cursor.execute("delete from status_var")

        # This table lets us know which web browser session is watching which variable
        ground_cursor.execute("select table_name from information_schema.tables where table_name = 'session_status_var'")

        if len(ground_cursor.fetchall()) == 0:
            ground_cursor.execute("CREATE TABLE session_status_var (" +
                                    "status_var_id INTEGER REFERENCES status_var (id) ON DELETE CASCADE, " +
                                    "session_id INTEGER, " +
                                    "PRIMARY KEY (status_var_id, session_id))")
        else:
            # If the table already exists, due to we will populate again the status_var table, and the indexes will change as well, we have to delete all these elements
            ground_cursor.execute("delete from session_status_var")

            self.db_conn_con.commit()

        # Populate the recently created table
        self.updateVariableList()

        onboard_cursor.close()

    def createVariableList(self):
        if not self.ground_con:
            return False

        ground_cursor = self.ground_con.cursor()

        # We create a table with a list of available variables
        ground_cursor.execute("select table_name from information_schema.tables where table_name = 'status_var'")
        if len(ground_cursor.fetchall()) == 0:
            ground_cursor.execute("CREATE TABLE status_var ( " +
                                "id SERIAL PRIMARY KEY, " +
                                "channel VARCHAR(128), " +
                                "name VARCHAR(128), " +
                                "UNIQUE (channel, name))")
            self.ground_con.commit()

        return True

    def updateVariableList(self):
        if not (self.onboard_con and self.ground_con):
            return False

        ground_cursor = self.ground_con.cursor()
        onboard_cursor = self.onboard_con.cursor()

        if self.createVariableList():
            onboard_cursor.execute("select distinct on (channel, name) channel, name from status")
            for row in onboard_cursor.fetchall():
                try:
                    ground_cursor.execute("select * from status_var where channel = '%s' and name = '%s'" % tuple(row))
                    if len(ground_cursor.fetchall()) == 0:
                        ground_cursor.execute("insert into status_var(channel, name) values ('%s', '%s')" % tuple(row))
                except:
                    self.logs.exception("Problem inserting the status variable %s" % str(row))
                    print("We got a problem inserting into status_var: %s" % str(row))
            self.ground_con.commit()

    def addVariable(self, channel, name):
        with self.lock:
            if "fast" not in self._connections:
                # If we dont have the main connection, there is no reason for adding new variables
                return
            c = self._connections["fast"]
            onboard_cursor = c.cursor()

        try:
            onboard_cursor.execute("select * from sync_status_var where channel = '%(channel)s' and name = '%(name)s'" % vars())
            c.commit()
            found = (len(onboard_cursor.fetchall()) > 0)
            onboard_cursor.execute("select count(*) from status where channel = '%(channel)s' and name = '%(name)s'" % vars())

            exist = (int(onboard_cursor.fetchone()[0]) > 0)

            if exist and not found:
                onboard_cursor.execute("insert into sync_status_var(channel, name) values ('%(channel)s', '%(name)s')" % vars())
                c.commit()

            onboard_cursor.close()
        except Exception as e:
            print(str(e) + "\n" + str(Exception))
            self.logs.exception("the variable " + channel + "." + name + " couldn't be added to the watching list")

    def delVariable(self, channel, name):
        with self.lock:
            if "fast" not in self._connections:
                # If we dont have the main connection, there is no reason for adding new variables
                return
            c = self._connections["fast"]
            onboard_cursor = c.cursor()

        try:
            onboard_cursor.execute("select count(*) from sync_status_var where channel = '%(channel)s' and name = '%(name)s'" % vars())
            if len(onboard_cursor.fetchall()) != 0:
                onboard_cursor.execute("delete from sync_status_var where channel = '%(channel)s' and name = '%(name)s'" % vars())
            else:
                print("Channel: %(channel)s Name: %(name)s is not in the sync_status_var" % vars())
        except Exception as e:
            print(str(e) + "\n" + str(Exception))
            self.logs.exception("the variable " + channel + "." + name + " couldn't be removed of the watching list")
            return False

        c.commit()
        onboard_cursor.close()
        return True

    def getRecords(self, name, recordTime):
        """
        Get updates over the main link and insert them on the ground
        """
        with self.lock:
            if "fast" not in self._connections:
                # If we dont have the main connection, there is no reason for adding new variables
                return
            c = self._connections["fast"]
            onboard_cursor = c.cursor()

        onboard_cursor = c.cursor()
        ground_cursor = self._db_conn.cursor()

        try:
            onboard_cursor.execute("select id, name, timestamp, value, channel from status where (name = '%(name)s' and timestamp > ((select max(timestamp) from status where name = '%(name)s') - %(recordTime)f))" % vars())
            records = onboard_cursor.fetchall()
        except:
            self._lost_connection("fast")

    def run(self, stop_event, verbose=False):
        while not stop_event.is_set():
            self._run(verbose)
            stop_event.wait(self.period)

    def _run(self, verbose=False):
        with self._lock:

            if "fast" in self._connections:
                try:
                    conn = "fast"
                    c = self._connections[conn]
                    cursor = c.cursor()
                    cursor.execute("execute last_values")
                except:
                    self._lost_connection(conn)

            elif "slow" in self._connections:
                try:
                    conn = "slow"
                    c = self._connections[conn]
                    cursor = c.cursor()
                    cursor.execute("execute IMU_Lon_and_Lat")
                except:
                    self._lost_connection(conn)
            else:
                self.log.warning("No connection to the plane")
                return

        ground_cursor = self._db_conn.cursor()

        for row in onboard_cursor.fetchall():
            if conn == "slow":  # TODO: What does this code do then?
                # if not self.onboard_con:
                # We must add the channel which was not retrieved because we have to save bytes in the communication
                row = row + ('IMU',)

            ground_cursor.execute("insert into status " +
                                      "(onboard_id, name, timestamp, value, channel, inserted) " +
                                      "values (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', now())" % tuple(row))
            self._db_conn.commit()

        if verbose:
            try:
                ground_cursor.execute("select distinct on (name, channel) id, timestamp, inserted, onboard_id, channel, name, value from status where id >= (select max(id) from status)-(select count(*) from (select distinct on (name, channel) name, channel from status) foo)")
                SyncDB._pretty_ground_status(ground_cursor.fetchall())
            except:
                self.logs.exception("It could not retrieve the data from the ground database")
                print("Ground database failed")
        ground_cursor.close()

    @staticmethod
    def _pretty_ground_status(list):
        print("    id   | timestamp         |          inserted          | onboard_id |      channel      |         name         | value  " +
            "\n---------+-------------------+----------------------------+------------+-------------------+----------------------+--------")
        for row in list:
            print(" %7d | %9f | %26s | %10i | %17s | %20s | %8s " % (row[0], row[1], str(row[2]), row[3], row[4], row[5], row[6]))
        print("===============================================")

###################################################################
# Code to be executed when this module is called by the interpreter

if __name__ == '__main__':
    s = SyncDB.getInstance()

    if s.getReady(5):
        s.run(threading.Event(), verbose=True)
