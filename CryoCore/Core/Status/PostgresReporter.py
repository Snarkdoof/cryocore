import time
import psycopg2 as postgresql

from CryoCore.Core import API
from CryoCore.Core.Status import Status
import threading as threading


class PostgresStatusReporter(Status.OnChangeStatusReporter):

    def __init__(self, name="System.Status.Postgres"):
        """
        Log messages to a database on change
        """
        Status.OnChangeStatusReporter.__init__(self, name)

        self.name = name

        self.cfg = API.get_config(name)
        self.log = API.get_log(name)
        self.cfg.require(["db_name", "db_user", "db_password", "db_host"])

        self.db_connections = {}
        self.lock = threading.Lock()

        self._prepare_db()

    def _lost_db_connection(self):
        thread_name = threading.currentThread().getName()
        self.lock.acquire()
        try:
            if thread_name in self.db_connections:
                del self.db_connections[thread_name]
        finally:
            self.lock.release()

    def _get_db(self, temporary_connection=False):

        if temporary_connection:
            conn = postgresql.connect(self.cfg["db_name"])
            return conn

        self.lock.acquire()
        try:
            thread_name = threading.currentThread().getName()
            if thread_name not in self.db_connections:
                self.db_connections[thread_name] = postgresql.connect(host=self.cfg["db_host"],
                                                                      database=self.cfg["db_name"],
                                                                      user=self.cfg["db_user"],
                                                                      password=self.cfg["db_password"])
            return self.db_connections[thread_name]
        finally:
            self.lock.release()

    def _execute(self, SQL, parameters=[], temporary_connection=False, ignore_error=False):
        """
        Execute an SQL statement with the given parameters.  Does handle if
        the database is locked or not
        """
        # Must convert SQL + parameters to a valid SQL statement string
        # REALLY???
        SQL = SQL.replace("?", "'%s'")
        params = []
        for p in parameters:
            params.append(str(p).encode("string_escape"))
        SQL = SQL % tuple(params)
        parameters = ()

        while True:
            try:
                conn = self._get_db(temporary_connection=temporary_connection)
                cursor = conn.cursor()
                cursor.execute(SQL, tuple(parameters))
                conn.commit()
                return cursor
            except postgresql.InterfaceError as e:
                # Lost connection?
                self.log.warning("Lost connection to DB (%s)?" % e)
                self._lost_db_connection()
                continue

            except postgresql.OperationalError as e:
                if ignore_error:
                    try:
                        conn.commit()
                    except:
                        pass

                    return

                print("Got operational error:", e.__class__, "[%s]" % str(e))
                if str(e) == "database is locked":
                    time.sleep(0.1)  # Retry later
                elif str(e) == "cannot commit - no transaction is active":
                    return
                else:
                    print(e)
                    try:
                        conn.commit()
                    except:
                        pass
                    self.log.exception("Got an operational error during sql operation")
                    raise e
            except Exception as e:
                try:
                    conn.commit()
                except:
                    pass
                print(e)
                self.log.fatal("Got an error during sql operation: '%s' (SQL was: '%s')" % (e, SQL))
                self._lost_db_connection()
                raise e

    def _prepare_db(self):
        """
        This function will prepare the db for a first utilisation
        It will create tables if needed
        """

        try:
            self._execute("SELECT count(id) FROM status", ignore_error=True)
        except Exception as e:
            print("Got exception on select:", e)

            # Table does dot exist ?
            self.log.info("Table 'status' does not appear to exist, trying to create it")
            conn = self._get_db()
            conn.commit()

            self._execute("""CREATE TABLE status (
            id SERIAL PRIMARY KEY,
            timestamp FLOAT,
            channel VARCHAR(256),
            name VARCHAR(128),
            value VARCHAR(128)
            )""", ignore_error=True)

            self._execute("CREATE INDEX stat_name ON status(name)",
                          ignore_error=True)
            self._execute("CREATE INDEX stat_channel ON status(channel)",
                          ignore_error=True)
            self._execute("CREATE INDEX stat_time ON status(timestamp)",
                          ignore_error=True)

    def report(self, event):
        """
        Report to DB
        """
        try:
            SQL = "INSERT INTO status(timestamp, channel, name, value) "\
                "VALUES (?, ?, ?, ?)"
            self._execute(SQL,
                          (event.get_timestamp(),
                           event.status_holder.get_name(),
                           event.get_name(),
                           str(event.get_value())))
        except:
            self.log.exception("Updating status information %s.%s" % (event.status_holder.get_name(), event.get_name()))
