
import threading as threading
import time

import sqlite3

from CryoCore.Core import Status, API, Utils

class DBStatusReporter(Status.OnChangeStatusReporter):
    def __init__(self, name, db_name="status.db"):
        """
        Log messages to a database on change
        """
        Status.OnChangeStatusReporter.__init__(self, name)

        
        self.name = name
        self.db_name = db_name
        
        self.cfg = API.get_config(name)
        self.log = API.get_log(name)

        self.db_connections = {}
        self.lock = threading.Lock()

        Utils.prepare_file(db_name) # Ensure that all dirs exist
        self._prepare_db()

    def _get_db(self, temporary_connection=False):

        if temporary_connection:
            conn = sqlite3.connect(self.db_name,
                                   isolation_level=None)
            # TODO: Better with different isolation_level?
            return conn
        
        self.lock.acquire()
        try:
            thread_name = threading.currentThread().getName()
            if thread_name not in self.db_connections:
                self.db_connections[thread_name] = sqlite3.connect(self.db_name,
                                                                   isolation_level=None)
              
            return self.db_connections[thread_name]
        finally:
            self.lock.release()

    def _close_db(self):
        
        self.lock.acquire()
        try:
            thread_name = threading.currentThread().getName()
            del self.db_connections[thread_name]
        finally:
            self.lock.release()
        
    def _execute(self,
                 SQL,
                 parameters = [],
                 temporary_connection = False,
                 ignore_error = False):
        """
        Execute an SQL statement with the given parameters.  Does handle if
        the database is locked or not
        """
        while True:
            try:
                cursor = self._get_db(temporary_connection = temporary_connection).cursor()
                return cursor.execute(SQL, parameters)
            except sqlite3.OperationalError as e:
                if ignore_error:
                    return

                print("Got operational error:", e.__class__, "[%s]"% str(e))
                if str(e) == "database is locked":
                    time.sleep(0.1) # Retry later
                elif str(e) == "cannot commit - no transaction is active":
                    return
                else:
                    print(e)
                    self.log.exception("Got an operational error during sql operation")
                    raise e
                
            except Exception as e:
                print(e)
                self.log.fatal("Got an error during sql operation: '%s'"%e)
                if not temporary_connection:
                    self._close_db()
                raise e

    def _prepare_db(self):
        """
        This function will prepare the db for a first utilisation
        It will create tables if needed
        """

        # Get list of existing tables
        cursor = self._execute("select tbl_name from sqlite_master where type='table' order by tbl_name")
        tables = []
        for row in cursor.fetchall():
            tables.append(row[0])

        # Gathering status info on channels:
        if not "status" in tables:
            #We create the table
            self._execute("""CREATE TABLE status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
	    timestamp DOUBLE,
            channel VARCHAR(128),
            name VARCHAR(128),
            value VARCHAR(128)
            )""")

        self._execute("CREATE INDEX stat_name ON status(name)", ignore_error=True)
        self._execute("CREATE INDEX stat_channel ON status(channel)", ignore_error=True)
        self._execute("CREATE INDEX stat_time ON status(timestamp)", ignore_error=True)

    def report(self, event):
        """
        Report to DB
        """
        
        SQL = "INSERT INTO status(timestamp, channel, name, value) "\
              "VALUES (?, ?, ?, ?)"
        self._execute(SQL,
                      (event.get_timestamp(),
                       event.status_holder.get_name(),
                       event.get_name(),
                       event.get_value()))
