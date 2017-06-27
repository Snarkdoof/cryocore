# coding=utf-8

from __future__ import print_function

import time
import threading as threading
import sqlite3
from CryoCore.Core import API
from CryoCore.Core.Utils import *
# from sys import stderr
# import MySQLdb
import mysql.connector as MySQLdb
import mysql.connector.pooling as mysqlpooling
from operator import itemgetter

import warnings

DEBUG = False
SLOW_WARNING = False


class sqliteDB:
    """
    This is a small sqlite3 database useful for software bits that need
    to access their own little DB.

    Typical usage: inherit this class and call the constructor with
    the filename to use.  Then implement the _prepare_db(self) method
    that is executed on init to create the necessary tables.

    You can now run self._execute(sql, params) which returns a cursor.
    This class is threadsafe too.
    """
    def __init__(self, db_name, config=None, tables={}):
        """
        Initialize a DB object.  The 'tables' should be a map of
        table_name:SQL STATEMENT.  If the tables are not already present,
        they will be created.

        Create indexes as you need
        (use self._execute("CREATE INDEX...", ignore_error=True))
        """

        assert db_name
        self.db_name = db_name
        self.db_connections = {}
        self._db_lock = threading.RLock()

        if config:
            self._mycfg = config
        else:
            self._mycfg = API.get_config("System.InternalDB")

        prepare_file(self.db_name)  # Ensure that all dirs exist
        self._prepare_db(tables)

    def _get_db(self, temporary_connection=False):

        if temporary_connection:
            conn = sqlite3.connect(self.db_name,
                                   isolation_level=None)  # Better with different isolation_level?
            return conn

        with self._db_lock:
            thread_name = threading.currentThread().ident
            if thread_name not in self.db_connections:
                self.db_connections[thread_name] = sqlite3.connect(self.db_name,
                                                                   isolation_level=None)
            return self.db_connections[thread_name]

    def _execute(self, SQL, parameters=[], temporary_connection=False, ignore_error=False):
        """
        Execute an SQL statement with the given parameters.  Does handle if
        the database is locked or not
        """
        while True:
            try:
                cursor = self._get_db(temporary_connection=temporary_connection).cursor()
                if not SLOW_WARNING:
                    return cursor.execute(SQL, parameters)
                start_time = time.time()
                r = cursor.execute(SQL, tuple(parameters))
                end_time = time.time()
                if end_time - start_time > 0.100:
                    print("WARNING: Slow query (%dms): '%s(%s)'" % (int((end_time - start_time) * 1000), SQL, parameters))
                return r
            except sqlite3.OperationalError as e:
                if ignore_error:
                    return
                print("Got operational error:", e.__class__, "[%s]" % str(e))
                if str(e) == "database is locked":
                    time.sleep(0.1)  # Retry later
                elif str(e) == "cannot commit - no transaction is active":
                    return
                else:
                    print(e)
                    raise e

            except Exception as e:
                print("FATAL", e)
                self.conn = None
                raise e

    def _prepare_db(self, tables):
        """
        This function will prepare the db for a first utilisation
        It will create tables if needed
        """

        # Get list of existing tables
        cursor = self._execute("select tbl_name from sqlite_master where type='table' order by tbl_name")
        existing_tables = []
        for row in cursor.fetchall():
            existing_tables.append(row[0])

        for key in list(tables.keys()):
            if key not in existing_tables:
                self._execute(tables[key])


class pgsql:
    """
    This class provides posgtres bits that are needed to access postgres.

    The config:
    db_name: Name of the DB to use
    db_user: user
    db_password: password
    db_host: database host, default localhost

    You can now run self._execute(sql, params) which returns a cursor.
    This class is threadsafe too.
    """

    def __init__(self, name, config=None):
        """
        Log messages to a database on change
        """
        # import psycopg2 as postgresql
        self._my_name = name
        self._mycfg = config
        self.log = API.get_log(name)
        self.db_connections = {}
        self._db_lock = threading.Lock()

    def __del__(self):
        with self._db_lock:
            for c in list(self.db_connections.values()):
                try:
                    c.commit()
                except:
                    pass

    def _get_db(self, temporary_connection=False):
        cfg = {"db_name": self._default_cfg["db_name"],
               "db_host": self._default_cfg["db_host"],
               "db_user": self._default_cfg["db_user"],
               "db_password": self._default_cfg["db_password"]}

        # Override defaults
        for elem in ["db_name", "db_host", "db_user", "db_password"]:
            if self._mycfg and self._mycfg[elem]:
                cfg[elem] = self._mycfg[elem]

        import psycopg2 as postgresql
        if temporary_connection:
            conn = postgresql.connect(cfg["db_name"])
            return conn

        with self._db_lock:
            thread_name = threading.currentThread().ident
            if thread_name not in self.db_connections:
                self.db_connections[thread_name] = postgresql.connect(host=cfg["db_host"],
                                                                      database=cfg["db_name"],
                                                                      user=cfg["db_user"],
                                                                      password=cfg["db_password"])

            return self.db_connections[thread_name]

    def _execute(self, SQL, parameters=[], temporary_connection=False, ignore_error=False, commit=True):
        """
        Execute an SQL statement with the given parameters.

        The SQL statement works fine if you use "?", sqlite style, e.g.
        "INSERT INTO table VALUES (?,?,?)".  Parameters will then be escaped
        automatically.
        """
        import psycopg2 as postgresql
        # Must convert SQL + parameters to a valid SQL statement string
        # REALLY???
        # TODO: ESCAPE PROPERLY HERE
        SQL = SQL.replace("?", "%s")
        conn = None
        while True:
            try:
                conn = self._get_db(temporary_connection=temporary_connection)
                cursor = conn.cursor()
                if len(parameters) > 0:
                    cursor.execute(SQL, tuple(parameters))
                else:
                    cursor.execute(SQL)
                if commit:
                    conn.commit()
                return cursor
            except postgresql.OperationalError as e:
                try:
                    if conn:
                        conn.commit()
                except:
                    pass

                if ignore_error:
                    return

                print("Got operational error:", e.__class__, "[%s]" % str(e))
                self.log.exception("Got an operational error during sql operation")
                raise e

            except Exception as e:
                print("Exception during sql operation:", SQL, str(parameters), e)
                try:
                    conn.commit()
                except:
                    pass

                self.log.exception("Got an error during sql operation: '%s'" % e)
                self.log.fatal("SQL was: %s %s" % (SQL, str(parameters)))
                self.conn = None
                raise e


class mysql:
    """
    This class provides bits that are needed to access mysql.

    The config:
    db_name: Name of the DB to use
    db_user: user
    db_password: password
    db_host: database host, default localhost

    You can now run self._execute(sql, params) which returns a cursor.
    This class is threadsafe too.

    If you provide a db_name to the init function, it will override
    any config for that exact parameter

    """

    def __init__(self, name, config=None, can_log=True, db_name=None, ssl=None, num_connections=3, min_conn_time=10):
        """
        Generic database wrapper
        if can_log is set to False, it will not try to log (should
        only be used for the logger!)
        min_conn_time is the minimum amount of time a DB connection is allowed to live since last execute before LRU can recycle it
        """
        self.num_connections = num_connections
        self.ssl = ssl
        self._db_name = db_name
        self._my_name = name
        self._mycfg = config
        if self._mycfg:
            self._mycfg.set_default("min_conn_time", 10.0)
        self._min_conn_time = min_conn_time
        if can_log:
            self.log = API.get_log(name)
        else:
            self.log = None
        self.connPool = None
        self.lastUsedConn = {}
        self.db_connections = {}
        self._db_lock = threading.RLock()

    def __del__(self):
        return  # No commit should be necessary
        with self._db_lock:
            for c in list(self.db_connections.values()):
                try:
                    c.commit()
                except:
                    pass

    def _init_sqls(self, sql_statements):
        """
        Prepare the database with the given SQL statements (statement, params)
        Errors are ignored for indexes, warnings are logged and not sent
        to the console
        """
        if self.log:
            self.log.debug("Initializing tables")
        with warnings.catch_warnings():
            warnings.simplefilter('error', MySQLdb.Warning)
            for statement in sql_statements:
                try:
                    if statement.lower().startswith("create index"):
                        ignore_error = True
                    else:
                        ignore_error = False
                    self._execute(statement, ignore_error=ignore_error)
                except MySQLdb.Warning as e:
                    if self.log:
                        self.log.warning("Preparing table '%s': %s" % (statement, e))

        if self.log:
            self.log.debug("Initializing tables DONE")

    def close_connection(self, thread_name=None):
        with self._db_lock:
            if thread_name is None:
                thread_name = threading.currentThread().ident
            if thread_name in self.db_connections:
                # print("Closing connection for", thread_name)
                try:
                    self.db_connections[thread_name][1].close()
                except:
                    pass
                try:
                    self.db_connections[thread_name][0].close()
                except Exception as e:
                    print("Error closing connection:", e)
                    pass

                del self.db_connections[thread_name]
                del self.lastUsedConn[thread_name]
                # print(self.__class__, "Closed connection for", thread_name, len(self.db_connections))

    def _get_conn_cfg(self):

        cfg = API.get_config_db()
        # Override defaults
        for elem in ["db_name", "db_host", "db_user", "db_password", "db_compress"]:
            if self._mycfg and self._mycfg[elem]:
                cfg[elem] = self._mycfg[elem]

        if self._db_name:
            cfg["db_name"] = str(self._db_name)

        if cfg["db_compress"] is None:
            cfg["db_compress"] = 0
        return cfg

    def _closeConnectionLRU(self):
        if self._min_conn_time is not None:
            min_time = self._min_conn_time
        else:
            min_time = self._mycfg["min_conn_time"]

        with self._db_lock:
            l = []
            for tn in self.lastUsedConn:
                if time.time() - self.lastUsedConn[tn] > min_time:  # Allow at least 10 seconds for each thread
                    l.append((tn, self.lastUsedConn[tn]))
            if len(l) == 0:
                # print("Trying to clear LRU cache, but no entries are old enough", self.lastUsedConn)
                return
            l.sort(key=itemgetter(1))

            thread_name = int(l[0][0])
            self.close_connection(thread_name)

    def _get_cursor(self, temporary_connection=False):
        return self._get_db()[1]

    def _get_db(self, temporary_connection=False):
        if not self.connPool:
            cfg = self._get_conn_cfg()
            # print(self.__class__, "Creating new pool with", self.num_connections, "connections", cfg)
            self.connPool = mysqlpooling.MySQLConnectionPool(
                pool_name="config",
                pool_size=self.num_connections,
                host=cfg["db_host"],
                user=cfg["db_user"],
                passwd=cfg["db_password"],
                db=cfg["db_name"],
                use_unicode=True,
                autocommit=True,
                charset="utf8")

        thread_name = threading.currentThread().ident
        with self._db_lock:
            if thread_name not in self.db_connections:
                try:
                    conn = self.connPool.get_connection()
                except MySQLdb.errors.PoolError as e:
                    if e.errno == -1:
                        # Exhausted pool try to free a connection, simple LRU
                        self._closeConnectionLRU()
                        # Retry
                        conn = self.connPool.get_connection()
                    else:
                        raise e
                # conn = self.connPool.get_connection()

                self.db_connections[thread_name] = (conn, conn.cursor())
                # print(self.__class__, "Allocatinged connection to", thread_name, len(self.db_connections))

            self.lastUsedConn[thread_name] = time.time()
            return self.db_connections[thread_name]

    def a_get_cursor(self, temporary_connection=False):
        # return self._get_conn(temporary_connection).cursor()
        return self._get_conn(temporary_connection)[1]

    def a_get_db(self, temporary_connection=False):
        return self._get_conn(temporary_connection)[0]

    def a_get_conn(self, temporary_connection=False):
        if temporary_connection:
            cfg = self._get_conn_cfg()
            conn = MySQLdb.connect(host=cfg["db_host"],
                                   user=cfg["db_user"],
                                   passwd=cfg["db_password"],
                                   db=cfg["db_name"],
                                   ssl=self.ssl,
                                   compress=cfg["db_compress"],
                                   use_unicode=True,
                                   charset="utf8")
            conn.autocommit(1)
            return (conn, conn.cursor())

        with self._db_lock:
            thread_name = threading.currentThread().ident  # getName()
            if thread_name not in self.db_connections:
                # print("Creating new connection for", thread_name, len(self.db_connections))
                cfg = self._get_conn_cfg()
                try:
                    conn = MySQLdb.connect(host=cfg["db_host"],
                                           user=cfg["db_user"],
                                           passwd=cfg["db_password"],
                                           db=cfg["db_name"],
                                           compress=cfg["db_compress"],
                                           use_unicode=True,
                                           autocommit=True,
                                           allow_local_infile=True,
                                           charset="utf8")
                    # TODO: if necessary, ssl can be provided with ssl_ca, ssl_cert and ssl_key

                    if 0:
                        conn = MySQLdb.connect(host=cfg["db_host"],
                                               user=cfg["db_user"],
                                               passwd=cfg["db_password"],
                                               db=cfg["db_name"],
                                               ssl=self.ssl,
                                               compress=cfg["db_compress"],
                                               use_unicode=True,
                                               local_infile=1,
                                               charset="utf8")
                        conn.autocommit(1)
                except Exception as e:
                    print("Error connecting: ", e)
                    raise e
                if DEBUG:
                    if self.log:
                        self.log.debug("New db connection for thread %s (%d)" % (thread_name, len(self.db_connections)))
                    else:
                        print("New db connection for thread %s (%d)" % (thread_name, len(self.db_connections)))

                self.db_connections[thread_name] = (conn, conn.cursor())

            return self.db_connections[thread_name]

    def _execute(self, SQL, parameters=[], temporary_connection=False,
                 ignore_error=False, commit=True,
                 log_errors=True, log_warnings=False):
        """
        Execute an SQL statement with the given parameters.

        The SQL statement works fine if you use "?", sqlite style, e.g.
        "INSERT INTO table VALUES (?,?,?)".  Parameters will then be escaped
        automatically.
        """
        # Must convert SQL + parameters to a valid SQL statement string
        # REALLY???
        # TODO: ESCAPE PROPERLY HERE
        rep = 0
        SQL = SQL.replace("?", "%s")
        if DEBUG:
            if self.log:
                self.log.debug("SQL %s(%s)" % (SQL, str(parameters)))
            else:
                print("SQL %s(%s)" % (SQL, str(parameters)))

        with warnings.catch_warnings():
            warnings.simplefilter('error', MySQLdb.Warning)

            for rep in range(0, 15):
                try:
                    cursor = self._get_cursor(temporary_connection=temporary_connection)
                    if SLOW_WARNING:
                        start_time = time.time()
                    if len(parameters) > 0:
                        cursor.execute(SQL, tuple(parameters))
                    else:
                        cursor.execute(SQL)
                    if SLOW_WARNING:
                        end_time = time.time()
                        if end_time - start_time > 0.100:
                            print("WARNING: Slow query (%dms): '%s(%s)'" % (int((end_time - start_time) * 1000), SQL, parameters))
                    return cursor
                except MySQLdb.Warning as e:
                    if self.log and log_warnings:
                        self.log.warning("Warning running '%s(%s)': %s" %
                                         (SQL, str(parameters), e))
                    return

                except MySQLdb.OperationalError as e:
                    print("Error", e.errno, e)
                    self.close_connection()
                    if self.log and log_errors:
                        self.log.exception("Got an operational error during sql operation")
                    time.sleep(1.0)
                except MySQLdb.errors.InterfaceError as e:
                    print("DB Interface error", e.errno)
                    self.close_connection()
                    if self.log and log_errors:
                        self.log.exception("Got an operational error during sql operation")
                    time.sleep(1.0)
                except MySQLdb.errors.ProgrammingError as e:
                    if e.errno == 1061:
                        # Duplicate key
                        if ignore_error:
                            return
                    print(e.errno, e)
                    if e.errno == -1:
                        if e.msg != "Cursor is not connected":
                            raise e
                        # TODO: Failed to trigger the exception below, this is thrown in lots of cases
                        # Cursor not connected - likely connection was reused by someone else, retry in a little bit
                        time.sleep(0.1)
                        continue
                    print("unknown error", e.errno, e)
                    raise e
                except MySQLdb.errors.PoolError:
                    print("WARNING: ConnectionPool exchausted, retry in a second (%s)" % e, self.__class__)
                    print("   connections:", len(self.db_connections), threading.currentThread().ident, self._db_connections)
                    time.sleep(1)
                except Exception as e:
                    print("Exception:", SQL, str(parameters), e.__class__, e)
                    if self.log and log_errors:
                        self.log.exception("Got an error during sql operation: '%s'" % e)
                        self.log.fatal("SQL was: %s %s" % (SQL, str(parameters)))
                    else:
                        import traceback
                        import sys
                        traceback.print_exc(file=sys.stdout)

                    raise e

        if API.api_stop_event.is_set():
            print("STOPPED, not throwing exceptions")
            return None

        raise Exception("Failed to connect to DB (tried %d times)" % rep)
