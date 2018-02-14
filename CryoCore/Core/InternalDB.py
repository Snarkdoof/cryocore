# coding=utf-8

from __future__ import print_function

import time
import threading as threading
from CryoCore.Core import API
from CryoCore.Core.Utils import *
# from sys import stderr
# import MySQLdb
import mysql.connector as MySQLdb

# import mysql.connector.pooling as mysqlpooling
# from operator import itemgetter
import warnings
import logging

import sys
if sys.version_info.major == 3:
    import queue
else:
    import Queue as queue


DEBUG = False
SLOW_WARNING = False

dbThreads = {}


class TooSlowException(Exception):
    pass


class FakeCursor():
    def __init__(self, result):
        if "return" not in result:
            self.resultset = []
        else:
            self.resultset = result["return"]
        self.index = 0
        if "rowcount" in result:
            self.rowcount = result["rowcount"]
        else:
            self.rowcount = None

        if "lastrowid" in result:
            self.lastrowid = result["lastrowid"]
        else:
            self.lastrowid = None

    def fetchone(self):
        if self.index >= len(self.resultset):
            return None
        res = self.resultset[self.index]
        self.index += 1
        return res

    def fetchall(self):
        return self.resultset

    def close(self):
        pass


class AsyncDB(threading.Thread):

    @staticmethod
    def getDB(config, name="global"):
        global dbThreads
        if name not in dbThreads:
            dbThreads[name] = AsyncDB(config)
            dbThreads[name].daemon = True  # Will be stopped too quickly otherwise...
            dbThreads[name].start()
        return dbThreads[name]

    def __init__(self, config=None):
        threading.Thread.__init__(self)
        self.runQueue = queue.Queue()
        self._db_name = None
        self.db_conn = None
        self.stop_event = API.api_stop_event
        self.running = True
        self.cursor = None
        if config:
            self._mycfg = config
        else:
            self._mycfg = {}
        self._lock = threading.Lock()

        self.log = logging.getLogger("DB")
        if len(self.log.handlers) < 1:
            hdlr = logging.StreamHandler(sys.stdout)
            # ihdlr = logging.handlers.RotatingFileHandler("UAVConfig.log",
            #                                            maxBytes=26214400)
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s')
            hdlr.setFormatter(formatter)
            self.log.addHandler(hdlr)
            self.log.setLevel(logging.DEBUG)

    def __del__(self):
        # print("AsyncDB deleted")
        try:
            self._close_connection()
        except:
            pass
        pass

    def execute(self, task):
        with self._lock:  # We protect the shutdown phase - if we queue something as we shut down, we'll hang
            if not self.running:
                raise Exception("Can't execute queries when not running")
            # if API.api_stop_event.isSet():
            #    print("*** WARNING: executing statements after API shutdown", task)

            self.runQueue.put(task)

    def run(self):
        self.running = True
        self.get_connection()
        stop_time = 0
        should_stop = False
        # print("ASYNCDB starting")
        # print(os.getpid(), threading.currentThread().ident, "o")
        while not should_stop:  # We wait for a bit after stop has been called to ensure that we finish all tasks
            try:
                with self._lock:
                    if self.stop_event.isSet() and self.runQueue.empty():
                        if not stop_time:
                            stop_time = time.time()
                        elif time.time() - stop_time > 2:
                            should_stop = True
                            self.running = False

                if self.runQueue.empty():
                    time.sleep(0.1)
                    continue

                task = self.runQueue.get(block=False, timeout=0.5)
                event, retval, SQL, parameters, ignore_error = task
                self._async_execute(event, retval, SQL, parameters, ignore_error)
            except queue.Empty:
                time.sleep(0.1)  # Condition variables, blocking queue - it all doesn't work...
                print(os.getpid(), "AsyncDB IDLE")
                continue
            except:
                print("Unhandled exception")
                import traceback
                traceback.print_exc(file=sys.stdout)
                time.sleep(0.25)

        if 0 or DEBUG:
            self.log.debug("ASYNC_DB STOPPED")

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

    def get_connection(self):
        while self.running:  # stop_event.isSet():
            cfg = self._get_conn_cfg()
            try:
                if not self.db_conn:
                    self.db_conn = MySQLdb.MySQLConnection(host=cfg["db_host"],
                                                           user=cfg["db_user"],
                                                           passwd=cfg["db_password"],
                                                           db=cfg["db_name"],
                                                           use_unicode=True,
                                                           autocommit=True,
                                                           charset="utf8")

                if self.db_conn:
                    return self.db_conn
            except:
                self.log.exception("Failed to get connection, trying in 5 seconds")
                time.sleep(5)

    def _close_connection(self):
        self.cursor = None
        try:
            self.db_conn.close()
        except:
            pass
        self.db_conn = None
        # _get_conn_pool(self._db_cfg)._close_connection()

    def _get_cursor(self, temporary_connection=False):
        try:
            if temporary_connection:
                return self.get_connection().cursor()

            if self.cursor is None:
                self.cursor = self.get_connection().cursor()
            return self.cursor
        except MySQLdb.OperationalError:
            self._close_connection()
            return self._get_cursor(temporary_connection)

    def _async_execute(self, event, retval, SQL, parameters=[],
                       temporary_connection=False,
                       ignore_error=False):
        """
        Execute an SQL statement with the given parameters.
        """
        retval["status"] = "failed"
        if DEBUG:
            self.log.debug(SQL + "(" + str(parameters) + ")")

        while True:
            try:
                try:
                    cursor = self._get_cursor()
                except Exception as e:
                    if not self.running:
                        retval["statue"] = "DB Inteface stopped"
                        break
                    # if self.stop_event.isSet():
                    #    print("STOP EVENT IS SET")
                    #    break
                    print("[%s] No connection, retrying in a bit" % os.getpid(), e)
                    import traceback
                    traceback.print_exc()
                    try:
                        self._close_connection()
                    except:
                        pass
                    time.sleep(1)
                    continue

                if len(parameters) > 0:
                    cursor.execute(SQL, tuple(parameters))
                else:
                    cursor.execute(SQL)
                retval["status"] = "ok"
                retval["return"] = []
                retval["rowcount"] = cursor.rowcount
                retval["lastrowid"] = cursor.lastrowid
                res = []
                if cursor.rowcount != 0 and SQL.upper().startswith("SELECT"):
                    try:
                        for row in cursor.fetchall():
                            res.append(row)
                    except:
                        pass  # fetchall likely used with no result
                retval["return"] = res
                break
            except MySQLdb.Warning:  # Is this really the correct thing to do?
                print("WARNING")
                break
            except MySQLdb.IntegrityError as e:
                retval["error"] = "IntegrityError: %s" % str(e)
                print("Integrity error %s, SQL was '%s(%s)'" % (SQL, str(parameters), e))
                if self.log:
                    self.log.exception("Integrity error %s, SQL was '%s(%s)'" % (SQL, str(parameters), e))
                break

            except MySQLdb.OperationalError as e:
                print("Error", e.errno, e)
                retval["error"] = "OperationalError: %s" % str(e)
                self._close_connection()
                time.sleep(1.0)
            except MySQLdb.errors.InternalError as e:
                # Likely a deadlock - check if we have error number 1213 (or 40001?)
                if e.errno == 1213:
                    # Deadlock detected, retry
                    print("*** DB Warning: Deadlock detected, retrying")
                    time.sleep(0.1)
                # Retry
            except MySQLdb.errors.InterfaceError as e:
                import traceback
                traceback.print_exc(file=sys.stdout)
                print("DB Interface error", e.errno)
                self._close_connection()
                retval["error"] = "DBError: %s" % str(e)
                time.sleep(1.0)
                # raise Exception("DEBUG")
            except MySQLdb.ProgrammingError as e:
                if ignore_error:
                    try:
                        if e.errno == 1061:
                            # Duplicate key
                            break
                    except:
                        retval["error"] = "UnhandledError: %s" % str(e)
                        break
                retval["error"] = "ProgrammingError: %s" % str(e)
                break
            except Exception as e:
                retval["error"] = "UnhandledError: %s" % str(e)
                print("Unhandled exception in _execute", e, e.__class__)
                import traceback
                traceback.print_exc(file=sys.stdout)
                print("SQL was:", SQL, str(parameters))
                break
                # raise e

        event.set()


class mysql:
    """
    This class provides bits that are needed to access mysql.

    The config:
    db_name: Name of the DB to use
    db_user: use
    db_password: password
    db_host: database host, default localhost

    You can now run self._execute(sql, params) which returns a cursor.
    This class is threadsafe too.

    If you provide a db_name to the init function, it will override
    any config for that exact parameter

    """

    def __init__(self, name, config=None, can_log=True, db_name=None, ssl=None, num_connections=3, min_conn_time=10, is_direct=False):
        """
        Generic database wrapper
        if can_log is set to False, it will not try to log (should
        only be used for the logger!)
        min_conn_time is the minimum amount of time a DB connection is allowed to live since last execute before LRU can recycle it
        """
        self._is_direct = is_direct
        self.stop_event = API.api_stop_event
        self.ssl = ssl
        self._db_name = db_name
        self._my_name = name
        self._mycfg = config
        self.cursor = None
        if self._mycfg:
            self._mycfg.set_default("min_conn_time", 10.0)
        self._min_conn_time = min_conn_time
        if can_log:
            self.log = API.get_log(name)
        else:
            self.log = logging.getLogger("uav_config")
            if len(self.log.handlers) < 1:

                hdlr = logging.StreamHandler(sys.stdout)
                # ihdlr = logging.handlers.RotatingFileHandler("UAVConfig.log",
                #                                            maxBytes=26214400)
                formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s')
                hdlr.setFormatter(formatter)
                self.log.addHandler(hdlr)
                self.log.setLevel(logging.DEBUG)

        if is_direct:
            self.db = AsyncDB(config)
        else:
            if db_name is None:
                db_name = "global"
            self.db = AsyncDB.getDB(config, db_name)

    def _init_sqls(self, sql_statements):
        """
        Prepare the database with the given SQL statements (statement, params)
        Errors are ignored for indexes, warnings are logged and not sent
        to the console
        """
        if DEBUG and self.log:
            self.log.debug("Initializing tables: %s" % os.getpid())
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

        if DEBUG and self.log:
            self.log.debug("Initializing tables DONE")

    def _execute(self, SQL, parameters=None,
                 temporary_connection=False,
                 ignore_error=False):
        if parameters is None:
            parameters = []
        if self._is_direct:
            try:
                if not self.cursor:
                    # self.cursor = AsyncDB.getDB(None)._get_cursor(False)
                    self.cursor = self.db._get_cursor(temporary_connection)
                self.cursor.execute(SQL, parameters)
                return self.cursor
            except Exception as e:
                self.cursor = None
                if ignore_error:
                    return self.cursor
                raise e
        event = threading.Event()
        retval = {}
        self.db.execute([event, retval, SQL, parameters, ignore_error])
        t = time.time()
        event.wait(60.0)
        if time.time() - t > 2.0:
            if SLOW_WARNING:
                print("*** SLOW ASYNC EXEC: %.2f" % (time.time() - t), SQL, parameters)
        if not event.isSet():
            raise TooSlowException("Failed to execute query in time (%s)" % SQL)

        if not ignore_error and "error" in retval:
            raise Exception(retval["error"])
        return FakeCursor(retval)
