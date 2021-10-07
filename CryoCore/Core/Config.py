# coding=utf-8

"""
This class is not based on existing database wrappers, as they
all use the config service to get hold of their configuration.
"""
from __future__ import print_function

import mysql.connector as mysql
import threading
import os.path
import warnings
import sys

import json
import time

import logging
import logging.handlers

# from CryoCore.Core.Utils import logTiming

try:
    from CryoCore.Core.Status import SharedMemoryReporter
    shm = True
except:
    # print("Missing Shared memory support")
    shm = False

if sys.version_info.major == 3:
    import queue
else:
    import Queue as queue

DEBUG = False
_ANY_VERSION = 0


class ConfigException(Exception):
    pass


class NoSuchParameterException(ConfigException):
    pass


class NoSuchVersionException(ConfigException):
    pass


class VersionAlreadyExistsException(ConfigException):
    pass


class IntegrityException(ConfigException):
    pass


class CacheException(ConfigException):
    pass

_CONFIG_DB_CONNECTION_POOL = None


def _toUnicode(string):
    """
    Function to change a string (unicode or not) into a unicode string
    Will try utf-8 first, then latin-1.
    TODO: Is there a better way?  There HAS to be!!!
    """
    if sys.version_info.major == 3:
        if string.__class__ == str:
            return string
        try:
            return str(string, "utf-8")
        except:
            pass
        if string.__class__ == bytes:
            return str(string, "latin-1")
        return str(string)
    if string.__class__ not in [str, unicode]:
        return unicode(str(string), "utf-8")
    if string.__class__ == unicode:
        return string
    try:
        return unicode(string, "utf-8")
    except:
        pass
    return unicode(string, "latin-1")


class ConfigParameter:
    """
    Configuration Parameter objects allows more advanced interaction with a config parameter.
    """

    def __init__(self, cfg, id, name, parents, path, datatype, value, version,
                 last_modified, children=None, config=None, comment=None):
        """
        parents is a sorted list of parent ID's for the full path
        """
        if len(parents) == 0:
            raise Exception("Need parents")
        self.id = id
        self.name = name
        self.parents = parents
        self.path = path
        self._set_last_modified(last_modified)
        self.comment = comment
        self._cfg = cfg
        # Make 'float' the same as 'double' (no difference internally)
        if datatype == 'float':
            self.datatype = 'double'
        else:
            self.datatype = datatype
        self.version = version
        if not children:
            self.children = []
        else:
            self.children = children[:]
        self._config = config
        self.set_value(value, datatype, commit=False)

    def __str__(self):
        if self.value is None:
            return ""  # self.value
        return _toUnicode(self.value)

    def __eq__(self, value):
        """
        Overload == to see if the value of the parameter is the same as the given value
        """
        return self.value == value

    def __ne__(self, value):
        """
        Overload == to see if the value of the parameter is the same as the given value
        """
        return self.value != value

    def _set_last_modified(self, last_modified):
        if last_modified:
            self.last_modified = last_modified.timestamp()  # time.mktime(last_modified.timetuple()) + (last_modified.microsecond / 1e6)
        else:
            self.last_modified = None

    def set_comment(self, comment, commit=True):
        """
        Set the comment of the parameter
        """
        self.comment = comment
        if commit:
            self._cfg._commit(self)

    def set_value(self, value, datatype=None, check=True, commit=True):
        """
        Set the value explicitly.
        If check is False, the datatype is overwritten in the database. Use with caution
        If datatype is not set, the datatype is re-used from earlier.  If given, the datatype is updated.
        """
        if datatype:
            self.datatype = datatype
        elif self.datatype:
            datatype = self.datatype
        else:
            datatype = "string"  # raise Exception("Unknown datatype for %s"%self.name)

        if sys.version_info.major == 3:
            strings = [str]
        else:
            strings = [str, unicode]

        if check:
            try:
                if datatype == "folder":
                    self.value = None
                elif value.__class__ in strings:
                    if datatype == "boolean":
                        if value.__class__ == bool:
                            self.value = value
                        else:
                            if value.isdigit():
                                self.value = int(value) == 1
                            else:
                                self.value = value.lower() == "true"
                    elif datatype == "double":
                        self.value = float(value)
                    elif datatype == "integer":
                        self.value = int(value)
                    else:
                        self.value = value
                else:
                    self.value = value
            except:
                raise ConfigException("Could not 'cast' '%s' %s to a '%s' (native class: %s)" % (self.name, value, datatype, value.__class__))
        else:
            self.value = value

        if commit:
            self._cfg._commit(self, check)

    def _get_id(self):
        return self.id

    def get_full_path(self):
        """
        Returns the full path of the parameter
        """
        if self.path:
            return ".".join([self.path, self.name])
        return self.name

    def get_name(self):
        """
        Returns the name of the parameter
        """
        return self.name

    def get_value(self):
        """
        Returns the value of the parameter, casted to whatever the datatype is.
        All strings are unicode objects
        """
        if self.datatype == "integer":
            return int(self.value)
        if self.datatype == "double":
            return float(self.value)
        if self.datatype == "boolean":
            if (self.value.__class__ == bool):
                return self.value
            return self.value.lower() in ["true", "1"]
        if self.value.__class__ == "bytes":
            return _toUnicode(self.value)
        return self.value

    def get_children(self):
        """
        Returns the children, if any.  Returns a list, or [] if no children
        """
        return self.children

    def get_version(self):
        """
        Returns the config version
        """
        return self.version


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


class NamedConfiguration:
    """
    Named configuration allows a single configuration object to
    provide different roots
    """
    def __init__(self, root, version, config):

        self._parent = config
        self.version = version
        self.root = root
        if self.root and self.root[-1] != ".":
            self.root += "."

        self.add_version = self._parent.add_version
        self.clear_version = self._parent.clear_version
        self.delete_version = self._parent.delete_version
        self.set_version = self._parent.set_version
        self.list_versions = self._parent.list_versions
        self.copy_configuration = self._parent.copy_configuration
        self.search = self._parent.search
        self.get_by_id = self._parent.get_by_id
        self.get_version_info_by_id = self._parent.get_version_info_by_id
        self.deserialize = self._parent.deserialize
        self.last_updated = self._parent.last_updated
        self.del_callback = self._parent.del_callback
        try:
            r = "root"
            if self.root:
                r = self.root[:-1]
            self.children = self._parent.get(r).children
        except:
            self.children = None

    def _get_datatype(self, value):
        if value is None:
            return "string"

        datatype = "string"
        if value.__class__ == float:
            datatype = "double"
        elif value.__class__ == int:
            datatype = "integer"
        elif value.__class__ == int:
            datatype = "integer"
        elif value.__class__ == bool:
            datatype = "boolean"
            value = str(value)
        elif value.isdigit():
            datatype = "integer"
        elif value.count(".") == 1 and value.replace(".", "").isdigit():
            datatype = "double"
        elif value.lower() in ["true", "false"]:
            datatype = "boolean"
        else:
            datatype = "string"
        return datatype

    def get_children(self):
        r = "root"
        if self.root:
            r = self.root[:-1]

        return self._parent.get(r).children

    def serialize(self, path=None, root=None, version=None):
        if not version:
            version = self.version
        if not root:
            root = self.root
        elif self.root:
            root = self.root + "." + root
        return self._parent.serialize(path, root=self.root, version=version)

    def remove(self, path, version=None):
        if not version:
            version = self.version
        r = "root."
        if self.root:
            r = self.root

        self._parent.remove(r + path, version)

    def clear_all(self):
        self._parent.clear_all(self.version)

    def keys(self, path=None, root=None):
        if not root:
            root = self.root
        elif self.root:
            root = self.root + "." + root
        return self._parent.keys(path=path, root=root)

    def get_leaves(self, root=None, recursive=True):
        leaves = []
        if root is None:
            root = self.root[:-1]
        else:
            root = self.root + root
        for leave in self._parent.get_leaves(root, True, recursive=recursive):
            leaves.append(leave.replace(root + ".", ""))

        return leaves

    def require(self, params):
        self._parent.require(params, self.root)

    def add_callback(self, params, func, version=None):
        self._parent.add_callback(params, func, version=version, root=self.root)

    def set_default(self, name, value, datatype=None):
        self._parent.set_default(name, value, datatype, self.root)

    def get(self, _full_path, version=None, version_id=None,
            absolute_path=False, add=True):
        if not version:
            version = self.version

        return self._parent.get(_full_path, version, version_id,
                                absolute_path, add, self.root)

    def set(self, _full_path, value, version=None, datatype=None, comment=None, version_id=None,
            absolute_path=False, check=True, create=False):
        return self._parent.set(_full_path, value, version=version, datatype=datatype, comment=comment,
                                version_id=version_id, absolute_path=absolute_path, check=check,
                                create=create, root=self.root)

    def add(self, _full_path, value=None, datatype=None, comment=None,
            version=None,
            parent_id=None, overwrite=False, version_id=None, root=None):
        if not version:
            version = self.version
        return self._parent.add(_full_path, value, datatype, comment,
                                version, parent_id, overwrite, version_id, root=self.root)

    def __setitem__(self, name, value):
        """
        Short for get(name).set_value(value) - also creates the parameter if it did not exist.
        Usage:
          cfg["someparameter"] = value
          cfg["somefolder.somesubparameter"] = value
        """
        try:
            datatype = self._get_datatype(value)
            self.get(name).set_value(value, datatype=datatype, check=False)
        except NoSuchParameterException:
            # Create it
            self.add(name, value)

    def __getitem__(self, name):
        """
        Short for get(name).get_value() - also returns None as opposed to throwing NoSuchParameterException
        Usage:
          if cfg["someparameter"]:
          if cfg["somefolder.somesubparameter"] == expectedvalue:
            ...
        """
        try:
            val = self.get(name).get_value()
            if sys.version_info[0] == 2:
                if val.__class__ == str:
                    return val.encode("utf-8")
            return val
        except Exception:
            return None


class Configuration(threading.Thread):
    """
    MySQL based configuration implementation for the CryoWing UAV.
    It is thread-safe.
    """

    def __init__(self, version=None, root="", stop_event=None, db_cfg=None, is_direct=False, auto_init=True):
        """
        DO NOT USE this, use CryoCore.API.get_config instead
        """
        threading.Thread.__init__(self)
        self.stop_event = stop_event
        self._internal_stop_event = threading.Event()
        self._db_cfg = db_cfg
        self._is_direct = is_direct
        if root and root[-1] != ".":
            self.root = root + "."
        elif root is None:
            self.root = ""
        else:
            self.root = root

        self.running = True
        self._cb_lock = threading.Lock()
        self._cb_thread = None
        self.connPool = None
        self.db_conn = None

        self._load_lock = threading.RLock()
        self._lock = threading.Lock()
        self.callbackCondition = threading.Condition()
        self._notify_counter = 0
        self._runQueue = queue.Queue()
        self._condition = threading.Condition()
        self.cursor = None
        self.shmreporter = None

        self.dbg = threading.Lock()

        self._id_cache = {}
        self.cache = {}
        self._version_cache = {}
        self._callback_items = {}

        from .API import get_config_db
        self._cfg = get_config_db("config")

        self.log = logging.getLogger("uav_config")

        if len(self.log.handlers) < 1:
            hdlr = logging.StreamHandler(sys.stdout)
            # ihdlr = logging.handlers.RotatingFileHandler("UAVConfig.log",
            #                                            maxBytes=26214400)
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s')
            hdlr.setFormatter(formatter)
            self.log.addHandler(hdlr)
            self.log.setLevel(logging.DEBUG)

        self.get_connection()
        if not is_direct:
            self.start()

        if auto_init:
            self._prepare_tables()

        if not version or version == "default":
            try:
                version = self.get("root.default_version", version="default").get_value()
            except Exception:
                self.log.exception("DEBUG")
                self.set_version("default", create=True)
                self.add("root.default_version", "default", version="default")
                version = "default"

        self.set_version(version, create=True)
        self.version = version  # self._get_version_id(version)
        self.version_id = self._get_version_id(self.version)

        # self._init_id_cache()
        self._fill_full_cache()



        if shm:
            self.shmreporter = SharedMemoryReporter.SharedMemoryReporter()

    def _init_id_cache(self):
        SQL = "SELECT id, parent, name FROM config WHERE version=%s ORDER BY id"
        cursor = self._execute(SQL, [self.version_id])

        if self.version_id not in self._id_cache:
            self._id_cache[self.version_id] = {}

        paths = {}
        for id, parent, name in cursor.fetchall():
            if parent in paths:
                name = paths[parent][0] + "." + name
                id_path = paths[parent][1][:]
            else:
                id_path = [0]
            id_path.append(id)
            paths[id] = name, id_path
            self._id_cache[self.version_id][name] = id_path

    def _fill_full_cache(self):
        SQL = "SELECT id, parent, name, value, datatype, version, " + \
            "last_modified, comment FROM config WHERE version=%s ORDER BY id"
        cursor = self._execute(SQL, [self.version_id])

        for id, parentid, name, value, datatype, version, last_modified, comment in cursor.fetchall():
            if version not in self._id_cache:
                self._id_cache[version] = {}

            if parentid == 0:
                parent_ids = [0]
                path = None
                full_path = name
                parent = None
            else:
                try:
                    parent = self._cache_lookup_by_id(version, parentid)
                    path = parent.get_full_path()
                    full_path = path + "." + name
                    parent_ids = self._id_cache[version][path]
                except:
                    # We're lacking a parent, strange but DB is always correct.
                    continue

            param = ConfigParameter(self, id, name, parent_ids, path,
                                    datatype, value, version, last_modified,
                                    config=self, comment=comment)

            self._cache_update(version, full_path, param)
            id_path = parent_ids[:] + [id]
            self._id_cache[version][full_path] = id_path

            # If I have a parent, add me to it as child
            if parent:
                parent.children.append(param)

    def _cache_update(self, version, full_path, cp, expires=1):
        if version not in self.cache:
            self.cache[version] = {}
        self.cache[version][full_path] = cp, time.time() + expires
        # print("+", version, full_path, self.cache[version].keys())

    def _cache_remove(self, version, full_path):
        if version in self.cache:
            if full_path in self.cache[version]:
                del self.cache[version][full_path]
            # if len(self.cache[version]) == 0:
            #    del self.cache[version]
        # print("-", version, full_path, self.cache[version].keys())
        # if version in self._id_cache:
        #   myname = full_path[full_path.rfind(".") + 1:]
        #    for (parent_id, name) in self._id_cache[version]:
        #        if myname == name:
        #            del self._id_cache[version][(parent_id, name)]
        #            break

    def _cache_refresh(self, version, full_path):
        # Todo: Check if there is any new config - if there is, remove this?
        return self._cache_remove(version, full_path)

    def _cache_lookup(self, version, full_path):
        if version in self.cache:
            if full_path in self.cache[version]:
                val, expires = self.cache[version][full_path]
                if expires < time.time() or val is None:
                    # self.log.debug("** EXPIRE %s %s %s" % (version, full_path, self.cache[version].keys()))
                    self._cache_refresh(version, full_path)
                else:
                    # self.log.debug("** HIT %s %s %s" % (version, full_path, self.cache[version].keys()))
                    return val
        # self.log.debug("** FAIL %s %s %s" % (version, full_path, self.cache[version].keys()))
        raise CacheException("Missing parameter %s (version %s)" % (full_path, version))

    def _cache_lookup_by_id(self, version, id):
        if version in self.cache:
            # Must SEARCH the cache, no index for now - still faster than n sql statements
            now = time.time()
            for val, expires in self.cache[version].values():
                if not val:
                    continue
                if expires < now:
                    continue  # Don't clean the entire cache now
                if val.id == id:
                    return val
        # self.log.debug("** FAIL %s %s %s" % (version, full_path, self.cache[version].keys()))
        raise CacheException("Missing parameter %d (version %s)" % (id, version))

    def __del__(self):
        try:
            self._internal_stop_event.set()
        except:
            pass

        try:
            self.db_conn.close()
        except Exception as e:
            print("IGNORED: Failed to close DB connection", e)

    def get_connection(self):
        while not self.stop_event.isSet():
            try:
                if not self.db_conn:
                    if self._cfg["ssl.enabled"]:
                        self.db_conn = mysql.MySQLConnection(host=self._cfg["db_host"],
                                                             user=self._cfg["db_user"],
                                                             passwd=self._cfg["db_password"],
                                                             db=self._cfg["db_name"],
                                                             use_unicode=True,
                                                             autocommit=True,
                                                             charset="utf8",
                                                             ssl_key=self._cfg["ssl.key"],
                                                             ssl_ca=self._cfg["ssl.ca"],
                                                             ssl_cert=self._cfg["ssl.cert"])
                    else:
                        self.db_conn = mysql.MySQLConnection(host=self._cfg["db_host"],
                                                             user=self._cfg["db_user"],
                                                             passwd=self._cfg["db_password"],
                                                             db=self._cfg["db_name"],
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
        # return self.get_connection().cursor()

        try:
            if self.cursor is None or temporary_connection:
                self.cursor = self.get_connection().cursor()
            return self.cursor
        except mysql.OperationalError:
            self._close_connection()
            return self._get_cursor(temporary_connection)

    def _execute(self, SQL, parameters=[],
                 temporary_connection=False,
                 ignore_error=False):

        if self._is_direct:
            try:
                if DEBUG:
                    self.log.debug("%d: " % threading.get_ident() +  SQL % tuple(parameters))
                cursor = self._get_cursor(True)
                cursor.execute(SQL, parameters)

                if self.shmreporter and SQL.startswith("INSERT") or SQL.startswith("UPDATE"):
                    ts = time.time()
                    # print(ts, "Broadcast (direct)")
                    e = SharedMemoryReporter.SimpleEvent("config", ts, "updated", ts)
                    self.shmreporter.report(e)

                with self.callbackCondition:
                    self._notify_counter += 1
                    self.callbackCondition.notifyAll()


                return cursor
            except Exception as e:
                if ignore_error:
                    return cursor
                raise e

        # raise RuntimeException("Deprecated")

        event = threading.Event()
        retval = {}

        #with self._lock:
        if 1:
            if not self.running:
                raise Exception("Can't execute more commands - have stopped")
            self._runQueue.put([event, retval, SQL, parameters, ignore_error])

        # t = time.time()
        event.wait(10.0)

        # print(time.time() - t, ":", SQL % tuple(parameters))
        #if time.time() - t > 2.0:
        #    print("*** SLOW ASYNC EXEC: %.2f" % (time.time() - t), SQL, parameters)
        if not event.isSet():
            raise Exception("Failed to execute Config query in time (%s)" % SQL)

        if not ignore_error and "error" in retval:
            raise Exception(retval["error"])
        return FakeCursor(retval)

    def run(self):
        self.get_connection()
        stop_time = 0
        should_stop = False
        # print(os.getpid(), threading.currentThread().ident, "RUNNING")
        from CryoCore.Core import API
        while not should_stop:  # We wait for a bit after stop has been called to ensure that we finish all tasks
            # with self._lock:
            if 1:
                if self.stop_event.isSet() and self._runQueue.empty():
                    if not stop_time:
                        # print("Async config should stop soon")
                        stop_time = time.time()
                    # In case API.shutdown_grace_period == 0, test the condition even if we just set stop_time
                    if time.time() - stop_time > API.shutdown_grace_period:
                        # print(f"Async config DB stopped: {API.shutdown_grace_period} {API.queue_timeout}")
                        self.running = False
                        should_stop = True
            try:
                task = self._runQueue.get(block=True, timeout=API.queue_timeout)
                event, retval, SQL, parameters, ignore_error = task
                self._async_execute(event, retval, SQL, parameters, ignore_error)
            except queue.Empty:
                # print(os.getpid(), "AsyncConfig IDLE", self.stop_event.isSet(), self._runQueue.empty(), should_stop)
                # time.sleep(0.1)  # Condition variables, blocking queue, doesn't work
                continue
            except:
                print("Unhandled exception")
                import traceback
                import sys
                traceback.print_exc(file=sys.stdout)
        if DEBUG:
            print("*** Async config STOPPED *** ")

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
                    if self.stop_event.isSet():
                        break
                    print("[%s] No connection, retrying in a bit" % os.getpid(), e)
                    import traceback
                    traceback.print_exc()
                    time.sleep(1)
                    continue
                if len(parameters) > 0:
                    cursor.execute(SQL, tuple(parameters))
                else:
                    cursor.execute(SQL)
                retval["status"] = "ok"
                retval["return"] = []
                res = []
                retval["rowcount"] = cursor.rowcount
                retval["lastrowid"] = cursor.lastrowid
                if cursor.rowcount != 0:
                    try:
                        for row in cursor.fetchall():
                            res.append(row)
                    except:
                        pass  # fetchall likely used with no result
                retval["return"] = res
                break
            except mysql.Warning:  # Is this really the correct thing to do?
                print("WARNING")
                break
            except mysql.IntegrityError as e:
                retval["error"] = "IntegrityError: %s" % str(e)
                print("Integrity error %s, SQL was '%s(%s)'" % (SQL, str(parameters), e))
                self.log.exception("Integrity error %s, SQL was '%s(%s)'" % (SQL, str(parameters), e))
                break

            except mysql.OperationalError as e:
                print("Error", e.errno, e)
                retval["error"] = "OperationalError: %s" % str(e)
                self._close_connection()
                time.sleep(1.0)
            except mysql.errors.InterfaceError as e:
                import traceback
                import sys
                traceback.print_exc(file=sys.stdout)
                print("DB Interface error", e.errno)
                self._close_connection()
                retval["error"] = "DBError: %s" % str(e)
                time.sleep(1.0)
            except mysql.ProgrammingError as e:
                if ignore_error:
                    try:
                        if e.errno == 1061:
                            # Duplicate key
                            break
                    except:
                        retval["error"] = "UnhandledError: %s" % str(e)
                        break
                        pass
                retval["error"] = "ProgrammingError: %s" % str(e)
                break
            except Exception as e:
                retval["error"] = "UnhandledError: %s" % str(e)
                print("Unhandled exception in _execute", e, e.__class__)
                import traceback
                import sys
                traceback.print_exc(file=sys.stdout)
                print("SQL was:", SQL, str(parameters))
                break
                # raise e

        event.set()

        if self.shmreporter and SQL.startswith("INSERT") or SQL.startswith("UPDATE"):
            ts = time.time()
            # print(ts, "Async broadcast")
            e = SharedMemoryReporter.SimpleEvent("config", ts, "updated", ts)
            self.shmreporter.report(e)

            with self.callbackCondition:
                self._notify_counter += 1
                self.callbackCondition.notify()


    def _prepare_tables(self):
        """
        Prepare all tables and indexes if they do not exist
        """

        with warnings.catch_warnings():
            warnings.simplefilter('error', mysql.Warning)

            SQL = """CREATE TABLE IF NOT EXISTS config_version (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) UNIQUE,
    device VARCHAR(128),
    comment TEXT) ENGINE = INNODB"""
            self._execute(SQL, ignore_error=False)

            self._execute("CREATE INDEX config_version_name ON config_version(name)",
                          ignore_error=True)
            self._execute("INSERT IGNORE INTO config_version (id, name) VALUES(0, 'default')",
                          ignore_error=True)
            SQL = """CREATE TABLE IF NOT EXISTS config (
    id INT PRIMARY KEY AUTO_INCREMENT,
    version INT NOT NULL,
    last_modified TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    parent INT NOT NULL,
    name VARCHAR(128),
    value VARCHAR(256),
    datatype ENUM ('boolean', 'integer', 'double', 'string', 'folder') DEFAULT 'string',
    comment TEXT,
    FOREIGN KEY (version) REFERENCES config_version(id) ON DELETE CASCADE,
    UNIQUE(version,parent,name)) ENGINE = INNODB"""
            # Workaround for errors when SQL server doesn't support precision on timestamp type
            try:
                self._execute(SQL, ignore_error=False)
            except:
                SQL = SQL.replace("TIMESTAMP(6)", "TIMESTAMP")
                self._execute(SQL, ignore_error=False)

            self._execute("CREATE INDEX config_name ON config(name)",
                          ignore_error=True)

            self._execute("CREATE INDEX config_parent ON config(parent)",
                          ignore_error=True)

    def _reset(self):
        """
        Clear temporary data - should be run before software is started
        """
        pass

    def clear_all(self, version):
        """
        Removes ALL config parameters - use with extreme caution - also, might not work
        """
        version_id = self._get_version_id(version)
        SQL = "DELETE FROM config WHERE version=%s"
        self._execute(SQL, [version_id])

        if version_id in self._id_cache:
            self._id_cache[version_id] = {}
        if version_id in self.cache:
            self.cache[version_id] = {}

    def add_version(self, version):
        """
        Add an empty configuration
        """

        with self._load_lock:
            # To avoid screaming in logs and stuff, just check if we have
            # the version already
            try:
                self._get_version_id(version)
                raise VersionAlreadyExistsException("Version '%s' already exists" % version)
            except NoSuchVersionException:
                pass

            SQL = "INSERT INTO config_version(name) VALUES(%s)"
            try:
                cursor = self._execute(SQL, [version])
                # cursor.close()
            except Exception as e:
                print("Version already existed...", e)
                raise VersionAlreadyExistsException(version)

            # Now we need to get the version ID back
            # TODO: Get the last auto-incremented value directly?

    def clear_version(self, version):
        return self.delete_version(version, True)

    def delete_version(self, version, keep_version=False):
        """
        Delete a version (and all config parameters of it!)
        TODO: This must be fixed, must also clear the caches
        """
        print("WARNING: delete_version requires restart")
        #print("***REFUSING TO DELETE FOR DEBUG PURPOSES")
        #return
        with self._load_lock:
            c = self._execute("SELECT id FROM config_version WHERE name=%s", [version])
            if c.rowcount > 1:
                raise Exception("No way!")
            version_id = c.fetchone()

            if not version_id:
                return

            SQL = "DELETE FROM config_version WHERE name=%s"
            self._execute(SQL, [version])

            SQL = "DELETE FROM config WHERE version=%s"
            self._execute(SQL, [version_id[0]])

    def set_version(self, version, create=False):
        """
        Convert a version string to an internal number.
        Throws NoSuchVersionException if it doesn't exist
        """
        with self._load_lock:
            try:
                self._cfg["version"] = self._get_version_id(version)
                self._cfg["version_string"] = version
                self.version_id = self._cfg["version"]
            except NoSuchVersionException as e:
                if not create:
                    raise e
                self.add_version(version)
                return self.set_version(version, create=False)

    def list_versions(self, partial_name=""):
        with self._load_lock:

            if partial_name:
                cursor = self._execute("SELECT config_version.name, config_version.comment, max(last_modified) FROM config_version, config WHERE config_version.id=config.version AND name LIKE '%" + partial_name.replace("'", "") + "%' GROUP BY config.version")
            else:
                cursor = self._execute("SELECT config_version.name, config_version.comment, max(last_modified) FROM config_version, config WHERE config_version.id=config.version GROUP BY config.version")

            versions = []
            for row in cursor.fetchall():
                versions.append((row[0], row[1], row[2].ctime()))
            return versions

    def _get_version_id(self, name):
        if name not in self._version_cache:
            cursor = self._execute("SELECT id FROM config_version WHERE name=%s",
                                   [name])
            if cursor.rowcount > 1:
                raise Exception("No way!")
            row = cursor.fetchone()
            if not row:
                raise NoSuchVersionException(name)
            self._version_cache[name] = row[0]
        return self._version_cache[name]

    def copy_configuration(self, old_version, new_version, overwrite=False):
        """
        Copy a configuration to a new configuration.
        """
        with self._load_lock:
            try:
                self.add_version(new_version)
            except:
                pass

            old_id = self._get_version_id(old_version)
            new_id = self._get_version_id(new_version)

            SQL = "INSERT INTO config (version, parent, name, value, datatype) SELECT %s,parent,name,value,datatype FROM config WHERE version=%s"
            self._execute(SQL, [new_id, old_id])

    def _get_id_path(self, full_path, version, create=True, overwrite=False, is_leaf=True):
        if DEBUG:
            self.log.debug("_get_id_path(%s)" % full_path)

        if version not in self._id_cache:
            self._id_cache[version] = {}

        if full_path in self._id_cache[version]:
            return self._id_cache[version][full_path][:]

        # First find the parent
        if full_path.count(".") == 0:
            if DEBUG:
                self.log.debug("%s is in the root" % full_path)
            id_path = [0]
            parent_path = "root"
            name = full_path
        else:
            parent_name, name = full_path.rsplit(".", 2)[-2:]
            parent_path = full_path.rsplit(".", 1)[0]

            id_path = self._get_id_path(parent_path, version, create,
                                        overwrite=overwrite, is_leaf=False)

        if id_path is None:
            return None

        my_id = self._get_param_id(id_path[-1], name, version)
        if my_id:
            id_path.append(my_id)
        else:
            if create and not is_leaf:
                self.add(full_path, datatype="folder", version_id=version)
            else:
                # No parameter - add it to the full cache
                self._cache_update(self.version_id, full_path, None)
                raise NoSuchParameterException(full_path)
            my_id = self._get_param_id(id_path[-1], name, version)
            if my_id:
                id_path.append(my_id)

        self._id_cache[version][full_path] = id_path[:]
        return id_path

    def _get_param_id(self, parent_id, name, version):
        if name == "root":
            return 0

        SQL = "SELECT id FROM config WHERE parent=%s AND name=%s "
        params = [parent_id, name]
        if version:
            SQL += "AND version=%s"
            params.append(version)

        cursor = self._execute(SQL, params)
        if cursor.rowcount > 1:
            raise Exception("No way!")
        row = cursor.fetchone()
        if not row:
            if DEBUG:
                # Cache this too?
                self.log.debug("Tried to get param id for %s (version %s) but failed" %
                               (name, version))
                c = self._execute("SELECT name, parent from config where version=%s", [version])
                c.fetchall()
                # print(c.fetchall())
            return None

        # self._id_cache[version][(parent_id, name)] = row[0]
        return row[0]

    def _get_full_path(self, full_path, root=None):
        if root is None:
            root = self.root

        if not full_path:
            return root[:-1]

        if full_path.startswith("root.") or full_path == "root":
            path = full_path[4:]
            if len(path) > 0 and path[0] == ".":
                path = path[1:]
            return path
        else:
            if root:
                if full_path:
                    return root + full_path
                else:
                    return root[:-1]
        return full_path

    def search(self, partial, version=None):
        """
        Search the config for a partial match
        """
        # if not version:
        #    version = self._cfg["version"]
        # else:
        #    version = self._get_version_id(version)
        if not version:
            version = self.version_id

        found = []
        with self._load_lock:
            cursor = self._execute("SELECT id FROM config WHERE version=%s AND name LIKE %s", [version, "%" + partial + "%"])
            for row in cursor.fetchall():
                found.append(row[0])

        res = []
        for item in found:
            res.append(self.get_by_id(item))
        return res

    def get_by_id(self, param_id, recursing=False):
        with self._load_lock:
            if param_id == 0:
                return [(0, "")]

            # Do we already have this?
            if 1:
                try:
                    item = self._cache_lookup_by_id(self.version_id, param_id)
                    if item:
                        return item
                    else:
                        raise NoSuchParameterException("Parameter number: %s (cache hit)" % param_id)
                except CacheException:
                    pass  # cache miss

            SQL = "SELECT id, parent, name, value, datatype, version, last_modified, comment FROM config WHERE id=%s"
            cursor = self._execute(SQL, [param_id])
            if cursor.rowcount > 1:
                raise Exception("No way!")
            row = cursor.fetchone()
            if not row:
                raise NoSuchParameterException("Parameter number: %s" % param_id)

            id, parent_id, name, value, datatype, version, timestamp, comment = row
            parent_ids = []

            if recursing:
                parent_info = self.get_by_id(parent_id, recursing=True)
                return parent_info + [(id, name)]

            parent_info = self.get_by_id(parent_id, recursing=True)
            path = ""
            parent_ids = []
            for (parent_id, parent_name) in parent_info:
                if (parent_name):
                    path += parent_name + "."
                parent_ids.append(parent_id)
            path = path[:-1]
            cp = ConfigParameter(self, id, name, parent_ids, path,
                                 datatype, value,
                                 version, timestamp, config=self,
                                 comment=comment)

            if cp.datatype == "folder":
                timestamp, cp.children = self._get_children(cp)
                cp._set_last_modified(timestamp)
            return cp

    def get(self, _full_path, version=None, version_id=None,
            absolute_path=False, add=True, root=None):
        """
        Get a ConfigParameter. Throws NoSuchParameterException if not found

        If version is specified, only matches are returned
        if version_id is specified, no text-to-id lookup is performed.
        if absolute_path is True, no root is added to the _full_path
        """
        if root is None:
            root = self.root

        with self._load_lock:
            if version_id:
                version = version_id
            else:
                if not version:
                    version = self.version_id  # self._cfg["version"]
                else:
                    version = self._get_version_id(version)

            if not absolute_path:
                full_path = self._get_full_path(_full_path, root)
            else:
                if root:
                    full_path = root + _full_path
                else:
                    full_path = _full_path

            if full_path.startswith("root."):
                full_path = full_path[5:]
            elif full_path.startswith("root"):
                full_path = full_path[4:]
            try:
                item = self._cache_lookup(version, full_path)
                if item:
                    return item
                else:
                    raise NoSuchParameterException("No such parameter: " + full_path)
            except CacheException:
                # Cache miss
                pass

            if DEBUG:
                self.log.debug("get(%s, %s, %s, %s, %s)" % (_full_path, full_path, version, root, add))

            parent_ids = []
            if full_path.count(".") == 0:
                name = full_path
                path = ""
                parent_ids = [0]
            else:
                (path, name) = full_path.rsplit(".", 1)

            if full_path != "root" and full_path != "":  # special case for root node
                # Do we have this in the cache?
                id_path = self._get_id_path(full_path, version, create=add)
                if not id_path:
                    parent_path = full_path[:full_path.rfind(".")]
                    parent_ids = self._get_id_path(parent_path, version, create=add)
                    if parent_ids is None:
                        if add:
                            self.add(parent_path, datatype="folder", version=version)

                    raise NoSuchParameterException("No such parameter: " + full_path)

                # Find the thingy
                SQL = "SELECT id, value, datatype, version, " + \
                    "last_modified, comment FROM config WHERE id=%s"
                params = [id_path[-1]]
                cursor = self._execute(SQL, params)
                if cursor.rowcount > 1:
                    raise Exception("No way!")
                row = cursor.fetchone()
                if not row:
                    # Caching failures fails - a create is typically called
                    # self._cache_update(version, full_path, None, time.time() + 0.2)
                    # No parameter - add it to the full cache
                    self._cache_update(self.version_id, full_path, None)
                    raise NoSuchParameterException("No such parameter: " + full_path)
                id, value, datatype, version, timestamp, comment = row
                cp = ConfigParameter(self, id, name, id_path[:-1], path,
                                     datatype, value,
                                     version, timestamp, config=self,
                                     comment=comment)
            else:
                cp = ConfigParameter(self, 0, "root", [0], "",
                                     "folder", "", version, None, config=self)

            if 0 or cp.datatype == "folder":
                timestamp, cp.children = self._get_children(cp)
                cp._set_last_modified(timestamp)

            self._cache_update(version, full_path, cp)
            return cp

    def _get_children(self, config_parameter):
        SQL = "SELECT id, name, value, datatype, version, " + \
            "last_modified, comment FROM config WHERE " +\
            "parent=%s AND version=%s ORDER BY name"
        cursor = self._execute(SQL, [config_parameter.id,
                                     config_parameter.version])

        parent_ids = config_parameter.parents + [config_parameter.id]
        path = config_parameter.get_full_path()
        res = []
        import datetime
        my_timestamp = datetime.datetime(1970, 1, 1)
        for id, name, value, datatype, version, timestamp, comment in cursor.fetchall():
            res.append(ConfigParameter(self, id, name, parent_ids, path,
                                       datatype, value, version, timestamp,
                                       config=self, comment=comment))
            my_timestamp = max(timestamp, my_timestamp) if timestamp is not None else my_timestamp

        return my_timestamp, res

    def _get_datatype(self, value):
        if value is None:
            return "string"

        datatype = "string"
        if value.__class__ == float:
            datatype = "double"
        elif value.__class__ == int:
            datatype = "integer"
        elif value.__class__ == int:
            datatype = "integer"
        elif value.__class__ == bool:
            datatype = "boolean"
            value = str(value)
        elif value.isdigit():
            datatype = "integer"
        elif value.count(".") == 1 and value.replace(".", "").isdigit():
            datatype = "double"
        elif value.lower() in ["true", "false"]:
            datatype = "boolean"
        else:
            datatype = "string"
        return datatype

    def remove(self, full_path, version=None):
        """
        Remove a config element (recursively).  Use with caution
        """
        if not version:
            version = self.version

        def _rec_delete(id, version, path):
            ret = []
            c = self._execute("SELECT id, name FROM config WHERE parent=%s AND version=%s", [id, version])
            for row in c.fetchall():
                ret.extend(_rec_delete(row[0], version, path + "." + row[1]))
            self._cache_remove(version, path)

            if path in self._id_cache[version]:
                del self._id_cache[version][path]

            ret.append((id, path))
            return ret

        with self._load_lock:
            param = self.get(full_path, version, add=False)
            params = _rec_delete(param._get_id(), param.get_version(), full_path)
            SQL = "DELETE FROM config WHERE "
            args = []
            for i, p in params:
                SQL += "id=%s OR "
                args.append(i)
            SQL = SQL[:-4]
            self._execute(SQL, args)
            if full_path.startswith("root."):
                full_path = full_path[5:]

            # Cache should be cleaned after _rec_delete
            # self._cache_remove(version, full_path)

            if 0:
                if full_path in self._id_cache[self.version_id]:
                    del self._id_cache[self.version_id][full_path]
                    print("Removed %s from ID cache" %full_path)
                else:
                    print("Warning: Removed element not in ID cache", full_path, self._id_cache[self.version_id].keys())

                if full_path in self.cache[self.version_id]:
                    del self.cache[self.version_id][full_path]
                    print("Removed %s from cache" % full_path)
                else:
                    print("Warning: Removed element not in cache", full_path)

    def add(self, _full_path, value=None, datatype=None, comment=None,
            version=None,
            parent_id=None, overwrite=False, version_id=None, root=None):
        """
        Add a new config parameter. If datatype is not specified,
        we'll guess.  If version is not specified, the current version
        is used.
        """
        if not root:
            id_path = []
        else:
            id_path = None

        if root is None:
            root = self.root
        with self._load_lock:
            full_path = self._get_full_path(_full_path, root)
            assert full_path
            if full_path.count(".") == 0:
                name = full_path
            else:
                (path, name) = full_path.rsplit(".", 1)
            if version_id:
                version = version_id
            elif not version:
                version = self._cfg["version"]
            else:
                version = self._get_version_id(version)
            if DEBUG:
                self.log.debug("Add (" + str(full_path) + ", " + str(value) + ", " + str(datatype) + ", " + str(version) + ")")
            if parent_id is None:
                if full_path.find(".") > -1:
                    parent_path = full_path.rsplit(".", 1)[0]
                    id_path = self._get_id_path(parent_path, version, create=True, is_leaf=False)
                    self._id_cache[version][parent_path] = id_path[:]
                    parent_id = id_path[-1]
                else:  # root
                    parent_id = 0

            if id_path is None:
                raise Exception("ID path is None, full_path is", full_path, "root", root)

            # Determine datatype
            if not datatype:
                datatype = self._get_datatype(value)

            if overwrite:
                SQL = "REPLACE"
            else:
                SQL = "INSERT"
            if datatype == "boolean":
                value = str(value)
            SQL += " INTO config (version, parent, name, value, datatype, comment) VALUES (%s, %s, %s, %s, %s, %s)"
            if DEBUG:
                self.log.debug(SQL + " (" + str((version, parent_id, name, value, datatype)) + ")")

            c = self._execute(SQL, (version, parent_id, name, value, datatype, comment))
            id_path.append(c.lastrowid)
            self._id_cache[version][full_path] = id_path
            if full_path.find(".") > -1:  # Add myself to the cache
                parent_path = full_path.rsplit(".", 1)[0]
                parent_ids = id_path[:][:-1]
                cp = ConfigParameter(self, c.lastrowid, name, parent_ids, parent_path,
                                     datatype, value,
                                     version, None, config=self,
                                     comment=comment)
                try:
                    parent = self.get(parent_path)
                    if parent:
                        parent.children.append(cp)
                    self._cache_update(self.version_id, full_path, cp, 1.0)
                except:
                    # This is bad stuff
                    pass

    def _clean_up(self):
        """
        Remove any parameters that are "lost", i.e. their parent is missing
        """
        with self._load_lock:
            # Clean up missing children now
            SQL = "SELECT config.id, config.name, parent.id FROM config LEFT OUTER JOIN config AS parent ON config.parent=parent.id WHERE parent.id IS NULL AND config.parent<>0"
            cursor = self._execute(SQL)
            params = []
            for row in cursor.fetchall():
                self.log.warning("DELETING PARAMETER %s - lost due to overwrite" % (row[1]))
                params.append(row[0])
            if len(params) > 0:
                SQL = "DELETE FROM config WHERE "
                SQL += "ID=%s OR " * len(params)
                SQL = SQL[:-4]
                self._execute(SQL, params)

    def set(self, _full_path, value, version=None, datatype=None, comment=None, version_id=None,
            absolute_path=False, check=True, create=False, root=None):

        with self._load_lock:
            if version and not version_id:
                version_id = self._get_version_id(version)
            try:
                param = self.get(_full_path, version_id=version_id, absolute_path=absolute_path, root=root)
            except Exception as e:
                if create:
                    self.add(_full_path, value, version=version, datatype=datatype, comment=comment, version_id=version_id, root=root)
                    param = self.get(_full_path, version_id=version_id, absolute_path=absolute_path, root=root)
                else:
                    raise e
            if DEBUG:
                self.log.debug("Updating parameter %s to %s (type: %s)" % (_full_path, value, datatype))

            if comment is not None:
                param.set_comment(comment)
            param.set_value(value, datatype=datatype, check=check)
            # self._commit(param, check) this is done by set_value now

    def _commit(self, config_parameter, check=True):
        """
        Commit an updated parameter to the database
        """
        with self._load_lock:
            # Integrity check?
            error = False
            if config_parameter.value:
                if not check:
                    config_parameter.datatype = self._get_datatype(config_parameter.value)
                else:
                    dt = self._get_datatype(config_parameter.value)
                    if config_parameter.datatype in ["double", "float"]:
                        if dt not in ["float", "double", "integer"]:
                            error = True
                    elif dt != config_parameter.datatype:
                        error = True
                    if error:
                        raise Exception("Refusing to save inconsistent datatype for config parameter %s=%s. Datatype of parameter is '%s' but type of value is '%s'." % (config_parameter.name, config_parameter.value, config_parameter.datatype, dt))

            SQL = "UPDATE config SET value=%s,datatype=%s,comment=%s WHERE id=%s AND version=%s"
            self._execute(SQL, [config_parameter.value,
                                config_parameter.datatype,
                                config_parameter.comment,
                                config_parameter.id,
                                config_parameter.version])

    def get_leaves(self, _full_path=None, absolute_path=False, recursive=True):
        """
        Recursively return all leaves of the given path
        """
        raise Exception("Deprecated, use children instead")
        with self._load_lock:
            param = self.get(_full_path, absolute_path=absolute_path, add=False)
            leaves = []
            folders = []
            for child in param.children:
                print("Checking", child.name, len(param.children))
                if child.datatype == "folder":
                    folders.append(child)
                elif len(child.children) == 0:
                    print("Found leave", child.get_full_path())
                    leaves.append(child.get_full_path()[len(self.root):])

            for folder in folders:
                print("Checking", folder.name, len(param.children))
                if recursive:
                    leaves += self.get_leaves(folder.get_full_path(),
                                              absolute_path=True)
                elif len(folder.children) == 0:
                    print("No children either in folder", folder.get_full_path())
                    leaves.append(folder.get_full_path()[len(self.root):])

            return leaves

    def keys(self, path=None, root=None):
        """
        List the keys of this node (names of the children)
        """
        with self._load_lock:
            try:
                param = self.get(path, root=root, add=False)
                children = []
                for child in param.children:
                    children.append(child.name)
                return children
            except NoSuchParameterException:
                return []

    def get_version_info_by_id(self, version_id):
        """
        Return a map of version info
        """
        with self._load_lock:
            SQL = "SELECT name, device, comment FROM config_version WHERE id=%s"
            cursor = self._execute(SQL, [version_id])
            if cursor.rowcount > 1:
                raise Exception("No way!")
            row = cursor.fetchone()
            if not row:
                raise NoSuchVersionException("ID: %s" % version_id)
            name, device, comment = row
            return {"name": name,
                    "device": device,
                    "comment": comment}

    def _serialize_recursive(self, root, version_id):
        """
        Internal, recursive function for serialization
        """
        param = self.get(root, version_id=version_id, absolute_path=True, add=False)
        children = []
        for child in param.children:
            children.append(self._serialize_recursive(child.get_full_path(), version_id))

        serialized = {"name": param.name,
                      "value": param.value,
                      "datatype": param.datatype,
                      "comment": param.comment,
                      "last_modified": param.last_modified,
                      "children": children}
        for child in children:  # Needed to simplify web use
            if not isinstance(child["name"], str):
                print("*** JIKES *** Expected string as child name, got", child["name"].__class__, child["name"])
                continue
            serialized[child["name"]] = child

        return serialized

    def _deserialize_recursive(self, serialized, root, version_id,
                               overwrite=False):
        """
        Internal, recursive function for deserialization
        """
        if not serialized or serialized.__class__ != dict:
            return

        if "value" in serialized:
            if root and root != "root":
                if overwrite:
                    try:
                        self.set(
                            root,
                            serialized["value"],
                            serialized["datatype"],
                            comment=serialized["comment"],
                            version_id=version_id)
                        self.set(root,
                                 serialized["value"],
                                 serialized["datatype"],
                                 comment=serialized["comment"],
                                 version_id=version_id)
                    except NoSuchParameterException:
                        self.log.exception("Must create new parameter %s version %s" % (root, version_id))
                        # new parameter, add it
                        self.add(root, serialized["value"],
                                 serialized["datatype"],
                                 comment=serialized["comment"],
                                 version_id=version_id, overwrite=overwrite)
                else:
                    self.add(root, serialized["value"], serialized["datatype"],
                             comment=serialized["comment"],
                             version_id=version_id, overwrite=overwrite)

        if "children" in serialized:
            print("Will do children", serialized["children"])
            for child in serialized["children"]:
                path = root + "." + child["name"]
                self._deserialize_recursive(child, path, version_id,
                                            overwrite)
        else:
            for elem in list(serialized.keys()):
                if elem in ["name", "datatype", "comment", "last_modified"]:
                    continue

                path = root + "." + elem
                self._deserialize_recursive(serialized[elem], path, version_id,
                                            overwrite)

    # ##################    JSON functionality for (de)serializing ###

    def serialize(self, path="", root=None, version=None):
        """
        Return a JSON serialized block of config
        """
        with self._load_lock:
            if version:
                version_id = self._get_version_id(version)
            else:
                version_id = self._cfg["version"]

            version_info = self.get_version_info_by_id(version_id)
            full_path = self._get_full_path(path, root=root)
            serialized = self._serialize_recursive(full_path, version_id)
            if not full_path:
                full_path = "root"
            elif full_path[-1] == ".":
                full_path = full_path[:-1]
            serialized = {full_path: serialized,
                          "version": version_info}

            return json.dumps(serialized, indent=1)

    def deserialize(self, serialized, root="", version=None, overwrite=False):
        """
        Parse a JSON serialized block of config
        """

        with self._load_lock:
            if version:
                version_id = self._get_version_id(version)
            else:
                version_id = self._cfg["version"]

            self.get_version_info_by_id(version_id)  # Ensure it's there
            if not root:
                root = "root"
            cfg = json.loads(serialized)

            # Clear caches
            # self._id_cache = {}  # The id cache makes massive issues if multiple config objects exists - removing one will not invalidate caches, hence trouble
            self._version_cache = {}

            if DEBUG:
                self.log.debug(
                    "Deserialize %s from device %s (%s) into config version %s" %
                    (cfg["version"]["name"],
                     cfg["version"]["device"],
                     cfg["version"]["comment"],
                     version))
            try:
                self.get(root)
            except:
                self.add(root, datatype="folder")

            self._deserialize_recursive(cfg, root, version_id, overwrite)
            self._clean_up()

    # ###################   Quick functions   ####################

    def __setitem__(self, name, value):
        """
        Short for get(name).set_value(value) - also creates the parameter if it did not exist.
        Usage:
          cfg["someparameter"] = value
          cfg["somefolder.somesubparameter"] = value
        """
        try:
            datatype = self._get_datatype(value)
            self.get(name).set_value(value, datatype=datatype, check=False)
        except NoSuchParameterException:
            # Create it
            self.add(name, value)

    def __getitem__(self, name):
        """
        Short for get(name).get_value() - also returns None as opposed to throwing NoSuchParameterException
        Usage:
          if cfg["someparameter"]:
          if cfg["somefolder.somesubparameter"] == expectedvalue:
            ...
        """
        try:
            val = self.get(name).get_value()
            if sys.version_info[0] == 2:
                if val.__class__ == str:
                    return val.encode("utf-8")
            return val
        except Exception:
            return None

    def set_default(self, name, value, datatype=None, root=None):
        """
        Set the default value of a parameter.  The parameter will be created if it does not exist.  If the parameter was set, it will not be changed by this function.
        """
        # Check if the root of this thing exists
        if root is None:
            root = self.root

        # Do a quick cache lookup - if we have the path, we don't need to create it
        if self.version_id in self._id_cache:
            if root + name in self._id_cache[self.version_id]:
                return

        try:
            self.get(name, root=root, absolute_path=True)
        except Exception:  # Could this be a NoSuchParameterException?
            # self.log.exception("Get %s failed, adding" % name)
            self.add(name, value, datatype=datatype, root=root)

    def require(self, param_list, root=None):
        """
        Raise a NoSuchParameterException if any of the parameters are not
        available
        """
        with self._load_lock:
            # This could be done faster, but who cares
            for param in param_list:
                self.get(param, root=root, add=False)

    def last_updated(self, version=None):
        """
        Return the last update time for the given version
        """
        if version:
            version_id = self._get_version_id(version)
        else:
            version_id = self._get_version_id(self.version)

        c = self._execute("SELECT UNIX_TIMESTAMP(MAX(last_modified)) FROM config WHERE version=%s", [version_id])
        if cursor.rowcount > 1:
            raise Exception("No way!")
        row = c.fetchone()
        return row[0]

    # ###############  Callback management ##################
    def _callback_thread_main(self):
        if DEBUG:
            self.log.debug("Callback thread started")

        # If shared memory, we'll use that too
        if shm:
            from CryoCore.Core.Status.StatusListener import StatusListener
            listener = StatusListener(evt=self.callbackCondition)
            listener.add_monitors([("config", "updated")])
        else:
            listener = None

        last_notified = 0
        while not self._internal_stop_event.is_set() and not self.stop_event.is_set():
            try:
                # Check all parameters we're interested in
                params = {}
                cbs = {}
                # print("Registered callbacks", self._callback_items)
                for key in self._callback_items:
                    registered = self._callback_items[key]
                    for paramid in registered["params"]:
                        lastupdate = registered["params"][paramid]
                        if lastupdate is None:
                            lastupdate = 0
                        if paramid not in params:
                            params[paramid] = lastupdate
                            cbs[paramid] = [(lastupdate, key)]
                        else:
                            params[paramid] = min(lastupdate, registered["params"][paramid])
                            cbs[paramid].append((lastupdate, key))

                if len(params) == 0:
                    # This process shoulnd't really run any more...
                    self.log.info("Callback thread running but no callbacks registered. Stopping")
                    self._cb_thread = None
                    return

                # We how have the list with times, make the SQL
                SQL = "SELECT id, name, value, last_modified FROM config WHERE "
                p = []
                for param in params:
                    SQL += "(id=%s AND last_modified>%s) OR "
                    p.append(param)
                    import datetime
                    p.append(datetime.datetime.fromtimestamp(params[param]))

                SQL = SQL[:-3]
                # print(time.time(), "Checking")
                cursor = self._execute(SQL, p)  # , temporary_connection=True)
                if cursor.rowcount > 0:
                    print(time.time(), "Performing callbacks")
                # We should now have a list of all the updated parameters we have, loop and call back
                for param_id, name, value, last_modified in cursor.fetchall():
                    last_modified = time.mktime(last_modified.timetuple())
                    for lastupdate, cbid in cbs[param_id]:
                        if lastupdate >= last_modified:
                            continue
                        self._callback_items[cbid]["params"][param_id] = last_modified

                        func = self._callback_items[cbid]["func"]
                        args = self._callback_items[cbid]["args"]
                        param = self.get_by_id(param_id)
                        try:
                            if args:
                                func(param, *args)
                            else:
                                func(param)
                        except:
                            self.log.exception("In callback handler")

                with self.callbackCondition:
                    self.callbackCondition.wait(1.0)

            except:
                self.log.exception("INTERNAL: Callback handler crashed badly")
                time.sleep(1)

    def del_callback(self, callback_id):
        """
        Remove a callback, using the ID returned by add_callback
        """

        with self._cb_lock:
            if callback_id in self._callback_items:
                del self._callback_items[callback_id]

    def add_callback(self, parameter_list, func, root=None, version=None, *args):
        """
        Add a callback for the given parameters.  Returns the ID of the callback (for use with del_callback)
        """
        if sys.version_info.major == 3:
            if parameter_list.__class__ == str:
                parameter_list = [parameter_list]
        elif parameter_list.__class__ in [str, unicode]:
            parameter_list = [parameter_list]

        if not self.stop_event:
            raise Exception("Require a stop_event to the configuration instance to allow callbacks")
        if not func:
            raise Exception("Refusing to add callback without a function")
        if version:
            version_id = self._get_version_id(version)
        else:
            # version_id = self._cfg["version"]
            version_id = self._get_version_id(self.version)
        import random
        callback_id = random.randint(0, 0xffffff)

        items = {}
        for param in parameter_list:
            p = self.get(param, version_id=version_id, root=root)
            items[p.id] = p.last_modified

        with self._cb_lock:
            self._callback_items[callback_id] = {"params": items, "func": func, "args": args}

            if not self._cb_thread:
                self._cb_thread = threading.Thread(target=self._callback_thread_main)
                self._cb_thread.daemon = True
                self._cb_thread.start()

        return callback_id


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "upgrade":
            cfg = Configuration()
            cfg._execute("alter table config modify last_modified timestamp(6)")
            cfg._execute("TRUNCATE TABLE config_callback")
            cfg._execute("alter table config_callback modify last_modified timestamp(6)")
