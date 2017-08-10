# coding=utf-8

"""
This class is not based on existing database wrappers, as they
all use the config service to get hold of their configuration.
"""
from __future__ import print_function

import mysql.connector as mysql
import mysql.connector.pooling as mysqlpooling
import threading
import os.path
import warnings
import sys
from operator import itemgetter

import json
import time

import logging
import logging.handlers

DEBUG = False
_ANY_VERSION = 0
_MAX_THREADS = 5


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
    if string.__class__ == unicode:
        return string
    try:
        return unicode(string, "utf-8")
    except:
        pass
    return unicode(string, "latin-1")


class _ConnectionPool:
    def __init__(self, db_cfg):
        # Defaults
        from API import get_config_db
        self._cfg = get_config_db()
        self.db_connections = {}
        if db_cfg:
            for param in self._cfg:
                if param in db_cfg:
                    self._cfg[param] = db_cfg[param]

        self.lastUsedConn = {}
        self.connPool = None
        self.lock = threading.Lock()

    def _closeConnectionLRU(self):

        for i in range(0, 6):
            l = []
            for tn in self.lastUsedConn:
                l.append((tn, self.lastUsedConn[tn]))
            if len(l) == 0:
                print("Wierd, trying to clear LRU cache, but no entries, lastusedconn is: %d: %s" % (len(self.lastUsedConn), str(self.lastUsedConn)))
                # Go drastic, delete the whole pool and create a new one
                # TODO: FIX THIS - IT SHOULD NEVER BE NECESSARY AND COULD LEAD TO CONNECTION LEAKS
                self.connPool = None
                self.db_connections = {}
                return
            l.sort(key=itemgetter(1))
            if time.time() - l[0][1] < 0.25:
                # Less than 250 ms since this the LRU item was last used, wait a bit and try again
                # print("Let the thing finish", time.time() - l[0][1], i)
                time.sleep(0.25 - (time.time() - l[0][1]))
            else:
                break

        thread_name = int(l[0][0])
        # print("Closing connection for", thread_name)
        try:
            self.db_connections[thread_name][1].close()
            self.db_connections[thread_name][0].close()
        except Exception as e:
            print("Exception closing", e)
            pass
        try:
            del self.db_connections[thread_name]
        except:
            pass
        try:
            del self.lastUsedConn[thread_name]
        except:
            pass

    def get_connection(self):
        #print("Get connection:", self._cfg)
        with self.lock:
            if not self.connPool:
                self.connPool = mysqlpooling.MySQLConnectionPool(
                    pool_name="config",
                    pool_size=_MAX_THREADS,
                    host=self._cfg["db_host"],
                    user=self._cfg["db_user"],
                    passwd=self._cfg["db_password"],
                    db=self._cfg["db_name"],
                    use_unicode=True,
                    autocommit=True,
                    charset="utf8")

            thread_name = threading.currentThread().ident
#            print(thread_name, "in", id(self.db_connections), self.db_connections.keys())
            if thread_name not in self.db_connections:
                try:
                    conn = self.connPool.get_connection()
                except mysql.errors.PoolError as e:
                    if e.errno == -1:
                        print("Pool exchausted, try to free connection", e)
                        # Exhausted pool try to free a connection, simple LRU
                        self._closeConnectionLRU()
                        # Retry
                        conn = self.connPool.get_connection()
                    else:
                        raise e
#                print(self.__class__, id(self), "Allocated connection to", thread_name, len(self.db_connections))
                self.db_connections[thread_name] = (conn, conn.cursor())

            self.lastUsedConn[thread_name] = time.time()
            return self.db_connections[thread_name]

    def _close_connection(self):
        with self.lock:
            thread_name = threading.currentThread().ident
            if thread_name in self.db_connections:
                try:
                    self.db_connections[thread_name][1].close()
                    self.db_connections[thread_name][0].close()
                except:
                    pass
#                print("Closing connection", thread_name)
                del self.db_connections[thread_name]


def _get_conn_pool(db_cfg=None):
    global _CONFIG_DB_CONNECTION_POOL
    if _CONFIG_DB_CONNECTION_POOL is None:
        _CONFIG_DB_CONNECTION_POOL = _ConnectionPool(db_cfg)
    return _CONFIG_DB_CONNECTION_POOL


class ConfigParameter:
    """
    Configuration Parameter objects allows more advanced interaction with a config parameter.
    """

    def __init__(self, cfg, id, name, parents, path, datatype, value, version,
                 last_modified, children=None, config=None, comment=None):
        """
        parents is a sorted list of parent ID's for the full path
        """
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
            self.last_modified = time.mktime(last_modified.timetuple())
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

        if check:
            try:
                if datatype == "folder":
                    self.value = None
                elif value.__class__ in [str, str]:
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


class Configuration:
    """
    MySQL based configuration implementation for the CryoWing UAV.
    It is thread-safe.
    """

    def __init__(self, version=None, root="", stop_event=None, db_cfg=None):
        """
        DO NOT USE this, use CryoCore.API.get_config instead
        """
        self.stop_event = stop_event
        self._internal_stop_event = threading.Event()
        self._db_cfg = db_cfg
        if root and root[-1] != ".":
            self.root = root + "."
        elif root is None:
            self.root = ""
        else:
            self.root = root

        self._cb_lock = threading.Lock()
        self._cb_thread = None
        self.connPool = None
        self._load_lock = threading.RLock()
        self.lock = threading.Lock()
        self.callbackCondition = threading.Condition()
        self._notify_counter = 0

        from API import get_config_db
        self._cfg = get_config_db()

        self.log = logging.getLogger("uav_config")
        if len(self.log.handlers) < 1:
            hdlr = logging.StreamHandler(sys.stdout)
            # ihdlr = logging.handlers.RotatingFileHandler("UAVConfig.log",
            #                                            maxBytes=26214400)
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s')
            hdlr.setFormatter(formatter)
            self.log.addHandler(hdlr)
            self.log.setLevel(logging.DEBUG)

        # self._id_cache = {}
        self.cache = {}
        self._version_cache = {}
        self._update_callbacks = {}

        self._prepare_tables()

        if not version or version == "default":
            try:
                version = self.get("root.default_version", version="default").get_value()
            except:
                self.set_version("default", create=True)
                self.add("root.default_version", "default", version="default")
                version = "default"

        self.set_version(version, create=True)
        self.version = version  # self._get_version_id(version)

    def _cache_update(self, version, full_path, cp, expires):
        if version not in self.cache:
            self.cache[version] = {}
        self.cache[version][full_path] = cp, time.time() + 0.2
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

    def _cache_lookup(self, version, full_path):
        if version in self.cache:
            if full_path in self.cache[version]:
                val, expires = self.cache[version][full_path]
                if expires < time.time():
                    # self.log.debug("** EXPIRE %s %s %s" % (version, full_path, self.cache[version].keys()))
                    self._cache_remove(version, full_path)
                else:
                    # self.log.debug("** HIT %s %s %s" % (version, full_path, self.cache[version].keys()))
                    return val
        # self.log.debug("** FAIL %s %s %s" % (version, full_path, self.cache[version].keys()))
        raise Exception("Missing parameter")

    def __del__(self):
        try:
            self._internal_stop_event.set()
        except:
            pass

        with self._cb_lock:
            for (cb_id, param_id) in self._update_callbacks:
                try:
                    self._execute("DELETE FROM config_callback WHERE id=%s AND param_id=%s",
                                  [cb_id, param_id])
                except:
                    # self.log.exception("Failed to clean up callbacks")
                    pass

    def _close_connection(self):
        _get_conn_pool(self._db_cfg)._close_connection()

    def _get_cursor(self, temporary_connection=False):
        return _get_conn_pool(self._db_cfg).get_connection()[1]

    def _execute(self, SQL, parameters=[],
                 temporary_connection=False,
                 ignore_error=False):
        """
        Execute an SQL statement with the given parameters.
        """

        if DEBUG:
            self.log.debug(SQL + "(" + str(parameters) + ")")

        while True:
            try:
                try:
                    cursor = self._get_cursor()
                except Exception as e:
                    print("No connection, retrying in a bit", e)
                    import traceback
                    traceback.print_exc()
                    time.sleep(1)
                    continue
                if len(parameters) > 0:
                    cursor.execute(SQL, tuple(parameters))
                else:
                    cursor.execute(SQL)
                return cursor
            except mysql.Warning:
                return
            except mysql.IntegrityError as e:
                print("Integrity error %s, SQL was '%s(%s)'" % (SQL, str(parameters), e))
                self.log.exception("Integrity error %s, SQL was '%s(%s)'" % (SQL, str(parameters), e))
                raise e

            except mysql.OperationalError as e:
                print("Error", e.errno, e)
                self._close_connection()
                time.sleep(1.0)
            except mysql.errors.InterfaceError as e:
                print("DB Interface error", e.errno)
                self._close_connection()
                time.sleep(1.0)
            except mysql.ProgrammingError as e:
                if ignore_error:
                    try:
                        if e.errno == 1061:
                            # Duplicate key
                            return
                    except:
                        pass
                raise e
            except Exception as e:
                print("Unhandled exception in _execute", e, e.__class__)
                import traceback
                import sys
                traceback.print_exc(file=sys.stdout)
                print("SQL was:", SQL, str(parameters))
                # raise e

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
    last_modified TIMESTAMP(6),
    parent INT NOT NULL,
    name VARCHAR(128),
    value VARCHAR(256),
    datatype ENUM ('boolean', 'integer', 'double', 'string', 'folder') DEFAULT 'string',
    comment TEXT,
    FOREIGN KEY (version) REFERENCES config_version(id) ON DELETE CASCADE,
    UNIQUE(version,parent,name)) ENGINE = INNODB"""
            self._execute(SQL, ignore_error=False)

            self._execute("CREATE INDEX config_name ON config(name)",
                          ignore_error=True)

            self._execute("CREATE INDEX config_parent ON config(parent)",
                          ignore_error=True)

            SQL = """CREATE TABLE IF NOT EXISTS config_callback  (
    id INT NOT NULL,
    param_id INT NOT NULL,
    last_modified TIMESTAMP(6),
    PRIMARY KEY (id, param_id),
    FOREIGN KEY (param_id) REFERENCES config(id) ON DELETE CASCADE)"""
            self._execute(SQL, ignore_error=True)

    def _reset(self):
        """
        Clear temporary data - should be run before software is started
        """
        self._execute("DELETE FROM config_callback")

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
                cursor.close()
            except:
                raise VersionAlreadyExistsException(version)

            # Now we need to get the version ID back
            # TODO: Get the last auto-incremented value directly?

    def delete_version(self, version):
        """
        Delete a version (and all config parameters of it!)
        """
        with self._load_lock:
            SQL = "DELETE FROM config_version WHERE name=%s"
            self._execute(SQL, [version])

    def set_version(self, version, create=False):
        """
        Convert a version string to an internal number.
        Throws NoSuchVersionException if it doesn't exist
        """
        with self._load_lock:
            try:
                self._cfg["version"] = self._get_version_id(version)
                self._cfg["version_string"] = version
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

    def _get_parent_ids(self, full_path, version, create=True, overwrite=False):
        if DEBUG:  # Makes it loop???
            self.log.debug("_get_parent_ids(%s)" % full_path)

        # First find the parent
        if full_path.count(".") == 0:
            if DEBUG:
                self.log.debug("%s is in the root" % full_path)
            return [0]

        parent_name, name = full_path.rsplit(".", 2)[-2:]
        parent_path = full_path.rsplit(".", 1)[0]

        parent_ids = self._get_parent_ids(parent_path, version, create,
                                          overwrite=overwrite)
        parent_id = self._get_parent_id(parent_ids[-1], parent_name, version)
        if parent_id is None:
            if not create:
                raise NoSuchParameterException("Parent of %s does not exist, full path: '%s'" % (name, full_path))

            if 0 and DEBUG:
                self.log.debug("No such parent - creating: %s, %s, %s" %
                               (parent_ids[-1], parent_name, version))

            self.add(parent_name, datatype="folder",
                     parent_id=parent_ids[-1],
                     overwrite=overwrite)
            parent_id = self._get_parent_id(parent_ids[-1], parent_name, version)
            if parent_id is None:
                raise NoSuchParameterException("Failed to create %s" % name)

        parent_ids.append(parent_id)
        return parent_ids

    def _get_parent_id(self, parent_id, name, version):
        if name == "root":
            return 0

        # if version not in self._id_cache:
        #     self._id_cache[version] = {}
        # if (parent_id, name) in self._id_cache[version]:
        #    return self._id_cache[version][(parent_id, name)]

        SQL = "SELECT id FROM config WHERE parent=%s AND name=%s "
        params = [parent_id, name]
        if version:
            SQL += "AND version=%s"
            params.append(version)

        cursor = self._execute(SQL, params)
        row = cursor.fetchone()
        if not row:
            if DEBUG:
                self.log.debug("Tried to get parent id for %s but failed" %
                               parent_id)
            return None

        # self._id_cache[version][(parent_id, name)] = row[0]
        return row[0]

    def _get_full_path(self, full_path):
        if not full_path:
            return self.root[:-1]

        if full_path.startswith("root"):
            path = full_path[4:]
            if len(path) > 0 and path[0] == ".":
                path = path[1:]
            return path
        else:
            if self.root:
                if full_path:
                    return self.root + full_path
                else:
                    return self.root[:-1]
        return full_path

    def search(self, partial, version=None):
        """
        Search the config for a partial match
        """
        if not version:
            version = self._cfg["version"]
        else:
            version = self._get_version_id(version)

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

            SQL = "SELECT id, parent, name, value, datatype, version, last_modified, comment FROM config WHERE id=%s"
            cursor = self._execute(SQL, [param_id])
            row = cursor.fetchone()
            if not row:
                raise NoSuchParameterException("parameter number: %s" % param_id)

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
            absolute_path=False, add=True):
        """
        Get a ConfigParameter. Throws NoSuchParameterException if not found

        If version is specified, only matches are returned
        if version_id is specified, no text-to-id lookup is performed.
        if absolute_path is True, no root is added to the _full_path
        """

        with self._load_lock:
            if version_id:
                version = version_id
            else:
                if not version:
                    version = self._cfg["version"]
                else:
                    version = self._get_version_id(version)

            if not absolute_path:
                full_path = self._get_full_path(_full_path)
            else:
                full_path = _full_path
            try:
                return self._cache_lookup(version, full_path)
            except:
                # Cache miss
                pass

            if DEBUG:
                self.log.debug("get(%s, %s, %s)" % (_full_path, full_path, version))

            parent_ids = []
            if full_path.count(".") == 0:
                name = full_path
                path = ""
                parent_ids = [0]
            else:
                (path, name) = full_path.rsplit(".", 1)

            if full_path != "root" and full_path != "":  # special case for root node
                if len(parent_ids) == 0:
                    parent_ids = self._get_parent_ids(full_path, version, create=False)

                # Find the thingy
                SQL = "SELECT id, value, datatype, version, " + \
                    "last_modified, comment FROM config WHERE " +\
                    "name=%s AND parent=%s"
                params = [name, parent_ids[-1]]
                if version != _ANY_VERSION:
                    SQL += " AND version=%s"
                    params.append(version)
                cursor = self._execute(SQL, params)
                row = cursor.fetchone()
                if not row:
                    raise NoSuchParameterException("No such parameter: " + full_path)
                id, value, datatype, version, timestamp, comment = row
                cp = ConfigParameter(self, id, name, parent_ids, path,
                                     datatype, value,
                                     version, timestamp, config=self,
                                     comment=comment)
            else:
                cp = ConfigParameter(self, 0, "root", [0], "",
                                     "folder", "", version, None, config=self)

            if cp.datatype == "folder":
                timestamp, cp.children = self._get_children(cp)
                cp._set_last_modified(timestamp)

            self._cache_update(version, full_path, cp, time.time() + 0.2)
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
            my_timestamp = max(timestamp, my_timestamp)

        return my_timestamp, res

    def _get_datatype(self, value):
        if not value:
            datatype = "string"

        elif value.__class__ == float:
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
            c = self._execute("SELECT id, name FROM config WHERE parent=%s AND version=%s", [id, version])
            for row in c.fetchall():
                _rec_delete(row[0], version, path + "." + row[1])
            self._execute("DELETE FROM config WHERE id=%s AND version=%s", [id, version])
            self._cache_remove(version, path)

        with self._load_lock:
            param = self.get(full_path, version)
            _rec_delete(param._get_id(), param.get_version(), full_path)

    def add(self, _full_path, value=None, datatype=None, comment=None,
            version=None,
            parent_id=None, overwrite=False, version_id=None):
        """
        Add a new config parameter. If datatype is not specified,
        we'll guess.  If version is not specified, the current version
        is used.
        """

        with self._load_lock:
            full_path = self._get_full_path(_full_path)

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
                parent_ids = self._get_parent_ids(full_path, version)
                parent_id = parent_ids[-1]

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

            self._execute(SQL, (version, parent_id, name, value, datatype, comment))

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

    def set(self, _full_path, value, version=None, datatype=None, comment=None, version_id=None, absolute_path=False, check=True, create=False):

        with self._load_lock:
            if version and not version_id:
                version_id = self._get_version_id(version)
            try:
                param = self.get(_full_path, version_id=version_id, absolute_path=absolute_path)
            except Exception as e:
                if create:
                    self.add(_full_path, value, version=version, datatype=datatype, comment=comment, version_id=version_id)
                    param = self.get(_full_path, version_id=version_id, absolute_path=absolute_path)
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

            SQL = "UPDATE config SET value=%s,datatype=%s,comment=%s WHERE id=%s AND parent=%s AND version=%s"
            self._execute(SQL, [config_parameter.value,
                                config_parameter.datatype,
                                config_parameter.comment,
                                config_parameter.id,
                                config_parameter.parents[-1],
                                config_parameter.version])
        with self.callbackCondition:
            self._notify_counter += 1
            self.callbackCondition.notify()

    def get_leaves(self, _full_path=None, absolute_path=False):
        """
        Recursively return all leaves of the given path
        """
        with self._load_lock:
            param = self.get(_full_path, absolute_path=absolute_path)
            leaves = []
            folders = []
            for child in param.children:
                if child.datatype == "folder":
                    folders.append(child)
                else:
                    leaves.append(child.get_full_path()[len(self.root):])

            for folder in folders:
                leaves += self.get_leaves(folder.get_full_path(),
                                          absolute_path=True)

            return leaves

    def keys(self, path=None):
        """
        List the keys of this node (names of the children)
        """
        with self._load_lock:
            param = self.get(path)
            leaves = []
            for child in param.children:
                leaves.append(child.name)
            return leaves

    def get_version_info_by_id(self, version_id):
        """
        Return a map of version info
        """
        with self._load_lock:
            SQL = "SELECT name, device, comment FROM config_version WHERE id=%s"
            cursor = self._execute(SQL, [version_id])
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
        param = self.get(root, version_id=version_id, absolute_path=True)
        children = []
        for child in param.children:
            children.append(self._serialize_recursive(child.get_full_path(),
                                                      version_id))
        serialized = {"name": param.name,
                      "value": param.value,
                      "datatype": param.datatype,
                      "comment": param.comment,
                      "last_modified": param.last_modified}
        for child in children:
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
                        self._set(
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

        for elem in list(serialized.keys()):
            if elem in ["name", "datatype", "comment", "last_modified"]:
                continue

            path = root + "." + elem
            self._deserialize_recursive(serialized[elem], path, version_id,
                                        overwrite)

    # ##################    JSON functionality for (de)serializing ###

    def serialize(self, root="", version=None):
        """
        Return a JSON serialized block of config
        """
        with self._load_lock:
            if version:
                version_id = self._get_version_id(version)
            else:
                version_id = self._cfg["version"]

            version_info = self.get_version_info_by_id(version_id)
            root = self._get_full_path(root)
            serialized = self._serialize_recursive(root, version_id)
            if not root:
                root = "root"
            serialized = {root: serialized,
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
            self.get(name).set_value(value)
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
        except:
            return None

    def set_default(self, name, value, datatype=None):
        """
        Set the default value of a parameter.  The parameter will be created if it does not exist.  If the parameter was set, it will not be changed by this function.
        """
        # Check if the root of this thing exists
        try:
            self.keys()
        except Exception:
            try:
                self.add("root." + self.root + name, value, datatype=datatype)
            except Exception:     # Debug really
                print("Addition failed, check config log")
                self.log.exception("Addition of %s failed" % name)

            # Missing root!
            # raise Exception("Missing root %s for '%s'"%(self.root, name))
        try:
            self.get(name)
        except Exception:
            # self.log.exception("Get %s failed, adding" % name)
            self.add(name, value, datatype=datatype)

    def require(self, param_list):
        """
        Raise a NoSuchParameterException if any of the parameters are not
        available
        """
        with self._load_lock:
            # This could be done faster, but who cares
            for param in param_list:
                self.get(param)

    def last_updated(self, version=None):
        """
        Return the last update time for the given version
        """
        if version:
            version_id = self._get_version_id(version)
        else:
            version_id = self._get_version_id(self.version)

        c = self._execute("SELECT UNIX_TIMESTAMP(MAX(last_modified)) FROM config WHERE version=%s", [version_id])
        row = c.fetchone()
        return row[0]

    # ###############  Callback management ##################
    def _callback_thread_main(self):
        if DEBUG:
            self.log.debug("Callback thread started")
        last_notified = 0
        while not self._internal_stop_event.is_set() and not self.stop_event.is_set():
            try:
                SQL = "SELECT config_callback.id, config_callback.param_id, name, value, config.last_modified FROM config, config_callback WHERE config.id=config_callback.param_id AND config_callback.last_modified<config.last_modified"
                cursor = self._execute(SQL)

                for cb_id, param_id, name, value, last_modified in cursor.fetchall():
                    try:
                        with self._cb_lock:
                            if not (cb_id, param_id) in self._update_callbacks:
                                # self.log.error("Requested callback '%s' (%s) that no longer exists: %s" % (name, str((cb_id, param_id)), str(self._update_callbacks)))
                                continue
                            (func, args) = self._update_callbacks[(cb_id, param_id)]
                            param = self.get_by_id(param_id)
                            if not param:
                                raise Exception("INTERNAL: Got update on deleted parameter %d" % param_id)

                        if args:
                            func(param, *args)
                        else:
                            func(param)
                    except Exception as e:
                        print("CALLBACK EXCEPTION:", e)
                        self.log.exception("In callback handler")

                    self._execute("UPDATE config_callback SET last_modified=%s WHERE id=%s and param_id=%s", [last_modified, cb_id, param_id])

                # We use a condition variable to ensure that we are awoken immediately on local changes
                # we might however be notified multiple times, so we use a counter to ensure that we don't sleep
                # if there was a notify while we worked
                with self.callbackCondition:
                    if self._notify_counter == last_notified:
                        self.callbackCondition.wait(1)
                    else:
                        last_notified = self._notify_counter
            except:
                self.log.exception("INTERNAL: Callback handler crashed badly")

    def del_callback(self, callback_id):
        """
        Remove a callback, using the ID returned by add_callback
        """
        self._execute("DELETE FROM config_callback WHERE id=%s",
                      [callback_id])

        with self._cb_lock:
            for (cb_id, param_id) in list(self._update_callbacks.keys())[:]:
                del self._update_callbacks[(cb_id, param_id)]

    def add_callback(self, parameter_list, func, version=None, *args):
        """
        Add a callback for the given parameters.  Returns the ID of the callback (for use with del_callback)
        """
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

        if not self._cb_thread:
            self._cb_thread = threading.Thread(target=self._callback_thread_main)
            self._cb_thread.start()

        for param in parameter_list:
            param = self.get(param, version_id=version_id)
            # Add to the database as callbacks
            SQL = "INSERT INTO config_callback (id, param_id, last_modified) SELECT " + str(callback_id) + ", id, last_modified FROM config WHERE id=%s"
            self._execute(SQL, [param.id])
            with self._cb_lock:
                self._update_callbacks[(callback_id, param.id)] = (func, args)
            if DEBUG:
                self.log.debug("Added callback (%s,%s): %s" %
                               (callback_id, param.id,
                                str((func, args))))

        return callback_id


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "upgrade":
            cfg = Configuration()
            cfg._execute("alter table config modify last_modified timestamp(6)")
            cfg._execute("TRUNCATE TABLE config_callback")
            cfg._execute("alter table config_callback modify last_modified timestamp(6)")
