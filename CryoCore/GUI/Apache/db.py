import sys
import time

from CryoCore import API
from CryoCore.Tools import TailLog
from CryoCore.Core.InternalDB import mysql as sqldb

global channel_ids
global param_ids
channel_ids = {}
param_ids = {}
DEBUG = False

API.__is_direct = True  # Apache runs these as separate instances - WE WANT SPEED!


def toUnicode(string):
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
        return str(string, "latin-1")
    if string.__class__ == unicode:
        return string
    try:
        return unicode(string, "utf-8")
    except:
        pass
    return unicode(string, "latin-1")


def toBytes(val):
    if sys.version_info.major == 3:
        if val.__class__ == str:
            return val.encode("utf-8")
        return val
    if val.__class__ == unicode:
        return val.encode("utf-8")
    return val


class LogDB(TailLog.TailLog):

    def __init__(self):
        TailLog.TailLog.__init__(self)

    def get_changes_by_time(self, args,
                            start_time,
                            stop_time):
        # TODO: Also do sessions, times and stuff here
        if args:
            sql = "AND " + args
        else:
            sql = ""

        cursor = self._execute("SELECT * FROM log WHERE timestamp>? AND timestamp<? AND %s ORDER BY id" % sql,
                               [start_time, stop_time])
        return cursor

    def get_changes_by_id(self, args, min_id, limit):
        """
        return updates since last time and
        """
        _last_id = min_id
        if not _last_id:
            cursor = self._execute("SELECT MAX(id) FROM log")
            row = cursor.fetchone()
            if row[0]:
                _last_id = max(row[0] - 10, 0)

        SQL = "SELECT * FROM log WHERE id>? "
        params = [_last_id]
        if len(args) > 0:
            SQL += "AND %s " % args[0]
            params += args[1]
        SQL += "ORDER BY id DESC "
        if limit:
            SQL += "LIMIT %s"
            params.append(int(limit))
        cursor = self._execute(SQL, params)
        return cursor

    def get_modules(self):
        """
        Return a list of modules that have been logging
        """
        cursor = self._execute("SELECT DISTINCT(module) FROM log")
        return cursor

    def get_log_levels(self):
        """
        Return a list of textual log levels that are available
        """
        return list(API.log_level_str.keys())


class DBWrapper:
    def __init__(self, status_db="System.Status.MySQL"):
        self.status_db = status_db
        self._dbs = {}

    def get_db(self, db_name=None):
        if db_name not in list(self._dbs.keys()):
            self._dbs[db_name] = StatusDB(db_name=db_name, status_db=self.status_db)

        return self._dbs[db_name]


class StatusDB(sqldb):

    def __init__(self, name="WebGui.StatusDB", db_name=None, status_db="System.Status.MySQL"):

        self._cached_params = {}
        self.name = name
        self.log = API.get_log(self.name)
        cfg = API.get_config(status_db)
        if not cfg["db_name"]:
            cfg = API.get_config("System.InternalDB")
        sqldb.__init__(self, name, cfg, db_name=db_name)

        init_sqls = ["""CREATE TABLE IF NOT EXISTS items (
item_key VARCHAR(160) PRIMARY KEY,
value TEXT)""",
                     """set sql_mode="STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
                     """]
        self._init_sqls(init_sqls)

    def _get_param_info(self, paramid):
        """
        Get the channel and name of a parameter
        """
        if paramid not in self._cached_params:
            SQL = "SELECT status_channel.name,status_parameter.name FROM status_channel,status_parameter "\
                "WHERE status_parameter.chanid=status_channel.chanid AND paramid=%s"
            cursor = self._execute(SQL, [paramid])
            row = cursor.fetchone()
            if not row:
                SQL = "SELECT status_channel.name,status_parameter2d.name, sizex, sizey FROM status_channel,status_parameter2d "\
                    "WHERE status_parameter2d.chanid=status_channel.chanid AND paramid=%s"
                cursor = self._execute(SQL, [paramid])
                row = cursor.fetchone()
                if not row:
                    raise Exception("No parameter '%s'" % paramid)
            return {"channel": row[0],
                    "name": row[1],
                    "type": "2d",
                    "size": (sizex, sizey)}
            # self._cached_params[paramid] = {"channel": row[0],
            #                                "name": row[1],
            #                                "type": "2d",
            #                                "size": (sizex, sizey)}
        return self._cached_params[paramid]

    # ########## Available interface ############

    def get_last_status_values(self, session, max_time=None):
        """
        Return the last (paramid, id, timestamp, value) of all parameters of a session
        """
        res = []
        SQL = "SELECT paramid FROM status_view_mapping WHERE viewid=%s"
        cursor = self._execute(SQL, [session["id"]])
        for row in cursor.fetchall():
            SQL2 = "SELECT id, timestamp, value FROM status WHERE paramid=%s "
            paramid = row[0]
            params = [paramid]
            if max_time:
                SQL2 += "AND timestamp<%s "
                params.append(max_time)
            SQL2 += "ORDER BY timestamp DESC LIMIT 1"
            cursor2 = self._execute(SQL2, params)
            for num, ts, val in cursor2.fetchall():
                res.append((num, ts, paramid, val))
        return res

    def get_timestamps(self):
        """
        Return the smallest and largest timestamp (for relative clocks)
        """
        cursor = self._execute("SELECT MIN(timestamp), MAX(timestamp) FROM status")
        try:
            row = cursor.fetchone()
            return {"min": row[0], "max": row[1]}
        except:
            return {}

    def get_op_clock(self, session, max_time=None, since=0):
        # Must get the max op_clock of all params of a session
        params = [since]
        if max_time:
            m = "timestamp < %s AND "
            params.append(max_time)
        else:
            m = ""
        SQL = "SELECT MAX(id) FROM status,status_view_mapping WHERE id>%s AND " + m + "viewid=%s AND status_view_mapping.paramid=status.paramid"
        params.append(session["id"])

        cursor = self._execute(SQL, params)
        row = cursor.fetchone()
        if not row:
            return since
        if row[0] is None:
            return since
        return int(row[0])

    def get_channels(self):
        cursor = self._execute("SELECT chanid, name FROM status_channel ORDER BY name")
        global channel_ids
        channels = []
        for row in cursor.fetchall():
            if row[1] not in channel_ids:
                channel_ids[row[1]] = row[0]
            channels.append(row[1])
        return channels

    def get_params(self, channel):
        if channel not in channel_ids:
            self.get_channels()

        cursor = self._execute("SELECT paramid, name FROM status_parameter WHERE chanid=%s ORDER BY name", [channel_ids[channel]])
        if DEBUG:
            self.log.debug("SELECT paramid, name FROM status_parameter WHERE chanid=%s ORDER BY name" + str([channel_ids[channel]]))
        params = []
        global param_ids
        for row in cursor.fetchall():
            fullname = channel + "." + row[1]
            if fullname not in param_ids:
                param_ids[fullname] = row[0]
            params.append(row[1])

        cursor = self._execute("SELECT paramid, name FROM status_parameter2d WHERE chanid=%s ORDER BY name", [channel_ids[channel]])
        for row in cursor.fetchall():
            fullname = channel + "." + row[1]
            if fullname not in param_ids:
                param_ids[fullname] = row[0]
            params.append(row[1])

        return params

    def get_params_with_ids(self, channel):
        if channel not in channel_ids:
            self.get_channels()

        cursor = self._execute("SELECT paramid, name FROM status_parameter WHERE chanid=%s ORDER BY name", [channel_ids[channel]])
        if DEBUG:
            self.log.debug("SELECT paramid, name FROM status_parameter WHERE chanid=%s ORDER BY name" + str([channel_ids[channel]]))
        params = []
        global param_ids
        for row in cursor.fetchall():
            fullname = channel + "." + row[1]
            if fullname not in param_ids:
                param_ids[fullname] = row[0]
            params.append((row[0], row[1]))

        cursor = self._execute("SELECT paramid, name, sizex, sizey FROM status_parameter2d WHERE chanid=%s ORDER BY name", [channel_ids[channel]])
        if DEBUG:
            self.log.debug("SELECT paramid, name FROM status_parameter2d WHERE chanid=%s ORDER BY name" + str([channel_ids[channel]]))
        global param_ids
        for row in cursor.fetchall():
            fullname = channel + "." + row[1]
            if fullname not in param_ids:
                param_ids[fullname] = row[0]
            params.append(("2d%d" % row[0], row[1], (row[2], row[3])))

        return params

    def get_data(self, params, start_time, end_time, since=0, since2d=0, aggregate=None, lastValues=False):
        if len(params) == 0:
            raise Exception("No parameters given")
        # print("Getting data between %s and %s (%s)" % (float(start_time) - time.time(), float(end_time) - time.time(), start_time))
        dataset = {}
        params2d = []
        max_id = since
        p = [start_time, end_time, since]
        SQL = "SELECT id, paramid, timestamp, value FROM status WHERE timestamp>%s AND timestamp<%s AND id> %s AND ("
        for param in params:
            if param.startswith("2d"):
                params2d.append(param[2:])
                continue
            SQL += "paramid=%s OR "
            p.append(param)
        if aggregate is not None:
            SQL = SQL[:-4] + ") GROUP BY paramid, timestamp DIV %s" % float(aggregate)
        else:
            SQL = SQL[:-4] + ") ORDER BY paramid, timestamp"
        if len(p) > 3:
            cursor = self._execute(SQL, p)
            for i, p, ts, v in cursor.fetchall():
                max_id = max(max_id, i)
                if p not in dataset:
                    dataset[p] = []

                try:
                    v = float(v)
                except:
                    pass
                dataset[p].append((ts, v))

        max_id2d = since2d
        if len(params2d) > 0:
            p = [start_time, end_time, since2d]
            SQL = "SELECT id, paramid, timestamp, value, posx, posy FROM status2d WHERE timestamp>%s AND timestamp<%s AND id> %s AND ("
            for param in params2d:
                SQL += "paramid=%s OR "
                p.append(param)
            if aggregate is not None:
                SQL = SQL[:-4] + ") GROUP BY paramid, timestamp DIV %d" % aggregate
            else:
                SQL = SQL[:-4] + ") ORDER BY paramid, id"

            cursor = self._execute(SQL, p)
            for i, p, ts, v, posx, posy in cursor.fetchall():
                p = "2d%d" % p
                max_id2d = max(max_id2d, i)
                if p not in dataset:
                    dataset[p] = []

                try:
                    v = int(v)
                except:
                    pass
                dataset[p].append((ts, v, (posx, posy)))

        if lastValues:
            missing = []
            for param in params:
                if param not in dataset:
                    missing.append(param)

            for param in missing:
                # Must look for the LAST value of the missing parameters
                if param not in params2d:
                    SQL = "SELECT id, paramid, timestamp, value FROM status WHERE paramid=%s ORDER BY id DESC LIMIT 1"
                else:
                    SQL = "SELECT id, paramid, timestamp, value, posx, posy FROM status2d WHERE paramid=%s ORDER BY id DESC LIMIT 1"
                c = self._execute(SQL, [param])
                row = c.fetchone()
                if row:
                    if param not in params2d:
                        i, p, ts, v = row
                        dataset[p] = [(ts, v)]
                    else:
                        i, p, ts, v, posx, posy = row
                        dataset[p] = [(ts, v, (posx, posy))]

        return max_id, max_id2d, dataset

    def get_max_data(self, params, start_time, end_time, since=0, aggregate=None):
        if len(params) == 0:
            raise Exception("No parameters given")

        if not aggregate:
            raise Exception("Need aggregate value")

        p = [start_time, end_time, since]
        SQL = "SELECT timestamp, max(value) FROM status WHERE timestamp>%s AND timestamp<%s AND id> %s AND ("
        for param in params:
            SQL += "paramid=%s OR "
            p.append(param)
        SQL = SQL[:-4] + ") GROUP BY timestamp DIV %d" % int(aggregate)

        API.get_log("WS").debug(SQL + "," + str(p))
        cursor = self._execute(SQL, p)
        dataset = {}
        for ts, v in cursor.fetchall():
            try:
                v = float(v)
            except:
                pass
            dataset[ts] = v
        return 0, dataset

    def add_item(self, key, item):
        SQL = "REPLACE INTO items (item_key, value) VALUES (%s, %s)"
        self._execute(SQL, [key, item])

    def get_item(self, key):
        SQL = "SELECT value FROM items WHERE item_key=%s"
        cursor = self._execute(SQL, [key])
        row = cursor.fetchone()
        if row:
            return row[0]
        return ""

    def list_items(self):
        SQL = "SELECT item_key FROM items"
        cursor = self._execute(SQL)
        res = []
        for row in cursor.fetchall():
            res.append(row[0])
        return res

    def log_search(self, keywords, lines, last_id, minlevel):
        if keywords.__class__ != list:
            raise Exception("Keywords must be a list")
        if last_id is None:
            # Go back at most 500 messages
            cursor = self._execute("SELECT MAX(id) FROM log")
            row = cursor.fetchone()
            if row[0]:
                last_id = max(row[0] - int(lines), 0)
        level = API.log_level_str[minlevel.upper()]
        SQL = "SELECT * FROM log WHERE level>=%d" % level + " AND id>" + str(last_id) + " AND "
        for arg in keywords:
            arg = arg.replace("'", "\'")
            SQL += "(module LIKE '%%" + arg + "%%' OR logger LIKE '%%" + arg + "%%' OR message LIKE '%%" + arg + "%%' ) OR "
        SQL = SQL[:-3] + "ORDER BY id"
        # return last_id, [SQL]

        logs = []
        max_id = 0
        cursor = self._execute(SQL)
        for row in cursor.fetchall():
            max_id = max(max_id, row[0])
            logs.append(list(row))

        return max_id, logs

    def log_getlist(self):

        SQL = "SELECT DISTINCT(module) FROM log"
        modules = list(self._execute(SQL).fetchall())
        SQL = "SELECT DISTINCT(logger) FROM log"
        loggers = list(self._execute(SQL).fetchall())
        return modules, loggers

    def log_getlines(self, last_id=None, start=None, end=None, modules="", loggers="", lines=500, minlevel="DEBUG"):

        modules = None
        loggers = None
        if last_id:
            last_id = int(last_id)
        if not last_id and (start is None):
            raise Exception("Need starttime if last_id is not given")
        if start and end is None:
            end = time.time()

        # Look for log message
        args = [API.log_level_str[minlevel.upper()]]
        SQL = "SELECT * FROM log WHERE level>=%s AND "
        if last_id > 0:
            SQL += "id>%s AND "
            args.append(last_id)
        if modules or loggers:
            SQL += "("

            if modules:
                for module in modules:
                    SQL += "UPPER(module=%s) OR "
                    args.append(module.upper())
                SQL = SQL[: -4]

            if loggers:
                for logger in loggers:
                    SQL += "UPPER(logger=%s) OR "
                    args.append(logger.upper())
                SQL = SQL[: -4]
            SQL += ") AND"
        if start:
            SQL += "time>%s AND "
            args.append(float(start))
        if end:
            SQL += "time<%s AND "
            args.append(float(end))
        SQL = SQL[:-5]
        SQL += "ORDER BY time LIMIT %s"
        args.append(min(1000, lines))

        # return args, [SQL]

        cursor = self._execute(SQL, args)
        max_id = 0
        logs = []
        for row in cursor.fetchall():
            max_id = max(row[0], max_id)
            logs.append(row)
        return max_id, logs

    def get_log_levels(self):
        """
        Return a list of textual log levels that are available
        """
        return API.log_level_str
