import time
from CryoCore.Core import API, InternalDB


class StatusDbReader(InternalDB.mysql):

    def __init__(self, name="System.Status.MySQL"):
        cfg = API.get_config(name)
        InternalDB.mysql.__init__(self, name, cfg, is_direct=False)
        self._id_cache = {}

    def _cache_lookup(self, chan, param):
        if (chan, param) not in self._id_cache:
            # Resolve
            self._id_cache[(chan, param)] = self.get_param_id(chan, param)
        return self._id_cache[(chan, param)]

    def get_channel_id(self, channel):
        """
        Return the parameter ID of the given channel, name
        """
        SQL = "SELECT chanid FROM status_channel WHERE name=%s"
        cursor = self._execute(SQL, (channel))
        if cursor.rowcount == 0:
            raise Exception("Missing channel %s" % (channel))
        return cursor.fetchone()[0]

    def get_param_id(self, channel, name):
        """
        Return the parameter ID of the given channel, name
        """
        SQL = "SELECT paramid FROM status_parameter,status_channel WHERE status_channel.name=%s AND status_parameter.name=%s AND status_parameter.chanid=status_channel.chanid"
        cursor = self._execute(SQL, (channel, name))
        if cursor.rowcount == 0:
            raise Exception("Missing parameter %s.%s" % (channel, name))
        row = cursor.fetchone()
        if row is None:
            raise Exception("Missing parameter '%s' in channel '%s'" % (name, channel))
        return row[0]

    def get_last_status_values(self, paramlist, since=-60, now=None):
        """
        Paramlist must be (channel, name), since must be a value.
        This method will fetch all updates for the given list since
        the since time, so it might take more resources than you expect.
        If since is a negative number, it will be regarded as now-time.
        since=-60 means the last 60 seconds.
        if now is given, that will be used as the max time, and a negative
        since will be in relation to the 'now' value
        Returns a map (channel, param) -> (timestamp, value)
        """
        if len(paramlist) == 0:
            raise Exception("Need parameter list")
        rev = {}
        params = []
        for channel, name in paramlist:
            paramid = self._cache_lookup(channel, name)
            params.append(paramid)
            rev[paramid] = (channel, name)
        if since < 0:
            if now:
                since = now + since
            else:
                since = time.time() + since
        args = []
        if now:
            extra = "timestamp<%s AND"
            args.append(now)
        else:
            extra = ""

        SQL = "SELECT paramid, timestamp, value FROM status WHERE " + extra + " timestamp>%s AND ("
        args.append(since)
        for p in params:
            SQL += "paramid=%s OR "
            args.append(p)

        SQL = SQL[:-3] + ") ORDER BY timestamp"
        cursor = self._execute(SQL, args)
        ret = {}
        for paramid, timestamp, value in cursor.fetchall():
            # This will overwrite the value, so only the last will be returned
            ret[rev[paramid]] = (timestamp, value)
        return ret

    def get_last_status_value(self, channel, name):
        """
        Return the last (timestamp, value) of the given parameter
        """
        try:
            paramid = self._cache_lookup(channel, name)
        except:
            return (None, None)

        # SQL = "SELECT timestamp, value FROM status WHERE id=(SELECT max(id) FROM status WHERE paramid=%s)"
        SQL = "SELECT timestamp, value FROM status WHERE paramid=%s ORDER BY id DESC LIMIT 1"
        cursor = self._execute(SQL, [paramid])
        # SQL = "SELECT timestamp, value FROM status,status_parameter,status_channel WHERE status_channel.name=%s AND status_parameter.name=%s AND status_parameter.chanid=status_channel.chanid AND status.paramid=status_parameter.paramid ORDER BY id DESC LIMIT 1"
        # cursor = self._execute(SQL, (channel, name))
        row = cursor.fetchone()
        if not row:
            return (None, None)
        return (row[0], row[1])

    def get_updates(self, paramlist, since=0):
        """
        Get the values for all parameters in the list since a given time. If since is 0, only the last value is given
        """
        retval = {"maxid": since, "params": {}}
        if since == 0:
            SQL = "SELECT id, timestamp, value FROM status WHERE paramid=%s ORDER BY id DESC LIMIT 1"
            for i in paramlist:
                cursor = self._execute(SQL, [i])
                row = cursor.fetchone()
                if row:
                    id, ts, val = row
                    retval["params"][i] = {"ts": ts, "val": val}
                    retval["maxid"] = max(retval["maxid"], id)
        else:
            # Get updates
            SQL = "SELECT id, paramid, timestamp, value FROM status WHERE id>%s AND ("
            for i in paramlist:
                SQL += "paramid=%s OR "
            SQL = SQL[:-4] + ") order by timestamp"
            cursor = self._execute(SQL, [since] + paramlist)
            for id, paramid, ts, val in cursor.fetchall():
                retval["params"][paramid] = {"ts": ts, "val": val}
                retval["maxid"] = max(retval["maxid"], id)

        return retval

    def get_last_status_value_by_id(self, paramid):
        """
        Return the last (timestamp, value) of the given parameter
        """
        SQL = "SELECT timestamp, value FROM status WHERE paramid=%s ORDER BY id DESC LIMIT 1"
        cursor = self._execute(SQL, [paramid])
        row = cursor.fetchone()
        if not row:
            return (None, None)
        return (row[0], row[1])

    def get_min_timestamp(self):
        """
        Return the earliest timestamp in the database
        """
        row = self._execute("SELECT MIN(timestamp) FROM status").fetchone()
        if row:
            return row[0]
        else:
            return None

    def get_max_timestamp(self):
        """
        Return the latest timestamp from the database
        """
        row = self._execute("SELECT MAX(timestamp) FROM status").fetchone()
        if row:
            return row[0]
        else:
            return None

    def get_channels(self):
        """
        Return a list of all available channels
        """
        SQL = "SELECT name FROM status_channel"
        cursor = self._execute(SQL)
        retval = []
        for row in cursor.fetchall():
            retval.append(row[0])
        return retval

    def get_parameters(self, channel):
        """
        Return a list of all available parameters on a channel
        """
        SQL = "SELECT status_parameter.name FROM status_parameter,status_channel WHERE status_channel.name='%s' AND status_channel.chanid=status_parameter.chanid" % channel
        cursor = self._execute(SQL)
        retval = []
        for row in cursor.fetchall():
            retval.append(row[0])
        return retval

    def get_parameters_by_name(self, name):
        """
        Return a list of [channel, paramid] for parameters with a given name
        """
        SQL = "SELECT chanid, name FROM status_parameter WHERE name=%s"
        cursor = self._execute(SQL, [name])
        retval = []
        for row in cursor.fetchall():
            retval.append((row[0], row[1]))
        return retval

    def get_channel_name(self, chanid):
        cursor = self._execute("SELECT name FROM status_channel WHERE chanid=%s", [chanid])
        return cursor.fetchone()[0]

    def get_channels_and_parameters(self):

        cursor = self._execute("SELECT paramid, s.name, c.name FROM "
                               "status_parameter as s, status_channel as c "
                               "WHERE c.chanid=s.chanid ORDER BY c.name, s.name")
        retval = {}
        for paramid, name, channel in cursor.fetchall():
            if channel not in retval:
                retval[channel] = {}
            retval[channel][name] = paramid
        return retval

if __name__ == "__main__":
    # DEBUG
    db = StatusDbReader()
    since = 0
    import time
    for i in range(0, 10):
        data = db.get_updates([361, 8, 2, 4, 5], since)
        since = data["maxid"]
        print(data)
        time.sleep(1)
    API.shutdown()
