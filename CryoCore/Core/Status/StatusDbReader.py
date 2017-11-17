from CryoCore.Core import API, InternalDB


class StatusDbReader(InternalDB.mysql):

    def __init__(self, name="System.Status.MySQL"):
        cfg = API.get_config(name)
        InternalDB.mysql.__init__(self, name, cfg, is_direct=True)
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
        SQL = "SELECT chanid FROM status_channel WHERE name=?"
        cursor = self._execute(SQL, (channel))
        if cursor.rowcount == 0:
            raise Exception("Missing channel %s" % (channel))
        return cursor.fetchone()[0]

    def get_param_id(self, channel, name):
        """
        Return the parameter ID of the given channel, name
        """
        SQL = "SELECT paramid FROM status_parameter,status_channel WHERE status_channel.name=? AND status_parameter.name=? AND status_parameter.chanid=status_channel.chanid"
        cursor = self._execute(SQL, (channel, name))
        if cursor.rowcount == 0:
            raise Exception("Missing parameter %s.%s" % (channel, name))
        return cursor.fetchone()[0]

    def get_last_status_value(self, channel, name):
        """
        Return the last (timestamp, value) of the given parameter
        """
        paramid = self._cache_lookup(channel, name)

        SQL = "SELECT timestamp, value FROM status WHERE id=(SELECT max(id) FROM status WHERE paramid=?)"
        # SQL = "SELECT timestamp, value FROM status WHERE paramid=? ORDER BY id DESC LIMIT 1"
        cursor = self._execute(SQL, [paramid])
        # SQL = "SELECT timestamp, value FROM status,status_parameter,status_channel WHERE status_channel.name=? AND status_parameter.name=? AND status_parameter.chanid=status_channel.chanid AND status.paramid=status_parameter.paramid ORDER BY id DESC LIMIT 1"
        # cursor = self._execute(SQL, (channel, name))
        row = cursor.fetchone()
        if not row:
            return (None, None)
        return (row[0], row[1])

    def get_last_status_value_by_id(self, paramid):
        """
        Return the last (timestamp, value) of the given parameter
        """
        SQL = "SELECT timestamp, value FROM status WHERE id=(SELECT max(id) FROM status WHERE paramid=?)"
        cursor = self._execute(SQL, (paramid))
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
