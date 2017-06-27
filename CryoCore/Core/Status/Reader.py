class StatusDB(db):
    def __init__(self, name="StatusReader"):

        self._cached_params = {}
        self._reverse = {}
        self.name = name
        self.log = API.get_log(self.name)
        cfg = API.get_config("System.Status.MySQL")
        db.__init__(self, name, cfg)

    def get_param_id(self, key, short):
        """
        Resolve and cache
        """
        if key not in list(self._cached_params.keys()):
            channel, parameter = key.split(":")

            SQL = "SELECT paramid FROM status_channel,status_parameter WHERE status_channel.chanid=status_parameter.chanid AND status_channel.name=%s AND status_parameter.name=%s"
            cursor = self._execute(SQL, [channel, parameter])
            try:
                self._cached_params[key] = cursor.fetchone()[0]
                self._reverse[self._cached_params[key]] = short
            except:
                raise Exception("Missing parameter '%s' for '%s'" % (key, short))
        return self._cached_params[key]

    def get_state(self, params):
        """
        Params must be a map of configparam:short_name, e.g. "Instruments.GPS.lat":"lat"
        """
        args = []
        SQL = "SELECT paramid, value FROM status WHERE "
        for key in list(params.keys()):
            SQL += "paramid=%s OR "
            args.append(self.get_param_id(key, params[key]))
        SQL = SQL[:-4]
        cursor = self._execute(SQL, args)

        status = {}
        for paramid, value in cursor.fetchall():
            status[self._reverse[paramid]] = value
        return status
