from CryoCore.Core import API, InternalDB


class LogDbReader(InternalDB.mysql):

    def __init__(self, name="System.Status.MySQL"):
        cfg = API.get_config(name)
        InternalDB.mysql.__init__(self, name, cfg, is_direct=False)
        self._id_cache = {}

    def get_updates(self, modules=[], loggers=[], since=0, max_lines=100, filter=None):
        """
        Get the values for all parameters in the list since a given time. If since is 0, only the last value is given
        filter must be a function that is passed each log line - the line will be
        added to the result only if it returns True
        """
        if since == 0:
            c = self._execute("SELECT MAX(id) FROM log")
            since = c.fetchone()[0] - max_lines

        retval = {"maxid": since, "logs": []}
        args = [since]
        SQL = "SELECT id, time, msecs, level, module, logger, line, function, message FROM log WHERE id>%s AND ("
        for module in modules:
            SQL += "module=%s OR "
            args.append(module)
        for logger in loggers:
            SQL += "logger=%s OR "
            args.append(logger)
        if SQL[-1] == "(":
            SQL = SQL[:-5]
        else:
            SQL = SQL[:-3] + ")"

        SQL += "ORDER BY id"

        cursor = self._execute(SQL, args)
        for id, ts, msecs, level, module, logger, line, function, message in cursor.fetchall():
            log = (id, ts, msecs, level, module, logger, line, function, message)
            if not filter or filter(log):
                retval["logs"].append(log)
            retval["maxid"] = max(retval["maxid"], id)

        return retval

if __name__ == "__main__":
    # DEBUG
    try:
        db = LogDbReader()
        since = 0
        import time

        def filter(log):
            if log[5].find("Worker") > -1:
                return True
            if log[5].endswith("HeadNode"):
                return True
            return False

        for i in range(0, 10):
            data = db.get_updates(since=since, filter=filter)
            since = data["maxid"]
            for line in data["logs"]:
                print(line)
            time.sleep(1)
    finally:
        API.shutdown()
