import time

from CryoCore.Core import API, InternalDB
from CryoCore.Core.Status import Status
import threading
import sys
if sys.version_info.major == 3:
    import queue
else:
    import Queue as queue
DEBUG = True


class MySQLStatusReporter(Status.OnChangeStatusReporter, InternalDB.mysql, threading.Thread):

    def __init__(self, name="System.Status.MySQL"):
        """
        Log messages to a database on change
        """
        threading.Thread.__init__(self)
        Status.OnChangeStatusReporter.__init__(self, name)
        self.name = name
        self.cfg = API.get_config(name)
        self.log = API.get_log(name)
        InternalDB.mysql.__init__(self, "MySQLStatusReporter", self.cfg)
        self.cfg.set_default("isconfigured", False)
        self._channels = {}
        self._parameters = {}
        self.tasks = queue.Queue()

        self._addLock = threading.Lock()
        self._addList = []
        self._addTimer = None

        self.start()

    def run(self):
        if API.api_auto_init:
            self._prepare_db()

        # Thread entry point
        stop_time = None
        last_clean = 0
        while True:  # Must loop - we only exit when idle, to ensure that we flush all items
            try:
                (event, ts, value) = self.tasks.get(block=True, timeout=1.0)
                # Should insert something
                # self._execute(sql, args)
                self._async_report(event, ts, value)
            except queue.Empty:
                if API.api_stop_event.is_set():
                    if stop_time is None:
                        stop_time = time.time()
                        continue
                    if time.time() - stop_time < 5:
                        # We have a few seconds "grace time" to ensure that we get all status messages
                        continue
                    break
                if time.time() - last_clean > 300:
                    last_clean = time.time()
                    self._clean_expired()  # Clean, we're idling
            except Exception as e:
                self.log.exception("Async status reporting fail")
                print("Async exception on status reporting", e)

    def _clean_expired(self):
        ts = time.time()
        try:
            self._execute("DELETE FROM status WHERE expires<%s", [ts])
            self._execute("DELETE FROM status2d WHERE expires<%s", [ts])
        except:
            self.log.exception("While cleaning expired status")

    def _prepare_db(self):
        """
        This function will prepare the db for a first utilisation
        It will create tables if needed
        """
        statements = [""" CREATE TABLE IF NOT EXISTS status_channel (
                      chanid INTEGER PRIMARY KEY AUTO_INCREMENT,
                      name VARCHAR(180) UNIQUE)""",

                      """CREATE TABLE IF NOT EXISTS status_parameter (
                      paramid INTEGER PRIMARY KEY AUTO_INCREMENT,
                      name VARCHAR(180),
                      chanid INTEGER NOT NULL,
                      UNIQUE KEY uid (name,chanid))""",

                      """CREATE TABLE IF NOT EXISTS status  (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            timestamp DOUBLE,
            paramid INTEGER REFERENCES status_parameter(paramid),
            chanid INTEGER REFERENCES status_channel(chanid),
            value VARCHAR(2048),
            aux INTEGER DEFAULT NULL,
            expires DOUBLE DEFAULT NULL
            )""",
                      """CREATE TABLE IF NOT EXISTS status_parameter2d (
                      paramid INTEGER PRIMARY KEY AUTO_INCREMENT,
                      name VARCHAR(128),
                      chanid INTEGER NOT NULL,
                      sizex SMALLINT NOT NULL,
                      sizey SMALLINT NOT NULL,
                      UNIQUE KEY uid (name,chanid))""",
                      """CREATE TABLE IF NOT EXISTS status2d  (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            timestamp DOUBLE,
            paramid INTEGER REFERENCES status_parameter2d(paramid),
            chanid INTEGER REFERENCES status_channel(chanid),
            posx SMALLINT,
            posy SMALLINT,
            value TINYINT UNSIGNED,
            expires DOUBLE DEFAULT NULL
            )""",

                      "CREATE INDEX stat_time ON status(timestamp)",
                      "CREATE INDEX stat_chanid ON status(chanid)",
                      "CREATE INDEX stat_paramid ON status(paramid)",
                      "CREATE INDEX stat_exp ON status(expires)",
                      "CREATE INDEX stat_aux ON status(aux)"]
        self._init_sqls(statements)
        self.cfg["isprepared"] = True

    def _update_event_ids(self, event, is2D=False):
        """
        Update DB ID's for this event
        """
        if not event._db_channel_id:
            holder_name = event.status_holder.get_name()
            if holder_name not in list(self._channels.keys()):
                SQL = "SELECT chanid FROM status_channel WHERE name=%s"
                cursor = self._execute(SQL, [holder_name])
                row = cursor.fetchone()
                if not row:
                    # Must insert
                    if DEBUG:
                        self.log.debug("New channel '%s'" % holder_name)
                    self._execute("INSERT INTO status_channel(name) VALUES (%s)",
                                  [holder_name])

                    return self._update_event_ids(event, is2D)  # Slightly dangerous, but should be OK as exceptions will break it
                self._channels[holder_name] = row[0]

            event._db_channel_id = self._channels[holder_name]

        if not event._db_param_id:
            param_name = event.get_name()
            if not (event._db_channel_id, param_name) in self._parameters:
                if is2D:
                    SQL = "SELECT paramid FROM status_parameter2d WHERE name=%s AND chanid=%s"
                else:
                    SQL = "SELECT paramid FROM status_parameter WHERE name=%s AND chanid=%s"
                assert event.get_name()
                assert event._db_channel_id
                cursor = self._execute(SQL, [param_name, event._db_channel_id])
                row = cursor.fetchone()
                if not row:
                    # Must insert
                    if DEBUG:
                        self.log.debug("New status parameter '%s'" % param_name)
                    if is2D:
                        self._execute("INSERT INTO status_parameter2d(name, chanid, sizex, sizey) VALUES (%s, %s, %s, %s)",
                                      [param_name, event._db_channel_id, event.size[0], event.size[1]])
                    else:
                        self._execute("INSERT INTO status_parameter(name, chanid) VALUES (%s, %s)",
                                      [param_name, event._db_channel_id])
                    return self._update_event_ids(event)  # Slightly dangerous, but should be OK as exceptions will break it
                else:
                    event._db_param_id = row[0]
                # If this is a 2d parameter, also update the size in case it changed
                if is2D:
                    self.log.debug("Updating status2d parameter %s with size %s" % (event._db_param_id, str(event.size)))
                    self._execute("UPDATE status_parameter2d SET sizex=%s, sizey=%s WHERE paramid=%s", [event.size[0], event.size[1], event._db_param_id])

                    # Delete this parameter if it's already a one dimensional one
                    self._execute("DELETE FROM status_parameter WHERE chanid=%s AND name=%s", [event._db_channel_id, param_name])

                self._parameters[(event._db_channel_id, param_name)] = row[0]
            event._db_param_id = self._parameters[(event._db_channel_id, param_name)]

    def report(self, event):
        """
        Report to DB
        """
        if (event.type.startswith("2d")):
            self.tasks.put((event, event.get_timestamp(), event.get_last_update()))
        else:
            self.tasks.put((event, event.get_timestamp(), str(event.get_value())))

    def _async_report2d(self, event, ts, value):
        try:
            if not event._db_param_id or not event._db_channel_id:
                self._update_event_ids(event, is2D=True)
            assert event._db_param_id
            assert event._db_channel_id
        except:
            self.log.exception("Could not resolve event ids for event")
            return

        aux = event.aux
        if not aux:
            aux = self.cfg["aux"]
        ts = event.get_timestamp()
        pos = value[0]
        val = value[1]
        if pos is None:  # Special initializiation of 2D elements
            if event.initial_value is not None:
                exp = event.get_expire_time()
                SQL = "INSERT INTO status2d(timestamp, paramid, chanid, posx, posy, value, expires) VALUES "
                args = []
                for x in range(0, event.size[0]):
                    for y in range(0, event.size[1]):
                        SQL += "(%s, %s, %s, %s, %s, %s, %s),"
                        args.extend([ts, event._db_param_id, event._db_channel_id, x, y, event.initial_value, exp])
                self._execute(SQL[:-1], args)
            return

        if event.resized:
            print("RESIZING PARAMETER %s TO (%s)" % (event._db_param_id, event.size))
            SQL = "UPDATE status_parameter2d SET sizex=%s, sizey=%s WHERE paramid=%s"
            self._execute(SQL, [event.size[0], event.size[1], event._db_param_id])

        try:
            params = [ts, event._db_param_id, event._db_channel_id, pos[0], pos[1], val, event.get_expire_time()]
            if aux and aux != "none":
                SQL = "INSERT INTO status2d(timestamp, paramid, chanid, posx, posy, value, expires, aux) "\
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                params.append(aux)
            else:
                SQL = "INSERT INTO status2d(timestamp, paramid, chanid, posx, posy, value, expires) "\
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
            self._execute(SQL, params)
        except Exception:
            self.log.exception("Updating status2d information %s.%s" % (event.status_holder.get_name(), event.get_name()))

    def _async_report(self, event, ts, value):
        if (event.type.startswith("2d")):
            self._async_report2d(event, ts, value)
            return
        try:
            if not event._db_param_id or not event._db_channel_id:
                self._update_event_ids(event)
            assert event._db_param_id
            assert event._db_channel_id
        except:
            self.log.exception("Could not resolve event ids for event")
            return

        aux = event.aux
        if not aux:
            aux = self.cfg["aux"]
        if aux == "none":
            aux = None
        ts = event.get_timestamp()
        params = [ts, event._db_param_id, event._db_channel_id, value, event.get_expire_time(), aux]
        with self._addLock:
            self._addList.append(params)
            # Set a timer for commit - if multiple ones have been added, they will be added together
            if self._addTimer is None:
                self._addTimer = threading.Timer(0.5, self.commit_jobs)
                self._addTimer.start()

        try:
            if 0:
                if aux and aux != "none":
                    SQL = "INSERT INTO status(timestamp, paramid, chanid, value, expires, aux) "\
                        "VALUES (%s, %s, %s, %s, %s, %s)"
                    params.append(aux)
                else:
                    SQL = "INSERT INTO status(timestamp, paramid, chanid, value, expires) "\
                        "VALUES (%s, %s, %s, %s, %s)"
                self._execute(SQL, params)
        except Exception:
            self.log.exception("Updating status information %s.%s" % (event.status_holder.get_name(), event.get_name()))

    def commit_jobs(self):
        """
        TODO: Could do this more efficient if we held the lock for shorter, but it doesn't seem like a big deal for now
        """
        with self._addLock:
            self._addTimer = None  # TODO: Should likely have a lock protecting this one
            if len(self._addList) == 0:
                # print("*** WARNING: commit_jobs called but no queued jobs")
                return

            SQL = "INSERT INTO status(timestamp, paramid, chanid, value, expires, aux) VALUES "
            args = []
            for entry in self._addList:
                SQL += "(%s, %s, %s, %s, %s, %s),"
                args.extend(entry)

                if len(args) > 1000:
                    self._execute(SQL[:-1], args)
                    SQL = "INSERT INTO status(timestamp, paramid, chanid, value, expires, aux) VALUES "
                    args = []
            if len(args) > 0:
                self._execute(SQL[:-1], args)
            self._addList = []

if __name__ == "__main__":
    import sys
    try:
        r = MySQLStatusReporter()
        if len(sys.argv) > 1:
            if sys.argv[1] == "upgrade":
                print("Upgrading tables")
                sql = "ALTER TABLE status ADD expires DOUBLE DEFAULT NULL"
                r._execute(sql)

    finally:
        API.shutdown()
