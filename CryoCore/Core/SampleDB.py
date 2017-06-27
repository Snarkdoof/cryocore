
from threading import Thread
from CryoCore.Core.InternalDB import mysql as DB
from CryoCore.Core import API


class SampleDB(DB, Thread):
    def __init__(self, inqueue, stop_event):
        Thread.__init__(self)
        DB.__init__(self, "SampleDB")
        self._inqueue = inqueue
        self._stop_event = stop_event
        self.cfg = API.get_config("Instruments.SampleDB")
        self.log = API.get_log("Instruments.SampleDB")

        if self.cfg["imu"] is None or self.cfg["imu"] == "auto":
            self._imu = self._detect_imu()
        else:
            self._imu = self.cfg["imu"]

        self.log.info("Using IMU: %s" % self._imu)

        statements = ["""CREATE TABLE IF NOT EXISTS sample (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  timestamp DOUBLE,
  instrument SMALLINT,
  path VARCHAR(512),
  q0 FLOAT DEFAULT NULL,
  q1 FLOAT DEFAULT NULL,
  q2 FLOAT DEFAULT NULL,
  q3 FLOAT DEFAULT NULL,
  lat FLOAT,
  lon FLOAT,
  alt FLOAT,
  roll FLOAT DEFAULT NULL,
  pitch FLOAT DEFAULT NULL,
  yaw FLOAT DEFAULT NULL)""",
                      """CREATE INDEX sample_ts ON sample(timestamp)"""]
        self._init_sqls(statements)

    def _detect_imu(self):
        """
        Guess which IMU we use by checking if anyone has any samples
        """

        try:
            ardu_imu = 0
            imu = 0
            c = self._execute("SELECT MAX(timestamp) FROM imu")
            r = c.fetchone()
            if r:
                imu = r[0]
        except Exception as e:
            self.log.error("No imu it seems: %s" % e)
            pass

        try:
            c = self._execute("SELECT MAX(timestamp) FROM arduimu")
            r = c.fetchone()
            if r:
                ardu_imu = r[0]

            if ardu_imu > imu:
                return "arduimu"
        except Exception as e:
            self.log.error("No arduimu it seems: %s" % e)
            pass

        return "imu"

    def _add_to_db(self, item):
        look_up = False
        # If item already has lat,lon,alt, use what info we have
        for i in ["lat", "lon", "alt"]:
            if i not in list(item.keys()):
                look_up = True
                break

        values = [item["timestamp"],
                  item["instrumentid"],
                  item["path"]]

        if look_up:
            success = False
            if self._imu == "arduimu":
                try:
                    # item should have "path", "timestamp" and "instrumentid" the rest we will find here
                    SQL = "INSERT INTO sample (timestamp, instrument, path, roll, pitch, yaw, lat, lon, alt) " \
                        "SELECT %s, %s, %s, roll, pitch, yaw, lat, lon, alt " \
                        "FROM arduimu WHERE timestamp<=%s AND lat<>0 ORDER BY timestamp DESC LIMIT 1"
                    values.append(item["timestamp"])
                    c = self._execute(SQL, values)
                    if c.rowcount > 0:
                        success = True
                except:
                    # Should we log this?  Could be quite often
                    self.log.exception("Sample with GPS failed, skipping GPS")
                    pass
                    success = True
            else:
                try:
                    # item should have "path", "timestamp" and "instrumentid" the rest we will find here
                    SQL = "INSERT INTO sample (timestamp, instrument, path, q0, q1, q2, q3, lat, lon, alt) " \
                        "SELECT %s, %s, %s, quaternion_q0, quaternion_q1, quaternion_q2, quaternion_q3, Lat, Lon, Alt " \
                        "FROM imu WHERE timestamp<=%s ORDER BY timestamp DESC LIMIT 1"
                    values.append(item["timestamp"])
                    c = self._execute(SQL, values)
                    if c.rowcount > 0:
                        success = True
                except:
                    # Should we log this?  Could be quite often
                    self.log.exception("Sample with GPS failed, skipping GPS")

            if not success:
                # Add failed - most likely we have no GPS info.  Insert it blank
                SQL = "INSERT INTO sample (timestamp, instrument, path) VALUES (%s, %s, %s)"
                self._execute(SQL, values[:3])

        else:
            # TODO: Support roll,pich,yaw on full samples too?
            entry = "partial"
            p = True
            for i in ["q0", "q1", "q2", "q3"]:
                if i not in item:
                    p = False
                    break
            if p:
                entry = "q"

            if entry == "partial":
                p = True
                for i in ["roll", "pitch", "yaw"]:
                    if i not in item:
                        p = False
                if p:
                    entry = "rpy"

            values += [item["lat"], item["lon"], item["alt"]]
            if entry == "q":
                values += [item["q0"], item["q1"], item["q2"], item["q3"]]
                SQL = "INSERT INTO sample (timestamp, instrument, path, lat, lon, alt, q0, q1, q2, q3) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            elif entry == "rpy":
                values += [item["roll"], item["pitch"], item["yaw"]]
                SQL = "INSERT INTO sample (timestamp, instrument, path, lat, lon, alt, roll, pitch, yaw) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            else:
                # Partial entry
                SQL = "INSERT INTO sample (timestamp, instrument, path, lat, lon, alt) VALUES (%s, %s, %s, %s, %s, %s)"
            self._execute(SQL, values)

    def fill_in(self):
        """
        Execute on existing sample table, trying to fill in all values from the IMU
        """
        if self._imu == "arduimu":
            print("Using ArduIMU")
            c = self._execute("SELECT min(timestamp) FROM arduimu WHERE lat <> 0")
            min_ts = c.fetchone()[0]

        print("Minimum timestamp is: ", min_ts)

        cursor = self._execute("SELECT id, timestamp, instrument, path FROM sample where lat=0 and timestamp>%s", [min_ts])
        if self._imu == "arduimu":
            SQL = "REPLACE INTO sample (id, timestamp, instrument, path, roll, pitch, yaw, lat, lon, alt) " \
                "SELECT %s, %s, %s, %s, roll, pitch, yaw, lat, lon, alt " \
                "FROM arduimu WHERE timestamp<=%s AND lat<>0 ORDER BY timestamp DESC LIMIT 1"
        else:
            raise Exception("Don't support other imus atm")

        for _id, _ts, _i, _p in cursor.fetchall():
            print(_id)
            c = self._execute(SQL, [_id, _ts, _i, _p, _ts])

    def run(self):
        print("Running")
        while not self._stop_event.is_set():
            try:
                item = self._inqueue.get(True, 1.0)
                self._add_to_db(item)
            except Exception as e:
                # Queue.Empty is raised if no item was available
                pass

if __name__ == "__main__":
    from . import Queue
    import threading as threading

    q = Queue.Queue()
    e = threading.Event()
    db = SampleDB(q, e)

    if 1:
        db.fill_in()
        raise SystemExit(0)

    db.start()

    # Traverse the given directory
    import sys
    import os
    import os.path
    import time

    path = sys.argv[1]
    for elem in os.listdir(path):
        if elem.count("_") != 2:
            continue

        f, ext = os.path.splitext(elem)
        instrument, stime, etime = f.split("_")
        if not instrument:
            instrument = 0
        # Estimate time offset
        sample = {"timestamp": stime,
                  "path": path + elem,
                  "instrumentid": instrument}
        q.put(sample)

    while not q.empty():
        time.sleep(1.0)
    e.set()
