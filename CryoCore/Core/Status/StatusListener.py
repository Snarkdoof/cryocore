import time
import json
import traceback
import threading

from CryoCore import API
from CryoCore.Core import CCshm
from CryoCore.Core.Status.StatusDbReader import StatusDbReader


class Clock():
    def __init__(self, starttime=None):
        if not starttime:
            self.offset = 0
        else:
            self.offset = time.time() - starttime

    def pos(self):
        return time.time() - self.offset

    def vel(self):
        return 1


def get_status_listener(clock=None):
    if clock or not CCshm.available:
        if clock:
            print("Using DB for time shifted access")
        else:
            print("Shared memory not supported, using DB")
        return StatusListenerDB(clock)
    return StatusListener()


class StatusListenerDB(threading.Thread):
    def __init__(self, clock=None):
        threading.Thread.__init__(self)
        self._channels = {}
        self._monitors = []  # List of paramids to monitor
        self._last_values = {}
        self.clock = clock
        if not clock:
            self.clock = Clock()
        self._db = StatusDbReader()
        self.start()

    def add_monitors(self, items):
        for channel, name in items:
            self._monitors.append((channel, name))

    def run(self):
        # Periodically fetch values so we don't block on reads
        last_run = -60
        # If the clock jumps, we need to clear stuff and restart it a bit
        last_pos = self.clock.pos()
        while not API.api_stop_event.isSet():
            now = self.clock.pos()
            if abs(now - last_pos) > 2:
                # RESET
                print("Skip detected")
                last_run = -60
            if len(self._monitors) > 0:
                updates = self._db.get_last_status_values(self._monitors, since=last_run, now=now)
                for update in updates:
                    self._last_values[update] = {
                        "channel": update[0],
                        "name": update[1],
                        "ts": updates[update][0],
                        "value": updates[update][1]
                    }
            time.sleep(0.5)
            last_run = now

    def get_last_value(self, chan, param):
        if (chan, param) in self._last_values:
            return self._last_values[(chan, param)]

    def get_last_values(self, items=None):
        """
        If items is none, return all last values (by reference)
        """
        if items:
            ret = {}
            for item in items:
                if item in self._last_values:
                    ret[item] = self._last_values[item]
                else:
                    ret[item] = None
            return ret
        return self._last_values


class StatusListener():
    def __init__(self, monitor_all=False, evt=None):
        self._channels = {}
        self._param_ids = []
        self._last_values = {}
        self._monitor_all = monitor_all
        if evt:
            self.condition_lock = evt
        else:
            self.condition_lock = threading.Condition()

        if not CCshm.available:
            raise Exception("Shared memory not available and no db implementation is done")
            print("WARNING: Shared memory not available - reverting to database")
            return
        self.run()

    def add_monitors(self, items):
        """
        Items should be a list of tuples (channel, name)
        """
        for chan, name in items:
            if chan not in self._channels:
                self._channels[chan] = []
            self._channels[chan].append(name)

    def add_monitors_by_id(self, items):
        """
        Items should be a list of parameter ids
        """
        for paramid in items:
            if paramid not in self._param_ids:
                self._param_ids.append(paramid)

    def run(self):
        # We need a separate daemon thread to get new data from the shared memory system.
        # Without it, we would block forever on Ctrl-C if no new status items appear.
        def getter():
            status_bus = None
            while True:
                try:
                    status_bus = CCshm.EventBus("CryoCore.API.Status", 0, 0)
                    break
                except:
                    print("Status event bus not ready yet..")
                    time.sleep(1)
            while True:
                data = status_bus.get_many()
                if data:
                    for item in data:
                        try:
                            d = json.loads(item.decode("utf-8"))
                            #print("Got something", d)
                            if self._monitor_all or \
                               (d["channel"] in self._channels and
                                d["name"] in self._channels[d["channel"]]):
                                    self._last_values[(d["channel"], d["name"])] = d
                        except:
                            print("Failed to parse or print data: %s" % (data))
                            traceback.print_exc()
                    with self.condition_lock:
                        self.condition_lock.notifyAll()

                    # Sleep to avoid lock thrashing, and buffer up more data before
                    # we do anything (is this necessary?)
                    time.sleep(0.005)

        t = threading.Thread(target=getter)
        t.setDaemon(True)
        t.start()

    def get_last_value(self, chan, param):
        if (chan, param) in self._last_values:
            return self._last_values[(chan, param)]

    def get_last_value_by_id(self, paramid):
        if paramid in self._last_values:
            return self._last_values[paramid]

    def get_last_values(self, items=None):
        """
        If items is none, return all last values (by reference)
        """
        if items:
            ret = {}
            for item in items:
                if item in self._last_values:
                    ret[item] = self._last_values[item]
                else:
                    ret[item] = None
            return ret
        return self._last_values

    def wait(self, timeout=1.0):
        """
        Wait for any updates or at most timeout seconds
        """

        with self.condition_lock:
            self.condition_lock.wait(timeout=timeout)
