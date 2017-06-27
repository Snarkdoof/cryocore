#!/usr/bin/env python

import os
import time
import re
import select
import subprocess

import sys
import signal
import fcntl

import threading

from CryoCore import API
from CryoCore.Core.Status import Sqlite3Reporter


class SystemInformation(threading.Thread):
    """

    Get information from the system
    """

    def __init__(self, interval=None, stop_event=None):
        """
        Dumps data every 'interval' seconds.
        The interval parameter will override config "sample_rate".
        If not given, "sample_rate" is used, with a default value of 10 seconds.

        Config params:
        "sample_rate" - float - how often to update information (default 10 seconds)
        "monitor_sensors" - boolean - also monitor sensors (default True)
        """

        threading.Thread.__init__(self)

        if not stop_event:
            stop_event = threading.Event()
        self.stop_event = stop_event
        self.name = "System.SystemInformation"

        self.cfg = API.get_config(self.name)
        self.cfg.require(["sample_rate", "monitor_sensors"])
        self.log = API.get_log(self.name)

        if interval:
            self.cfg["sample_rate"] = interval

        self.status = API.get_status("System.SystemInfo")
        self.status["state"] = "running"
        self.interval = int(self.cfg["sample_rate"])
        self.processes = {}

    def __del__(self):
        try:
            self.status["state"] = "stopped"
        except:
            pass

    def add_process_monitor(self, pid, name):
        if name in self.processes:
            raise Exception("Already have process %s registered" % pid)

        self.processes[pid] = name

    def run(self):

        self.log.info("System Information service running")

        # start sensor monitoring
        if self.cfg["monitor_sensors"]:
            self._sensor_thread = threading.Thread(target=self._sensor_info)
            self._sensor_thread.start()

        self._top = subprocess.Popen(["top", "-b", "-d", str(self.cfg["sample_rate"])], stdout=subprocess.PIPE)
        fcntl.fcntl(self._top.stdout, fcntl.F_SETFL, os.O_NONBLOCK)

        self.status["state"] = "idle"

        self._gather_info()  # Doesn't return until stopped

        self.log.info("System Information service stopped")

    def stop(self):
        self.stop_event.set()

    def _sensor_info(self):
        """
        Blocking call to monitor sensor output. It will fill in all info
        to the status object, in the form "adapter.sensor". If multiple
        sensors exists with the same name, it will append a number
        e.g. "adapter1.Temperature(2)"
        """

        last_run = 0
        adapter = None

        while not self.stop_event.is_set():
            try:
                reported = []
                p = subprocess.Popen("sensors", stdout=subprocess.PIPE)
                (input, output) = p.communicate()
                if sys.version_info.major == 3:
                    input = str(input, "utf-8")

                # Parse the output from sensors
                for line in input.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    m = re.match("Adapter:\s*(.*)", line)
                    if m:
                        adapter = m.groups()[0]
                        continue
                    m = re.match("(.*):\s*([\+\-\d\.]*).*", line)
                    if m:
                        (s, value) = m.groups()
                        name = adapter + "." + s.strip()
                        if name in reported:
                            # Multiple sensors with same name, number them
                            i = 2
                            while True:
                                if name + "(%d)" % i not in reported:
                                    break
                                i += 1
                            name = name + "(%d)" % i
                        self.status[name].set_value(value.strip(), force_update=True)
                        reported.append(name)
                        continue

                # Sleep until next run
                while time.time() - last_run < int(self.cfg["sample_rate"]):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)
                last_run = time.time()

            except Exception as e:
                self.log.exception("Exception reading sensor info")

    def _gather_info(self):
        # Read a block of output and process it
        self.log.debug("Gathering information")
        self.status["state"] = "running"
        stopped_processes = []
        while not self.stop_event.is_set():
            if self._top.poll():
                self.log.error("Top seems to have died")
            (r, w, e) = select.select([self._top.stdout], [], [], 1)
            if len(r) > 0:
                try:
                    line = self._top.stdout.readline()
                    if sys.version_info.major == 3:
                        line = str(line, "utf-8")
                except Exception as e:
                    # self.log.exception("Reading info")
                    continue
                if not line:
                    continue
            else:
                continue

            # Parse
            line = line.strip()
            m = re.match("Cpu\(s\):\s*(.*)%us,\s*(.*)%sy,\s*(.*)%ni,\s*(.*)%id.*", line, re.IGNORECASE)
            if not m:
                m = re.match("%Cpu\(s\):\s*(.*)us,\s*(.*)sy,\s*(.*)ni,\s*(.*)id.*", line, re.IGNORECASE)
            if m:
                if len(stopped_processes) > 0:
                    print("Processes that are stopped:", stopped_processes)

                # If we are missing info about some processes, this should be
                # reported
                stopped_processes = list(self.processes.keys())[:]
                (self.status["cpu_user"],
                 self.status["cpu_system"],
                 self.status["cpu_nice"],
                 self.status["cpu_idle"]) = [float(x.replace(",", ".")) for x in m.groups()]
                continue

            m = re.match("Mem:\s*(\d+)k total,\s*(\d+)k used,\s*(\d+)k free,\s*(\d+)k buffers", line, re.IGNORECASE)
            if not m:
                m = re.match("KiB Mem:\s*(\d+) total,\s*(\d+) used,\s*(\d+) free,\s*(\d+) buffers", line, re.IGNORECASE)
            if m:

                (self.status["mem_total"],
                 self.status["mem_used"],
                 self.status["mem_free"],
                 self.status["mem_buffers"]) = m.groups()
                continue
            m = re.match("Swap:\s*(\d+)k total,\s*(\d+)k used,\s*(\d+)k free,\s*(\d+)k cached", line, re.IGNORECASE)
            if not m:
                m = re.match("KiB Swap:\s*(\d+) total,\s*(\d+) used,\s*(\d+) free,\s*(\d+) cached", line, re.IGNORECASE)
            if m:

                (self.status["swap_total"],
                 self.status["swap_used"],
                 self.status["swap_free"],
                 self.status["mem_cached"]) = m.groups()
                continue
            else:
                # Find cpu and memory usage for known pids
                m = re.match("(\d+).*[SM]\s*(\d+)\s*(\d+\.\d).*", line, re.IGNORECASE)
                if m:
                    pid, cpu, mem = m.groups()
                    pid = int(pid)
                    if pid in self.processes:
                        # store this info
                        stopped_processes.remove(pid)
                        print(self.processes[pid], ":", cpu, "%cpu,", mem, "%mem")
        self.status["state"] = "stopped"

    def sighandler(self, signum, frame):
        """
        Signal handler for UNIX nodes
        """
        print("[GOT SIGNAL]", signum)
        if signum in [signal.SIGQUIT, signal.SIGTERM, signal.SIGINT]:
            print("Stopping")
            self.stop()

if __name__ == "__main__":

    interval = 15
    if len(sys.argv) > 1:
        interval = int(sys.argv[1])

    s = SystemInformation(interval=interval)

    import signal
    signal.signal(signal.SIGINT, s.sighandler)
    try:
        s.run()
    except Exception as e:
        print(e)
        pass

    print("Shutting down")
    API.shutdown()
