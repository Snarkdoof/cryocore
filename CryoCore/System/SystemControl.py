#!/usr/bin/env python

import os
import os.path
import time
import re
import select
import subprocess

import sys
import signal
import fcntl

import threading

from CryoCore import API


class SystemControl(threading.Thread):
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
        self.name = "System.SystemControl"
        self.parent_pid = os.getppid()

        self.cfg = API.get_config(self.name)
        self.cfg.set_default("default_start_delay", 1.0)
        self.cfg.require(["sample_rate", "monitor_sensors"])
        self.cfg.set_default("cc_expire_time", 7 * 24 * 86400)
        # We set the expire time for status to 7 days if nothing else is set
        API.cc_default_expire_time = int(self.cfg["cc_expire_time"])

        self.log = API.get_log(self.name)
        self.status = API.get_status(self.name)
        self.status["state"] = "starting"
        self._monitor_processes = {}
        self._process = {}

        self._configured_services = []
        for name in self.cfg.get_leaves():
            m = re.match("process\.(\S*)\.command", name)
            if m:
                self._configured_services.append(m.groups()[0])

        for name in self._configured_services:
            print("process.%s.status = stopped" % name)
            self.status["process.%s.status" % name] = "stopped"

    def __del__(self):
        try:
            self.status["state"] = "stopped"
        except:
            pass

    def _add_process_monitor(self, pid, name):
        if name in self._monitor_processes:
            raise Exception("Already have process %s registered" % pid)

        self._monitor_processes[pid] = name

    def _del_process_monitor(self, name):
        del self._monitor_processes[name]

    def run(self):
        self.status["state"] = "running"
        self.log.info("System Information service running")

        # start sensor monitoring
        if self.cfg["monitor_sensors"]:
            self._sensor_thread = threading.Thread(target=self._sensor_info)
            self._sensor_thread.start()

        self._top = subprocess.Popen(["top", "-b", "-d", str(self.cfg["sample_rate"])], stdout=subprocess.PIPE, preexec_fn=os.setsid)
        fcntl.fcntl(self._top.stdout, fcntl.F_SETFL, os.O_NONBLOCK)

        self._gather_info()  # Doesn't return until stopped

        self.log.info("System Information service stopped")

    def stop(self):
        self.stop_event.set()

        self.log.info("Stopping - stopping all processes too")
        for name in list(self._process.keys()):
            self._stop_process(name)

        time.sleep(5)
        for name in list(self._process.keys()):
            self._kill_process(name)

    def _check_processes(self):
        """
        Loop over all processes that I find, start them if they
        should be started, stop if they should be stopped
        """
        def by_priority(a, b):
            """
            Sort by priority
            """
            p_a = self.cfg["process.%s.priority" % a]
            p_b = self.cfg["process.%s.priority" % b]
            if not p_a:
                p_a = 100
            if not p_b:
                p_b = 100
            return p_a - p_b

        self._configured_services.sort()
        for name in self._configured_services:
            if self.cfg["process.%s.enabled" % name]:
                if name in list(self._process.keys()):
                    if self._process[name].poll() is None:
                        continue
                    self.log.debug("Know process %s, but poll returns a value" % name)

                self.log.warning("Process %s should run, but it isn't" % name)
                try:
                    self._start_process(name)
                except:
                    self.log.exception("Could not start process %s" % name)
            else:
                # Should stop it
                if self.status["process.%s.status" % name] == "running":
                    if name not in self._process:
                        self.log.error("INTERNAL: Missing info on process %s" % name)
                        continue

                    pid = self._process[name].pid
                    self.log.info("Stopping process %s (pid %s)" % (name, pid))
                    self._stop_process(name)

                    time.sleep(5)

                    if self.status["process.%s.status" % name] == "running":
                        self.log.warning("Process %s refusing to stop nicely, killing it" % name)
                        self._kill_process(name)

        # Now check the processes that are running
        for name in list(self._process.keys()):
            if self._process[name].poll() is not None:
                self.log.warning("Process %s (%s) stopped with return value %s" %
                                 (name, self._process[name].pid, self._process[name].poll()))
                self.status["process.%s.status" % name] = "stopped"
                del self._process[name]
            else:
                self.status["process.%s.status" % name] = "running"
        # Finally, check that parent process is still running; otherwise it's time to exit.
        self._check_parent()

    def _start_process(self, name):
        """
        Start a process
        """
        if name in list(self._process.keys()):
            raise Exception("Refusing to start '%s', already running with pid %s" % (name, self._process[name].pid))

        if not self.cfg["process.%s.command" % name]:
            self.log.error("Missing command for process '%s'" % name)
            return

        user = self.cfg["process.%s.user" % name]
        if not user:
            user = self.cfg["default_user"]

        env = []
        if self.cfg["process.%s.env" % name]:
            env = self.cfg["process.%s.env" % name].split(" ")
        elif self.cfg["default_environment"]:
            env = self.cfg["default_environment"].split(" ")

        command = ["sudo", "-u", user, "-H", "-s", "env"] + env + self.cfg["process.%s.command" % name].split(" ")

        self.log.debug(str(command))
        cwd = self.cfg["process.%s.dir" % name]
        self._process[name] = subprocess.Popen(command, cwd=cwd, preexec_fn=os.setsid)
        self.status["process.%s.pid" % name] = self._process[name].pid
        self._add_process_monitor(self._process[name].pid, name)

        self.log.info("Started process %s (%s)" % (name, self._process[name].pid))
        self.status["process.%s.status" % name] = "running"

        if self.cfg["process.%s.delay" % name]:
            time.sleep(self.cfg["process.%s.delay" % name])
        else:
            time.sleep(self.cfg["default_start_delay"])

    def _stop_process(self, name):
        """
        Terminate a process
        """
        self.log.info("Stopping %s" % name)

        if self.status["process.%s.status" % name] == "stopped":
            return
        try:
            if self._process[name].poll() is None:
                if self._process[name].pid:
                    os.killpg(os.getpgid(self._process[name].pid), signal.SIGQUIT)
        except:
            pass

        try:
            if self._process[name].poll():
                self.status["process.%s.status" % name] = "stopped"
                return
        except:
            pass

    def _kill_process(self, name):
        try:
            self.log.info("Terminating process!")
            if self._process[name].pid != 0:
                os.killpg(os.getpgid(self._process[name].pid), signal.SIGKILL)
            else:
                self.log.error("Refusing to kill my own process group")
        except:
            self.log.exception("Exception terminating process!")
        
        if self.status["process.%s.status" % name] == "stopped":
            return

        try:
            if self._process[name].poll() is None:
                self._process[name].terminate()
        except:
            self.log.exception("Terminating %s" % name)

        try:
            if self._process[name].poll():
                self.status["process.%s.status" % name] = "stopped"
                return
        except:
            self.log.exception("Checking process %s failed!" % name)

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

            # First we update the uptime
            self.status["uptime"] = int(float(open("/proc/uptime", "r").read().split()[0]))

            try:
                s = os.statvfs("/")
                self.status["disk_available"] = (s.f_bsize * s.f_bavail)

                reported = []
                p = subprocess.Popen("sensors", stdout=subprocess.PIPE, preexec_fn=os.setsid)
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

                # Read sensor input from file if available
                try:
                    if os.path.exists("/sys/devices/virtual/hwmon/hwmon0/temp1_input"):
                        temp = open("/sys/devices/virtual/hwmon/hwmon0/temp1_input", "r").read().strip()
                        self.status["cpu_temp"].set_value(temp, force_update=True)

                    if os.path.exists("/sys/class/gpio/gpio88/value"):
                        ac_ok = open("/sys/class/gpio/gpio88/value", "r").read().strip() == "1"
                        self.status["ac_ok"].set_value(ac_ok, force_update=True)

                    if os.path.exists("/sys/class/gpio/gpio115/value"):
                        batt_ok = open("/sys/class/gpio/gpio115/value", "r").read().strip() == "1"
                        self.status["batt_ok"].set_value(batt_ok, force_update=True)
                except:
                    self.log.exception("Reading ORDROID status")

                # Sleep until next run
                while time.time() - last_run < int(self.cfg["sample_rate"]):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)
                last_run = time.time()

            except Exception:
                # self.log.exception("Exception reading sensor info")
                pass

    def _gather_info(self):
        # stopped_processes = []
        # Read a block of output and process it
        while not self.stop_event.is_set():
            try:
                (r, w, e) = select.select([self._top.stdout], [], [], 1)
                if len(r) > 0:
                    try:
                        line = self._top.stdout.readline()
                    except Exception:
                        continue
                    if not line:
                        continue
                else:
                    continue

                # Parse
                line = line.strip()
                if sys.version_info.major == 3:
                    line = str(line, "utf-8")
                m = re.match("%Cpu\(s\):\s*(.*) us,\s*(.*) sy,\s*(.*) ni,\s*(.*) id.*", line, re.IGNORECASE)
                if not m:
                    m = re.match("Cpu\(s\):\s*(.*)%us,\s*(.*)%sy,\s*(.*)%ni,\s*(.*)%id.*", line, re.IGNORECASE)
                if m:
                    try:
                        self._check_processes()
                    except:
                        self.log.exception("Checking processes")

                    (self.status["cpu_user"],
                     self.status["cpu_system"],
                     self.status["cpu_nice"],
                     self.status["cpu_idle"]) = [float(x.replace(",", ".")) for x in m.groups()]
                    continue

                m = re.match("Mem:\s*(\d+)k total,\s*(\d+)k used,\s*(\d+)k free,\s*(\d+)k buffers", line, re.IGNORECASE)
                if m:

                    (self.status["mem_total"],
                     self.status["mem_used"],
                     self.status["mem_free"],
                     self.status["mem_buffers"]) = m.groups()
                    continue
                m = re.match("Swap:\s*(\d+)k total,\s*(\d+)k used,\s*(\d+)k free,\s*(\d+)k cached.*", line, re.IGNORECASE)
                if m:

                    (self.status["swap_total"],
                     self.status["swap_used"],
                     self.status["swap_free"],
                     self.status["mem_cached"]) = m.groups()
                    continue

                else:
                    # Find cpu and memory usage for known pids
                    # m = re.match("(\d+).*[SM]\s*(\d+)\s*(\d+\.\d).*", line, re.IGNORECASE)
                    m = re.match("(\d+)\s*\s*\S*\s*\S*\s*\S*\s*\S*\s*(\S*)\s*\S*\s+\S*[SM]\s*(\d+).*", line)
                    if m:
                        pid, mem, cpu = m.groups()
                        if mem[-1] == "g":
                            mem = float(mem[:-1]) * 1024
                        elif mem[-1] == "m":
                            mem = float(mem[:-1])
                        elif mem[-1] == "k":
                            mem = float(mem[:-1]) / 1024.0

                        pid = int(pid)
                        if pid in self._monitor_processes:
                            # store this info
                            self.status["process.%s.cpu" % self._monitor_processes[pid]].set_value(cpu, force_update=True)
                            self.status["process.%s.mem" % self._monitor_processes[pid]].set_value(mem, force_update=True)
            except:
                self.log.exception("In main loop - ignoring")

    def sighandler(self, signum, frame):
        """
        Signal handler for UNIX nodes
        """
        print("[GOT SIGNAL]", signum)
        if signum in [signal.SIGQUIT, signal.SIGTERM, signal.SIGINT]:
            print("Stopping")
            self.stop()

    def _check_parent(self):
        if self.parent_pid:
            try:
                os.kill(self.parent_pid, 0)
            except:
                self.log.info("Parent process exited; stoppying SystemControl")
                print("Stopping due to parent exit")
                self.stop()

def check_and_set_pid_running(path):
    # Check if we have a running system control already
    if (os.path.exists(path)):
        pid = 0
        with open(path, "r") as fd:
            pid = int(fd.read())
        try:
            os.kill(pid, 0)
            print("SystemControl process already running, NOT starting")
            raise SystemExit(1)
        except Exception as e:
            print(e)
            pass  # Process is NOT running
    with open(path, "w") as fd:
        fd.write(str(os.getpid()))


if __name__ == "__main__":
    interval = 5
    """
    Child mode is a work around for the process group stuff implemented earlier. Due
    to the separate process groups, killing the SystemControl process (which happens
    when calling service uav stop, for instance), no longer brings down the child processes
    started by SystemControl.
    
    The work around consists of the first SystemControl process starting a new child
    process in a separate process group, and waiting for that to exit. The child periodically
    checks that the parent is still alive, and if not, exits. Currently, the parent-exists check
    is done by sending a 0-signal to the parent process. Not perfect if it should exit
    and a new process gets the old pid, but in practice this shouldn't be very likely.)
    """
    child_mode = False
    if "--child" in sys.argv:
        child_mode = True
        sys.argv.remove("--child")
    if len(sys.argv) > 1:
        interval = int(sys.argv[1])
    if child_mode:
        check_and_set_pid_running("/var/run/uav-child.pid")
        s = SystemControl(interval=interval)

        signal.signal(signal.SIGINT, s.sighandler)
        try:
            s.run()
        except Exception as e:
            print(e)
            pass

        print("Requesting nice shutdown of processes")
        s.stop()
        print("Shutting down")
        API.shutdown()
    else:
        # Launch new instance with child flag. The new instance will notice that we
        # exit, and exit itself as well.
        check_and_set_pid_running("/var/run/uav.pid")
        print("Starting child: %s" % (" ".join(sys.argv)))
        ret = subprocess.call(["python"]+sys.argv+["--child"], preexec_fn=os.setsid)
        print("Parent exiting; child sys control returned with status %d" % (ret))

