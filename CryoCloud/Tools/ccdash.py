#!/usr/bin/env python3
from __future__ import print_function
import sys
from argparse import ArgumentParser

try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

import time
import curses
import re
import threading

from CryoCore import API
from CryoCore.Core.Status.StatusDbReader import StatusDbReader
from CryoCore.Core import PrettyPrint
import locale

locale.setlocale(locale.LC_ALL, '')
code = locale.getpreferredencoding()


class Parameter():
    def __init__(self, paramid, name, channel):
        self.paramid = paramid
        self.name = name
        self.channel = channel
        self.value = None


class DashBoard:

    def __init__(self):

        self.screen = curses.initscr()
        curses.start_color()
        curses.noecho()
        curses.cbreak()
        self.screen.keypad(True)

        self.cfg = API.get_config("Dashboard")

        self.cfg.set_default("params.cpu_user.source", "NodeController.*")
        self.cfg.set_default("params.cpu_user.name", "cpu.user")
        self.cfg.set_default("params.cpu_user.title", "User")
        self.cfg.set_default("params.cpu_user.type", "Resource")

        self.cfg.set_default("params.cpu_system.source", "NodeController.*")
        self.cfg.set_default("params.cpu_system.name", "cpu.system")
        self.cfg.set_default("params.cpu_system.title", "System")
        self.cfg.set_default("params.cpu_system.type", "Resource")

        self.cfg.set_default("params.cpu_idle.source", "NodeController.*")
        self.cfg.set_default("params.cpu_idle.name", "cpu.idle")
        self.cfg.set_default("params.cpu_idle.title", "Idle")
        self.cfg.set_default("params.cpu_idle.type", "Resource")

        self.cfg.set_default("params.memory.source", "NodeController.*")
        self.cfg.set_default("params.memory.name", "memory.available")
        self.cfg.set_default("params.memory.title", "Free memory")
        self.cfg.set_default("params.memory.type", "Resource")

        self.cfg.set_default("params.progress.source", "Worker.*")
        self.cfg.set_default("params.progress.name", "progress")
        self.cfg.set_default("params.progress.type", "Worker")

        self.cfg.set_default("params.state.source", "Worker.*")
        self.cfg.set_default("params.state.name", "state")
        self.cfg.set_default("params.state.type", "Worker")

        self.log = API.get_log("Dashboard")
        self.parameters = []

        # Set up some colors
        try:
            curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLUE)
            curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
            curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_CYAN)
            curses.init_pair(4, curses.COLOR_WHITE, curses.COLOR_RED)
            curses.init_pair(5, curses.COLOR_YELLOW, curses.COLOR_BLACK)
            curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_GREEN)
        except:
            self.log.exception("Initializing colors failed")

        self.resource_color = 5

        self.height, self.width = self.screen.getmaxyx()
        self.log.debug("Initialised %sx%s" % (self.height, self.width))

        self.screen.hline(0, 0, "*", self.width, curses.color_pair(4))
        self.centerText(self.screen, 0, " CryoCloud Dashboard ", curses.color_pair(4), 3)
        self.resourceWindow = curses.newwin(3, self.width, 1, 0)
        self.resourceWindow.hline(0, 0, " ", self.width, curses.color_pair(self.resource_color))
        self.resourceWindow.hline(1, 0, " ", self.width, curses.color_pair(self.resource_color))

        # self.screen.hline(self.height - 1, 0, "-", self.width, curses.color_pair(4))
        self.workerWindow = curses.newwin(10, self.width, 4, 0)
        self.worker_color = 3

        self.statusdb = StatusDbReader()

    def centerText(self, screen, line, text, color, pad=0):
        width = screen.getmaxyx()[1]
        start = int((width - (2 * pad + len(text))) / 2)
        if start < 0:
            start = 0
        pad = " " * pad
        text = "%s%s%s" % (pad, text.encode(code), pad)
        if text.__class__ == "bytes":
            text = str(text, "utf-8")
        screen.addstr(line, start, text[0:width], color)

    def __del__(self):
        self.screen.keypad(False)
        # curses.nocbreak()
        # curses.echo()
        # curses.endwin()

    def redraw(self):

        # This is a layout kind of thing - we show the total CPU on top
        # Get values
        try:
            resources = []
            tasks = []
            workers = []

            for parameter in self.parameters:
                # parameter.ts, parameter.value = self.statusdb.get_last_status_value_by_id(parameter.paramid)
                if parameter.value is None:
                    continue
                # self.log.debug("Parameter %s|%s has value %s" % (parameter.channel, parameter.name, parameter.value))
                if parameter.type.lower() == "resource":
                    resources.append(parameter)
                elif parameter.type.lower() == "tasks":
                    tasks.append(parameter)
                elif parameter.type.lower() == "worker":
                    workers.append(parameter)

            # Draw resource view
            cpu = "CPU: "
            cpu_usage = {}
            memory = "Memory: "
            for parameter in resources:
                ttl = parameter.channel.split(".")[-1]
                # self.log.debug("%s %s %s" % (parameter.name, parameter.title, parameter.value))
                if parameter.name.startswith("cpu"):
                    if ttl not in cpu_usage:
                        cpu_usage[ttl] = [0, 0]
                    cpu_usage[ttl][1] += float(parameter.value)
                    if parameter.name != "cpu.idle":
                        cpu_usage[ttl][0] += float(parameter.value)

                elif parameter.name.startswith("memory"):
                    memory += "%s: %s   " % (ttl, PrettyPrint.bytes_to_string(parameter.value))
            for ttl in cpu_usage:
                cpu += "%s: %s/%s%%   " % (ttl, int(cpu_usage[ttl][0]), int(cpu_usage[ttl][1]))
            self.resourceWindow.addstr(0, 0, cpu, curses.color_pair(self.resource_color))
            self.resourceWindow.addstr(1, 0, memory, curses.color_pair(self.resource_color))
            self.resourceWindow.refresh()

            workerinfo = {}
            for worker in workers:
                if worker.channel not in workerinfo:
                    workerinfo[worker.channel] = {"ts": 0}
                workerinfo[worker.channel][worker.name] = worker.value
                workerinfo[worker.channel]["ts"] = max(worker.ts, workerinfo[worker.channel]["ts"])

            idx = 0
            for worker in workerinfo:
                infostr = "%s: %s [%d%%] (%s)" %\
                    (worker, workerinfo[worker]["state"],
                     float(workerinfo[worker]["progress"]),
                     time.ctime(float(workerinfo[worker]["ts"])))
                self.workerWindow.addstr(idx, 4, infostr, curses.color_pair(self.worker_color))
                idx += 1

            self.workerWindow.refresh()
        except:
            self.log.exception("Refresh failed")

        # self.screen.refresh()

    def _get_input(self):
        while not API.api_stop_event.isSet():
            c = self.screen.getch()
            asc = -1
            try:
                asc = chr(c)
            except:
                pass
            if asc == "q":
                API.api_stop_event.set()

    def _refresher(self):
        last_run = 0
        # TODO: Write a get_last_changes [idlisit], since=...
        while not API.api_stop_event.isSet():
            for parameter in self.parameters:
                parameter.ts, parameter.value = self.statusdb.get_last_status_value_by_id(parameter.paramid)
            while not API.api_stop_event.isSet():
                timeleft = last_run + 1 - time.time()
                if timeleft > 0:
                    time.sleep(min(1, timeleft))
                else:
                    last_run = time.time()
                    break

    def run(self):

        t = threading.Thread(target=self._refresher)
        t.start()

        t2 = threading.Thread(target=self._get_input)
        t2.start()

        try:
            # channels = self.statusdb.get_channels()
            parameters = self.statusdb.get_channels_and_parameters()

            # Resolve the status parameters and logs we're interested in
            for param in self.cfg.get("params").children:
                name = param.get_full_path().replace("Dashboard.", "")
                for channel in parameters:
                    if re.match(self.cfg["%s.source" % name], channel):
                        try:
                            pname = self.cfg["%s.name" % name]
                            if pname in parameters[channel]:
                                paramid = parameters[channel][pname]
                            # self.log.debug("Resoving %s.%s" % (channel, self.cfg["%s.name" % name]))

                            # paramid = self.statusdb.get_param_id(channel, self.cfg["%s.name" % name])
                            p = Parameter(paramid, self.cfg["%s.name" % name], channel)
                            p.title = self.cfg["%s.title" % name]
                            p.type = self.cfg["%s.type" % name]
                            self.parameters.append(p)
                        except:
                            self.log.exception("Looking up %s %s" % (channel, self.cfg["%s.name" % name]))

            while not API.api_stop_event.isSet():
                self.redraw()
                time.sleep(5)
        except:
            self.log.exception("Exception in main loop")

if __name__ == "__main__":

    parser = ArgumentParser(description="CryoCloud dashboard")
    parser.add_argument("--db_name", type=str, dest="db_name", default="", help="cryocore or from .config")
    parser.add_argument("--db_user", type=str, dest="db_user", default="", help="cc or from .config")
    parser.add_argument("--db_host", type=str, dest="db_host", default="", help="localhost or from .config")
    parser.add_argument("--db_password", type=str, dest="db_password", default="", help="defaultpw or from .config")

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args()

    try:
        dash = DashBoard()
        dash.run()
    finally:
        print("Shutting down")
        API.shutdown()

        curses.nocbreak()
        curses.echo()
        curses.endwin()