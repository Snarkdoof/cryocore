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
            curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_WHITE)
            curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_CYAN)
            curses.init_pair(4, curses.COLOR_WHITE, curses.COLOR_RED)
            curses.init_pair(5, curses.COLOR_BLACK, curses.COLOR_YELLOW)
            curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_GREEN)
        except:
            self.log.exception("Initializing colors failed")
        self.height, self.width = self.screen.getmaxyx()
        self.log.debug("Initialised %sx%s" % (self.height, self.width))
        self.resourceWindow = curses.newwin(4, self.width, 0, 0)

        self.statusdb = StatusDbReader()

    def __del__(self):
        curses.nocbreak()
        self.screen.keypad(False)
        curses.echo()
        curses.endwin()

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
            cpu = "CPU:"
            memory = "Memory: "
            for parameter in resources:
                ttl = parameter.channel.split(".")[-1]
                # self.log.debug("%s %s" % (parameter.name, parameter.title))
                if parameter.name.startswith("cpu"):
                    cpu += "%s: %s   " % (ttl, parameter.value)
                elif parameter.name.startswith("memory"):
                    memory += "%s: %s   " % (ttl, parameter.value)

            self.resourceWindow.addstr(0, 0, cpu, curses.color_pair(1))
            self.resourceWindow.addstr(1, 0, memory, curses.color_pair(1))
            self.resourceWindow.refresh()
        except:
            self.log.exception("Refresh failed")

        # self.screen.refresh()

    def _get_input(self):
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
        API.shutdown()
