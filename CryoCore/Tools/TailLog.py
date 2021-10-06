#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

from __future__ import print_function

import time
import sys
import re
import json
import os

from argparse import ArgumentParser

from CryoCore import API
from CryoCore.Core.InternalDB import mysql

try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

ID = 0
TEXT = 1
LEVEL = 2
TIMESTAMP = 3
LINE = 5
FUNCTION = 6
MODULE = 7
LOGGER = 8


def to_seconds(timestring):
    # Convert to seconds since epoch
    try:
        return time.strptime(timestring)
    except:
        raise SystemExit("Bad time '%s', example is %s" % (timestring, time.ctime()))


def _should_delete(options, text):
    if options.yes:
        return True

    print(text, "(Answer Yes or No)?")
    answer = None
    while True:
        answer = input().lower()
        if answer not in ["yes", "no"]:
            print("Please answer 'yes' or 'no'")
            continue
        break
    if answer == "no":
        if options.verbose:
            print("Not clearing", item)
        return False
    return True


class TailLog(mysql):
    """
    Tail a status update database and dump the results as text.
    Allow some basic filtering too
    """

    def __init__(self, name="TailLog", default_show=True, clock=None):
        """
        default_show: should I print new messages by default
        """
        self.name = name
        self.clock = clock

        cfg = API.get_config("System.LogDB")
        mysql.__init__(self, self.name, config=cfg, can_log=False, is_direct=True)

        self.filters = []
        self.default_show = default_show

    def add_filter(self, filter):
        """
        Add a callback that will be showed or hidden - will be called
        for each new row.
        Must return True if the row should be printed,
        False if it should be hidden
        """
        self.filters.append(filter)

    def clear_all(self, options):
        """
        Delete all log messages.  USE WITH EXTREME CARE!
        """
        if options.verbose:
            print("Deleting log messages")
        self._execute("DELETE FROM log")

        if options.verbose:
            print("Clear DONE")

    def grep(self, options, args):
        """
        Perform a search and display the results (going back 200 messages)
        """
        if options.all:
            last_id = 0
        else:
            cursor = self._execute("SELECT MAX(id) FROM log")
            row = cursor.fetchone()
            if row[0]:
                last_id = max(row[0] - int(options.lines), 0)
            else:
                print("No log entries, tailing from now")
                last_id = 0

        r = ""
        for arg in args:
            r += ".*" + arg + ".*|"
        r = r[:-1]
        reg = re.compile(r)
        # Search in text, module, logger
        while not API.api_stop_event.isSet():
            SQL = "SELECT * FROM log WHERE id>%s ORDER BY id LIMIT 10000"
            cursor = self._execute(SQL, [last_id])
            last_lines = []
            should_print = 0
            for row in cursor.fetchall():
                last_id = row[ID]
                line = self._print_row(options, row, True)
                if reg.match(line):
                    for l in last_lines:
                        print(l)
                        last_lines = []
                    # Highlight if not BW
                    if options.bw:
                        print("--->", line)
                    else:
                        m = re.match("^\\033\[(.[2-3])m", line)
                        line = line.replace(m.groups()[0], "1;91", 1)
                        line = re.sub("\033\[", "\033[7;", line)
                        line += "\033[27m"
                        # print("\033[1m" + line)
                        print(line)

                    should_print = 4
                elif should_print > 0:
                    print(line)
                    should_print -= 1
                    if should_print == 0:
                        print("\n")
                else:
                    last_lines.append(line)
                    if len(last_lines) > 4:
                        last_lines.pop(0)

            if not options.follow:
                break

            time.sleep(1)

        # TODO: implement follow (-f)

    def get_list(self, option):
        if option == "modules":
            SQL = "SELECT DISTINCT(module) FROM log"
        elif option == "loggers":
            SQL = "SELECT DISTINCT(logger) FROM log"
        else:
            raise Exception("Don't support listing '%s'" % options)

        return list(self._execute(SQL).fetchall())

    def print_status(self, options, args):
        """
        Loop and print status. Does never return - kill it.

        This function doesn't list all entries, but goes back a few hundred entries
        and tails from there
        """
        target_file = None
        if options.tofile:
            if not os.path.isdir(os.path.dirname(options.tofile)):
                os.makedirs(os.path.dirname(options.tofile))
            target_file = open(options.tofile, "a+")
            options.since = time.ctime()
            options.follow = True

        search = ""
        if options.module:
            search += "UPPER(module)='%s' " % options.module.upper()
            if options.logger:
                search += "AND"
        if options.logger:
            search += "UPPER(logger)='%s' " % options.logger.upper()

        if options.since and not options.all:
            cursor = self._execute("SELECT MIN(id) FROM log WHERE time>=%s", [time.mktime(to_seconds(options.since))])
            row = cursor.fetchone()
            if row:
                last_id = row[0]
            if last_id is None:
                cursor = self._execute("SELECT MAX(id) FROM log")
                last_id = cursor.fetchone()[0]
        elif options.all:
            last_id = 0
        else:
            if self.clock:
                cursor = self._execute("SELECT MAX(id) FROM log WHERE time<%s", [self.clock.pos()])
            else:
                cursor = self._execute("SELECT MAX(id) FROM log")
            row = cursor.fetchone()
            if row:
                if not row[0]:
                    last_id = 0  # is None if no entries.
                else:
                    last_id = max(row[0] - int(options.lines), 0)
            else:
                print("No log entries, tailing from now")
                last_id = 0

        last_pos = 0
        start_id = last_id
        while True:
            try:
                t = ""
                if self.clock:
                    if self.clock.pos() < last_pos:
                        last_id = start_id
                    last_pos = self.clock.pos()
                    t = " AND time<%f " % last_pos
                rows = 0
                if options.verbose:
                    print("Searching")
                if len(args) > 0:
                    SQL = args[0] + " AND id>%s"
                    params = [last_id]
                elif search:
                    SQL = "SELECT * FROM log WHERE id>%s AND level>=%s AND " + t + search + " ORDER BY id"
                    params = [last_id, API.log_level_str[options.level]]
                else:
                    SQL = "SELECT * FROM log WHERE id>%s AND level>=%s" + t + " ORDER BY id"
                    params = [last_id, API.log_level_str[options.level]]

                SQL += " LIMIT 10000"
                cursor = self._execute(SQL, params)
                for row in cursor.fetchall():
                    rows += 1
                    if row[ID] > last_id:
                        last_id = row[ID]

                    do_print = self.default_show

                    # Any matches to hide it?
                    for filter in self.filters:
                        try:
                            if list(filter(row)):
                                do_print = True
                            else:
                                do_print = False
                            if do_print != self.default_show:
                                break  # Found a match, stop now

                        except Exception as e:
                            print("Exception executing filter " + str(filter) + ":", e)

                    if do_print:
                        if target_file:
                            self._write_to_file(target_file, options, row)
                        else:
                            self._print_row(options, row)

                if rows == 0:
                    if not options.follow:
                        break
                    if options.realtime:
                        # If we're doing realtime logs, start monitoring the event bus from now on.
                        # We might get a few duplicates, but not too many.
                        self._follow_realtime(options)
                    # No new activity, wait a bit before we try again
                    time.sleep(0.1)

            except Exception as e:
                print("Oops:", e)
                import traceback
                traceback.print_exc()

    def _write_to_file(self, target, options, row):
        # Is the file too big? Remove 25% from the start by reading the last 75%, truncate and write
        if options.maxfilesize:
            max_size = int(options.maxfilesize) * 1048576
            if os.path.getsize(options.tofile) > max_size:
                target.seek(max_size * 0.75)
                data = target.read()
                target.seek(0)
                target.truncate()
                target.write(data)

        if not row[TEXT].startswith("<pbl"):
            text = row[TEXT]
            pbl = None
        else:
            pbl, text = row[TEXT][4:].split("> ", 1)

        item = {
            "ts": row[TIMESTAMP],
            "module": row[MODULE],
            "line": row[LINE],
            "logger": row[LOGGER],
            "level": API.log_level[row[LEVEL]],
            "text": text,
        }

        if pbl:
            item["pebble"] = pbl

        target.write(json.dumps(item) + "\n")
        target.flush()

    def _follow_realtime(self, options):
        import traceback
        import json
        import threading
        from CryoCore.Core import CCshm
        if not CCshm.available:
            print("WARNING: Shared memory not available - realtime log reverting to database")
            options.realtime = False
            return

        target_file = None
        if options.tofile:
            if not os.path.isdir(os.path.dirname(options.tofile)):
                os.makedirs(os.path.dirname(options.tofile))
            target_file = open(options.tofile, "a+")
            options.since = time.ctime()
            options.follow = True

            
        # We need a separate daemon thread to get new data from the shared memory system.
        # Without it, we would block forever on Ctrl-C if no new log messages appear.
        def getter():
            log_bus = None
            while True:
                try:
                    log_bus = CCshm.EventBus("CryoCore.API.Log", 0, 0)
                    break
                except:
                    print("Event bus not ready yet..")
                    time.sleep(1)
            while True:
                data = log_bus.get_many()
                if data:
                    for item in data:
                        try:
                            d = json.loads(item.decode("utf-8"))
                            row = [ -1, d["message"], d["level"], d["time"], d["msecs"], d["line"], d["function"], d["module"], d["logger"] ]

                            do_print = self.default_show

                            # Any matches to hide it?
                            for filter in self.filters:
                                try:
                                    if list(filter(row)):
                                        do_print = True
                                    else:
                                        do_print = False
                                    if do_print != self.default_show:
                                        break  # Found a match, stop now

                                except Exception as e:
                                    print("Exception executing filter " + str(filter) + ":", e)

                            if do_print:
                                if target_file:
                                    self._write_to_file(target_file, options, row)
                                else:
                                    self._print_row(options, row)
                        except:
                            print("Failed to parse or print data: %s" % (data))
                            traceback.print_exc()
                    # Sleep to avoid lock thrashing, and buffer up more data before
                    # we do anything
                    time.sleep(0.016)            
        t = threading.Thread(target=getter, daemon=True)
        t.start()
        while True:
            time.sleep(1)
            
    def _print_row(self, options, row, noprint=False):
        # Convert time to readable and ignore the ID
        t = time.ctime(row[TIMESTAMP])

        if 0:
            # BW print
            print(t + " [%7s][%20s][%23s(%4s)][%10s] %s" %
                  (API.log_level[row[LEVEL]],
                   row[MODULE], row[FUNCTION], row[LINE],
                   row[LOGGER],
                   row[TEXT]))
        else:
            # Color print
            def colored(text, color):
                colors = {"green": "\033[92m",
                          "red": "\033[91m",
                          "yellow": "\033[93m",
                          "blue": "\033[94m",
                          "black": "\033[90m",
                          "cyan": "\033[36m",
                          "white": "\033[37m"}
                return colors[color] + text  # + "\033[0m"

            level_color = {API.log_level_str["DEBUG"]: "yellow",
                           API.log_level_str["INFO"]: "green",
                           API.log_level_str["WARNING"]: "red",
                           API.log_level_str["ERROR"]: "red",
                           API.log_level_str["FATAL"]: "red",
                           API.log_level_str["CRITICAL"]: "red"}

            text_color = {API.log_level_str["DEBUG"]: "green",
                          API.log_level_str["INFO"]: "cyan",
                          API.log_level_str["WARNING"]: "yellow",
                          API.log_level_str["ERROR"]: "red",
                          API.log_level_str["FATAL"]: "red",
                          API.log_level_str["CRITICAL"]: "red"}
            if options.bw:
                line = "%s [%7s][%20s (%4s)][%10s] %s" %\
                    (t, API.log_level[row[LEVEL]], row[MODULE], row[LINE], row[LOGGER], row[TEXT])
            else:
                line = colored(t, "yellow") + " [" + colored("%7s" % API.log_level[row[LEVEL]], level_color[row[LEVEL]]) + "][" +\
                    colored("%20s" % row[MODULE], "green") +\
                    "(%4s)][" % row[LINE] +\
                    colored("%10s" % row[LOGGER], "blue") + "]" +\
                    colored(row[TEXT], text_color[row[LEVEL]])

            if noprint:
                return line

            if len(options.keywords) > 0:
                for kwd in options.keywords:
                    if line.find(kwd) > -1:
                        print(line)
                        return
            else:
                print(line)


if __name__ == "__main__":

    tail = None
    # We're single threaded and don't want to make any tables
    API.__is_direct = True
    API.auto_init = False
    colors = False

    def module_completer(prefix, parsed_args, **kwargs):
        items = tail.get_list("modules")
        res = []
        for item in items:
            res.append(item[0])
        return res

    def logger_completer(prefix, parsed_args, **kwargs):
        items = tail.get_list("loggers")
        res = []
        for item in items:
            res.append(item[0])
        return res

    def since_completer(prefix, parsed_args, **kwargs):
        return [time.ctime(time.time() - 3600)]

    def number_completer(prefix, parsed_args, **kwargs):
        return ["200", "1000"]

    def null_completer(prefix, parsed_args, **kwargs):
        return ["look", "for", "something"]

    parser = ArgumentParser()
    parser.add_argument('keywords', type=str, nargs='*', help='Filter updates')

    parser.add_argument("--clear-all", action="store_true",
                        default=False,
                        help="Clear the whole database, then execute the command (if any is given)")

    parser.add_argument("-s", "--since", dest="since",
                        help="Only show logs that happen after this point in time").completer = since_completer

    parser.add_argument("--module", dest="module",
                        help="Only show logs from the given module").completer = module_completer

    parser.add_argument("--logger", dest="logger",
                        help="Only show logs from the given logger").completer = logger_completer

    parser.add_argument("--list", dest="list",
                        help="list something ('modules' or 'loggers')", choices=('modules', 'loggers'))

    parser.add_argument("--level", dest="level", default="DEBUG",
                        help="Limit to log levels (or higher)", choices=API.log_level_str.keys())

    parser.add_argument("-f", "--follow", action="store_true",
                        help="Follow the log - keep monitoring it")

    parser.add_argument("-n", "--lines", dest="lines",
                        help="Show the last n lines",
                        default=200).completer = number_completer

    parser.add_argument("-a", "--all", action="store_true",
                        help="Show all lines",
                        default=False)

    parser.add_argument("--yes", action="store_true",
                        help="Always say Yes (DANGEROUS!)",
                        default=False)

    parser.add_argument("--grep", dest="grep", nargs='+',
                        help="search database (can be slow)").completer = null_completer

    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="Verbose mode - say more about what's going on")

    parser.add_argument("--bw", action="store_true", default=False,
                        help="Black and white output")
    
    parser.add_argument("-r", "--realtime", action="store_true", default=True, help="Dump the realtime log. Does not support historical data")
    
    parser.add_argument("--db_name", type=str, dest="db_name", default="", help="cryocore or from .config")
    parser.add_argument("--db_user", type=str, dest="db_user", default="", help="cc or from .config")
    parser.add_argument("--db_host", type=str, dest="db_host", default="", help="localhost or from .config")
    parser.add_argument("--db_password", type=str, dest="db_password", default="", help="defaultpw or from .config")

    parser.add_argument("--tofile", type=str, dest="tofile", default="", help="Dump logs to a given file name, not terminal")
    parser.add_argument("--maxfilesize", type=str, dest="maxfilesize", default=None, help="Maximum filesize in MB")

    parser.add_argument("--motion", dest="motion", action="store_true", default=False,
                        help="Replay a mission according to motions")

    try:
        if "argcomplete" in sys.modules:
            argcomplete.autocomplete(parser)
        options = parser.parse_args()

        db_cfg = {}
        if options.db_name:
            db_cfg["db_name"] = options.db_name
        if options.db_user:
            db_cfg["db_user"] = options.db_user
        if options.db_host:
            db_cfg["db_host"] = options.db_host
        if options.db_password:
            db_cfg["db_password"] = options.db_password

        if len(db_cfg) > 0:
            API.set_config_db(db_cfg)

        clock = None
        if options.motion:
            try:
                import MCorp
                # app = MCorp.App("6459748540093085075", API.api_stop_event)
                app = MCorp.App("5479276526614340281", API.api_stop_event)
                clock = app.motions["live"]
                print("Motion time is", time.ctime(clock.pos()), clock.pos())
            except Exception as e:
                print("Failed to use motion:", e)

        tail = TailLog(clock=clock)

        if options.clear_all:
            if _should_delete(options, "DELETE all logs"):
                tail.clear_all(options)
                print("Database cleared")
                raise SystemExit()

        if not options.bw:
            colors = True
            print("\033[40;97m")  # Go black

        if options.grep:
            tail.grep(options, options.grep)
            raise SystemExit()

        # Add filter to limit the amount of crud printed by the autopilot
        # def filter_autopilot(row):
        #     if row[CHANNEL] == "AutoPilot" and \
        #       row[NAME].find("bytes_") > -1:
        #         return False
        #    return True

        try:
            if options.list:
                items = tail.get_list(options.list)
                print("  -= " + options.list + " =-")
                for item in items:
                    print(item)
                print(" ----------------")
                raise SystemExit()

            tail.print_status(options, [])
        except KeyboardInterrupt:
            pass
    finally:
        API.shutdown()
        if colors:
            print("\033[0m")
