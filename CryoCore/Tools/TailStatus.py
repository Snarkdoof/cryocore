#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

from __future__ import print_function
import sys
import time
import re

from CryoCore import API
from CryoCore.Core.InternalDB import mysql

from argparse import ArgumentParser

try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

ID = 0
TIMESTAMP = 1
NAME = 2
CHANNEL = 3
VALUE = 4
SIZEX = 5
SIZEY = 6
POSX = 7
POSY = 8


class TailStatus(mysql):
    """

    Tail a status update database and dump the results as text.
    Allow some basic filtering too
    """

    def __init__(self, name, default_show=True):
        """
        default_show: should I print new messages by default
        """
        # import threading
        # threading.Thread.__init__(self)

        self.name = name
        cfg = API.get_config("System.Status.MySQL")
        mysql.__init__(self, self.name, cfg)

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

    def clear_all(self):
        """
        Delete ALL status messages.  USE WITH EXTREME CARE!
        """
        self._execute("TRUNCATE status")
        self._execute("TRUNCATE status_parameter")
        self._execute("TRUNCATE status_channel")
        print("ALL CLEARED")

    def print_status(self, follow, max_lines=100):
        """
        Loop and print status. Does never return - kill it.

        Prints the last 100 updates and tails from then on
        """
        cursor = self._execute("SELECT MAX(id) FROM status")
        row = cursor.fetchone()
        if row[0]:
            last_id = max(row[0] - max_lines, 0)
        else:
            print("No status entries, tailing from now")
            last_id = 0

        cursor = self._execute("SELECT MAX(id) FROM status2d")
        row = cursor.fetchone()
        if row[0]:
            last_id_2d = max(row[0] - max_lines, 0)
        else:
            last_id_2d = 0

        while True:
            try:
                def process_results(c, max_id, is2D=None):
                    r = 0
                    for row in c.fetchall():
                        r += 1
                        if row[ID] > max_id:
                            max_id = row[ID]

                        do_print = self.default_show

                        # Any matches to hide it?
                        for filter in self.filters:
                            try:
                                if filter(row):
                                    do_print = True
                                else:
                                    do_print = False
                                if do_print != self.default_show:
                                    break  # Found a match, stop now
                            except Exception as e:
                                print("Exception executing filter " + str(filter) + ":", e)
                                import traceback
                                traceback.print_exc()
                        if do_print:
                            self._print_row(row, is2D)
                    return (r, max_id)
                rows = 0
                SQL = "SELECT id,timestamp,status_parameter2d.name,status_channel.name,value,sizex,sizey,posx,posy FROM status2d,status_parameter2d,status_channel WHERE status2d.chanid=status_channel.chanid AND status2d.paramid=status_parameter2d.paramid AND id>%s ORDER BY id"
                cursor = self._execute(SQL,
                                       [last_id_2d])
                (r, i) = process_results(cursor, last_id_2d, True)
                rows += r
                last_id_2d = i

                SQL = "SELECT id,timestamp,status_parameter.name,status_channel.name,value FROM status,status_parameter,status_channel WHERE status.chanid=status_channel.chanid AND status.paramid=status_parameter.paramid AND id>%s ORDER BY id"
                cursor = self._execute(SQL,
                                       [last_id])
                (r, i) = process_results(cursor, last_id)
                rows += r
                last_id = i

                if not follow:
                    return

                if rows == 0:
                    # No new activity, wait a bit before we try again
                    time.sleep(1)
            except Exception:
                import traceback
                traceback.print_exc()
            finally:
                pass

    def _print_row(self, row, is2D):

        # Use a bit of color to make it PRETTY! :D
        def colored(text, color):
            colors = {"green": "\033[92m",
                      "red": "\033[91m",
                      "yellow": "\033[93m",
                      "blue": "\033[94m",
                      "gray": "\033[90m",
                      "black": "\33[98m"}
            return colors[color] + text + "\033[0m"

        # Convert time to readable and ignore the ID
        t = colored(time.ctime(row[TIMESTAMP]), "yellow")
        channel = colored(row[CHANNEL], "red")
        name = colored(row[NAME], "blue")
        value = row[VALUE]
        if (is2D):
            size = row[SIZEX], row[SIZEY]
            pos = row[POSX], row[POSY]
            print(t + " [%s] %s = %s" % (channel, name, value), pos, size)
        else:
            print(t + " [%s] %s = %s" % (channel, name, value))

    def print_all(self, channel=None):
        SQL = "SELECT * FROM status"
        if channel:
            SQL += ",status_channel WHERE status_channel.chanid=status.chanid AND status_channel.name='%s'" % channel
        cursor = self._get_db().cursor()
        cursor.execute(SQL)
        for row in cursor.fetchall():
            self._print_row(row)
        cursor.close()

    def print_last(self, channel):
        # TODO: Wrap my head around advanced, nested SQL statements
        # SQL = "SELECT * FROM status WHERE id in (SELECT MAX(id) FROM status AS q WHERE status.name=q.name AND q.channel='%s')"%channel.encode("string_escape")

        SQL = "SELECT id,timestamp,status_parameter.name,status_channel.name,value FROM status,status_parameter,status_channel WHERE status.chanid=status_channel.chanid AND status.paramid=status_parameter.paramid AND id in (SELECT MAX(id) FROM status as q,status_channel WHERE status.paramid=q.paramid AND status.chanid=status_channel.chanid AND status_channel.name='%s')" % channel.encode("string_escape")
        cursor = self._execute(SQL)
        for row in cursor.fetchall():
            print("Got status:", row)

    def get_channels(self):
        """
        Returns the set of channels that are available
        """
        channels = []
        cursor = self._execute("SELECT name FROM status_channel")
        for row in cursor.fetchall():
            channels.append(row[0])

        return channels

    def get_params(self, channel):
        """
        Returns the set of channels that are available
        """
        params = []
        cursor = self._execute("SELECT status_parameter.name FROM status_parameter, status_channel WHERE status_parameter.chanid=status_channel.chanid AND status_channel.name=%s", [channel])
        for row in cursor.fetchall():
            params.append(row[0])

        cursor = self._execute("SELECT status_parameter2d.name FROM status_parameter2d, status_channel WHERE status_parameter2d.chanid=status_channel.chanid AND status_channel.name=%s", [channel])
        for row in cursor.fetchall():
            params.append(row[0])

        return params


def yn(message):
    """
    Present a yes/no question, return True iff yes
    """
    print(message)
    if sys.version_info.major == 3:
        response = input().strip()
    else:
        response = raw_input().strip()
    if response.lower() == "y" or response.lower() == "yes":
        return True
    return False


def usage():
    return """%s [options] [items]
    Where items is a list on the form channel:name. Autocomplete is suggested for this
    """ % (sys.argv[0])

if __name__ == "__main__":
    API.auto_init = False
    API.__is_direct = True

    try:
        tail = None

        def channel_completer(prefix, parsed_args, **kwargs):
            channels = tail.get_channels()
            return channels

        def completer(prefix, parsed_args, **kwargs):
            res = []
            if prefix.find(":") == -1:
                channels = tail.get_channels()
                for channel in channels:
                    res.append(channel + ":")
            else:
                channel, part_param = prefix.split(":")
                params = tail.get_params(channel)
                for param in params:
                    if part_param and param.startswith(part_param):
                        res.append(channel + ":" + param)
                    elif part_param == "":
                        res.append(channel + ":" + param)
            return res
        parser = ArgumentParser()

        parser.add_argument('parameters', type=str, nargs='*', help='Filter upates on channel:name (use autocomplete)').completer = completer

        parser.add_argument("--clear-all", action="store_true",
                            default=False,
                            help="Clear the whole status database")

        parser.add_argument("--list-channels", action="store_true",
                            default=False,
                            help="List all status channels")

        parser.add_argument("-n", "--lines", dest="lines",
                            help="Show the last n lines",
                            default=200)

        parser.add_argument("-f", "--follow", action="store_true",
                            help="Follow the status - keep monitoring it")

        parser.add_argument("--channel", dest="channel",
                            help="Only show status from the given channel").completer = channel_completer

        parser.add_argument("--db_name", type=str, dest="db_name", default="", help="cryocore or from .config")
        parser.add_argument("--db_user", type=str, dest="db_user", default="", help="cc or from .config")
        parser.add_argument("--db_host", type=str, dest="db_host", default="", help="localhost or from .config")
        parser.add_argument("--db_password", type=str, dest="db_password", default="", help="defaultpw or from .config")

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
        options.lines = int(options.lines)

        if len(db_cfg) > 0:
            API.set_config_db(db_cfg)

        tail = TailStatus("TailStatus")

        if options.clear_all:
            if yn("*** CLEAR ALL status info?  This cannot be undone"):
                tail.clear_all()
                print("Database cleared")
            raise SystemExit()
        if options.list_channels:
            channels = tail.get_channels()
            print(" -= Channels =-")
            for channel in channels:
                print(channel)
            print(" --------------")
            raise SystemExit()

        if 0:
            # Show historic match
            tail.print_last(sys.argv[1])
            raise SystemExit()

        if options.channel:
            def filter_channel(row):
                if row[CHANNEL] == options.channel:
                    return True
                return False
            tail.add_filter(filter_channel)

        if options.parameters:
            opts = []
            for r in options.parameters:
                if r.startswith(":"):
                    r = ".*" + r
                opts.append(re.compile(r))
            options.parameters = opts

            def filter_params(row):
                l = ":".join([row[CHANNEL], row[NAME]])
                for r in options.parameters:
                    return r.match(l) is not None

            tail.add_filter(filter_params)

        tail.print_status(options.follow, options.lines)

    finally:
        API.shutdown()
        print("API Shut down")
