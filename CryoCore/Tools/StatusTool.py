#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

from __future__ import print_function

import time
from CryoCore.Core.Status.StatusDbReader import StatusDbReader
from CryoCore import API
from argparse import ArgumentParser
import sys
try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

if __name__ == "__main__":

    try:
        db = None

        def channel_completer(prefix, parsed_args, **kwargs):
            channels = db.get_channels()
            return channels

        def completer(prefix, parsed_args, **kwargs):
            res = []
            if prefix.find(":") == -1:
                channels = db.get_channels()
                for channel in channels:
                    res.append(channel + ":")
            else:
                channel, part_param = prefix.split(":")
                params = db.get_parameters(channel)
                for param in params:
                    if part_param and param.startswith(part_param):
                        res.append(channel + ":" + param)
                    elif part_param == "":
                        res.append(channel + ":" + param)
            return res

        parser = ArgumentParser()
        parser.add_argument('parameters', type=str, nargs='*', help='Parameters to list/modify').completer = completer

        parser.add_argument("--min", dest="min",
                            action="store_true", default=False,
                            help="Print minimum timestamp")

        parser.add_argument("--max", dest="max",
                            action="store_true", default=False,
                            help="Print maximum timestamp")

        parser.add_argument("-l", "--list", dest="list",
                            help="List what (channels or parameters)")

        parser.add_argument("-c", "--channel", dest="channel",
                            help="The channel to query").completer = channel_completer

        parser.add_argument("-s", "--set", dest="set",
                            help="Set parameter=value")

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

        if len(db_cfg) > 0:
            API.set_config_db(db_cfg)

        db = StatusDbReader()

        if options.min:
            min_timestamp = db.get_min_timestamp()
            print("Minimum timestamp:", min_timestamp,
                  "(%.2f minutes ago)" % ((time.time() - (min_timestamp)) / 60.0))

        if options.max:
            print("Maximum timestamp:", db.get_max_timestamp())

        if options.list == "channels":
            channels = db.get_channels()
            print("Channels:")
            for channel in channels:
                print("    ", channel)

        elif options.list == "parameters":
            channels = db.get_channels()
            print("Parameters:")
            for channel in channels:
                parameters = db.get_parameters(channel)
                for parameter in parameters:
                    print("   [%20s] %s" % (channel, parameter))
                print()

        if options.set:
            s = API.get_status(options.channel)
            if options.set.count("=") != 1:
                raise SystemExit("Bad format, use parameter=value")
            p, v = options.set.split("=")
            if p.find(":") > -1:
                options.channel, p = p.split(":")
            if not options.channel:
                raise SystemExit("Missing channel")
            if not p:
                raise SystemExit("Missing parameter")
            s[p] = v
            raise SystemExit(0)

        for elem in options.parameters:
            if elem.find(":") > -2:
                options.channel, elem = elem.split(":")
            if not options.channel:
                raise SystemExit("Missing channel")
            if not elem:
                raise SystemExit("Missing parameter")
            (ts, val) = db.get_last_status_value(options.channel, elem)
            print("[%20s] %15s = %15s (%s)" % (options.channel,
                                               elem,
                                               val,
                                               time.ctime(ts)))
    finally:
        API.shutdown()
