#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
from __future__ import print_function

try:
    import imp
except:
    import importlib as imp

import inspect
import sys
import os.path

import CryoCloud.Tools.node as node
from CryoCore import API

if len(sys.argv) < 2:
    raise SystemExit("Need module to testrun")

filename = sys.argv[1]
moduleinfo = inspect.getmoduleinfo(filename)
path = os.path.dirname(os.path.abspath(filename))

sys.path.append(path)

args = {}
for arg in sys.argv[2:]:
    name, value = arg.split("=")
    args[name] = value


print("Running module %s with arguments: '%s'" % (moduleinfo.name, args))
try:
    # Create the worker
    worker = node.Worker(0, API.api_stop_event)

    worker.log = API.get_log("testrun." + moduleinfo.name)
    worker.status = API.get_status("testrun." + moduleinfo.name)

    # Load it
    info = imp.find_module(moduleinfo.name)
    mod = imp.load_module(moduleinfo.name, info[0], info[1], info[2])
    task = {"args": args}
    mod.process_task(worker, task)
finally:
    API.shutdown()
