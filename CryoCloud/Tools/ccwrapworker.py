#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
import sys
from CryoCloud.Tools.processwrapper import Wrapper

"""ccwrapworker wraps ccworker and forwards arguments"""
w = Wrapper(["ccworker"] + sys.argv[1:])
w.run()
