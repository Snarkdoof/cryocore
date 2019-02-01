"""

CryoCore.Core API for all UAV based code


"""
import logging

import threading
import logging.handlers
import sys

from CryoCore.Core.Status import Status
from CryoCore.Core.Config import Config


class MissingConfigException(Exception):
    pass

# Global stop-event for everything instantiated by the API
global api_stop_event
api_stop_event = threading.Event()


log_level_str = {"CRITICAL": logging.CRITICAL,
                 "FATAL": logging.FATAL,
                 "ERROR": logging.ERROR,
                 "WARNING": logging.WARNING,
                 "INFO": logging.INFO,
                 "DEBUG": logging.DEBUG}

log_level = {logging.CRITICAL: "CRITICAL",
             logging.FATAL: "FATAL",
             logging.ERROR: "ERROR",
             logging.WARNING: "WARNING",
             logging.INFO: "INFO",
             logging.DEBUG: "DEBUG"}

global CONFIGS
CONFIGS = {}


log = logging.getLogger("CryoCore")
hdlr = logging.StreamHandler(sys.stdout)
# hdlr = logging.handlers.RotatingFileHandler("cryocore.log",
#                                            maxBytes=1024*1024*256)
formatter = logging.Formatter('<%(levelname)s> %(asctime)s [%(filename)s:%(lineno)d] %(message)s')
hdlr.setFormatter(formatter)
log.addHandler(hdlr)
log.setLevel(logging.DEBUG)


def shutdown():
    """
    Shut the API down properly
    """

    global api_stop_event
    api_stop_event.set()

configs = {}


def get_config(name=None, version="default"):
    """
    Rewritten to return configWrappers, that wrap a
    configManagerClient singleton due to heavy resource usage
    """
    if (name, version) not in configs:
        configs[(name, version)] = Config(name, ".cfg" + version)
    return configs[(name, version)]


# @logTiming
def get_log(name):
    return log


# @logTiming
def get_status(name):
    return Status(name)


def _toUnicode(string):
    """
    Function to change a string (unicode or not) into a unicode string
    Will try utf-8 first, then latin-1.
    TODO: Is there a better way?  There HAS to be!!!
    """
    if sys.version_info.major == 3:
        if string.__class__ == str:
            return string
        try:
            return str(string, "utf-8")
        except:
            pass
        if string.__class__ == bytes:
            return str(string, "latin-1")
        return str(string)
    if string.__class__ == unicode:
        return string
    try:
        return unicode(string, "utf-8")
    except:
        pass
    return unicode(string, "latin-1")
