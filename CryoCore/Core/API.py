# coding=utf-8

"""

CryoCore.Core API for all UAV based code


"""
import socket
import os
import time
import logging
import json

from CryoCore.Core.Status import Status
from CryoCore.Core.Config import Configuration, NamedConfiguration

# from CryoCore.Core.Utils import logTiming
import threading
import multiprocessing


class MissingConfigException(Exception):
    pass

global cc_default_expire_time
cc_default_expire_time = None  # No default expiry

# coding=utf-8

# Global stop-event for everything instantiated by the API
global api_stop_event
api_stop_event = multiprocessing.Event()
api_stop_event.isSet = api_stop_event.is_set

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

global CCGLOBALS
CCGLOBALS = {
    "CONFIGS": {},
    "main_configs": {},
    "LOGS": {},
    "LOG_DESTINATION": {},
    "glblStatusReporter": None,
    "glblSharedMemoryReporter": None,
    "reporter_collection": None
}

global DEFAULT_LOGLEVEL
DEFAULT_LOGLEVEL = logging.DEBUG

__is_direct = False

api_auto_init = True

global queue_timeout
queue_timeout = 0.5

global shutdown_grace_period
shutdown_grace_period = 2

def get_status_reporter():

    global CCGLOBALS
    if not CCGLOBALS["glblStatusReporter"]:
        from CryoCore.Core.Status.MySQLReporter import MySQLStatusReporter as DBReporter
        CCGLOBALS["glblStatusReporter"] = DBReporter()
    return CCGLOBALS["glblStatusReporter"]

def get_shared_memory_status_reporter():
    global CCGLOBALS
    if not CCGLOBALS["glblSharedMemoryReporter"]:
        from CryoCore.Core.Status.SharedMemoryReporter import SharedMemoryReporter
        CCGLOBALS["glblSharedMemoryReporter"] = SharedMemoryReporter()
    return CCGLOBALS["glblSharedMemoryReporter"] 

class GlobalDBConfig:
    singleton = None

    def __init__(self):
        self.cfg = {"db_name": "cryocore",
                    "db_host": "localhost",
                    "db_user": "cc",
                    "db_password": u"Kjøkkentrappene bestyrer sørlandske databehandlingsrutiner",
                    "db_compress": False,
                    "max_connections": 5,
                    "ssl.enabled": False,
                    "ssl.key": None,
                    "ssl.ca": None,
                    "ssl.cert": None}

        # Check in the user's home dir
        userconfig = os.path.expanduser("~/.cryoconfig")
        if os.path.isfile(userconfig):
            with open(userconfig, "r") as f:
                cfg = json.loads(f.read())
            for param in self.cfg:
                if param in cfg:
                    self.cfg[param] = cfg[param]
            for param in cfg:
                if param.startswith("override_"):
                    self.cfg[param] = cfg[param]

        # Local override
        if os.path.isfile(".config"):
            with open(".config", "r") as f:
                cfg = json.loads(f.read())
            for param in self.cfg:
                if param in cfg:
                    self.cfg[param] = cfg[param]
            for param in cfg:
                if param.startswith("override_"):
                    self.cfg[param] = cfg[param]

    @staticmethod
    def get_singleton():
        if GlobalDBConfig.singleton is None:
            GlobalDBConfig.singleton = GlobalDBConfig()
        return GlobalDBConfig.singleton

    def set_cfg(self, cfg):
        for key in self.cfg:
            if key in cfg:
                self.cfg[key] = cfg[key]

    def get_cfg(self):
        return self.cfg


def set_config_db(cfg):
    """
    Set the default configuration database parameters
    """
    # Sanity check
    try:
        legal_keys = ["db_host", "db_password", "db_name", "db_user"]
        for key in cfg:
            if key not in legal_keys:
                raise Exception("Bad config supplied, must only contain from: %s" % str(legal_keys))
        GlobalDBConfig.get_singleton().set_cfg(cfg)
    except Exception as e:
        raise Exception("Bad config supplied", e)


def get_config_db(what=None):
    import copy
    cfg = copy.copy(GlobalDBConfig.get_singleton().get_cfg())

    # Do we have a particular override?
    if what:
        n = "override_%s" % what
        if n in cfg:
            for key in cfg[n]:
                if key in cfg:
                    cfg[key] = cfg[n][key]
    return cfg




def shutdown():
    """
    Shut the API down properly
    """
    global api_stop_event
    api_stop_event.set()

    try:
        global reporter_collection
        del reporter_collection
    except:
        pass


def reset():
    """
    Reset API state.  This function is not completed, but has been
    made to handle multiprocess "forks" that make copies of state
    it really should not copy.

    Perhaps a better way is to ensure that all external connections are
    dependent on thread-id + process id?
    """
    # shutdown()

    # global api_stop_event
    # api_stop_event = threading.Event()
    global CCGLOBALS
    CCGLOBALS = {
        "CONFIGS": {},
        "main_configs": {},
        "LOGS": {},
        "LOG_DESTINATION": {},
        "glblStatusReporter": None,
        "glblSharedMemoryReporter": None,
        "reporter_collection": None
    }

    try:
        from CryoCore.Core.InternalDB import AsyncDB
        AsyncDB.reset()
    except Exception as e:
        print("[ERROR]: Resetting InternalDB.asyncDB:", e)

    return

# @logTiming
def get_config(name=None, version="default", db_cfg=None):
    """
    Rewritten to return configWrappers, that wrap a
    configManagerClient singleton due to heavy resource usage
    """
    if db_cfg is None:
        db_cfg = GlobalDBConfig.get_singleton().cfg

    if version not in CCGLOBALS["main_configs"]:
        CCGLOBALS["main_configs"][version] = Configuration(stop_event=api_stop_event,
                                                           version=version,
                                                           db_cfg=db_cfg,
                                                           # is_direct=True,
                                                           is_direct=__is_direct,
                                                           auto_init=api_auto_init)

    if not (name, version) in CCGLOBALS["CONFIGS"]:
        CCGLOBALS["CONFIGS"][(name, version)] = NamedConfiguration(name, version, CCGLOBALS["main_configs"][version])
        # CONFIGS[(name, version)] = Configuration(root=name,
        #                                         stop_event=api_stop_event,
        #                                         version=version,
        #                                         db_cfg=db_cfg)
    return CCGLOBALS["CONFIGS"][(name, version)]


def set_log_level(loglevel):
    if loglevel in log_level:
        ll = loglevel
    else:
        if loglevel not in log_level_str:
            raise Exception("Bad loglevel '%s', must be one of %s" % (loglevel, log_level_str.keys()))
        ll = log_level_str[loglevel]

    global DEFAULT_LOGLEVEL
    DEFAULT_LOGLEVEL = ll
    if CCGLOBALS["LOG_DESTINATION"]:
        CCGLOBALS["LOG_DESTINATION"].level = ll


class PrefixedLogger(logging.getLoggerClass()):

    def __init__(self, name, level=0, prefix=None):
        logging.Logger.__init__(self, name, level)
        self.prefix = prefix

    def _log(self, level, msg, args, exc_info=None, extra=None, prefix=None):
        if not prefix and self.prefix:
            prefix = self.prefix

        if prefix:
            if callable(prefix):
                msg = "<%s> " % prefix() + msg
            else:
                msg = "<%s> " % prefix + msg
        return super(PrefixedLogger, self)._log(level, msg, args, exc_info, extra)

logging.setLoggerClass(PrefixedLogger)


# @logTiming
def get_log(name, prefix=None):
    # global LOGS
    # global LOG_DESTINATION
    if not CCGLOBALS["LOG_DESTINATION"]:
        from CryoCore.Core.dbHandler import DbHandler
        CCGLOBALS["LOG_DESTINATION"] = DbHandler()
    if name not in CCGLOBALS["LOGS"]:
        CCGLOBALS["LOGS"][name] = logging.getLogger(name)
        CCGLOBALS["LOGS"][name].propagate = False
        CCGLOBALS["LOGS"][name].prefix = prefix
        CCGLOBALS["LOGS"][name].setLevel(DEFAULT_LOGLEVEL)
        CCGLOBALS["LOGS"][name].addHandler(CCGLOBALS["LOG_DESTINATION"])

    return CCGLOBALS["LOGS"][name]


# @logTiming
def get_status(name):
    holder = Status.get_status_holder(name, api_stop_event)
    try:
        holder.add_reporter(get_status_reporter())
    except Exception as e:
        print("Database reporter could not be added for %s:" % name, e)
    try:
        reporter = get_shared_memory_status_reporter()
        if reporter.can_report():
            holder.add_reporter(reporter)
    except Exception as e:
        print("Failed to add shared memory status reporter for %s:" % name, e)

    return holder


def get_status_listener(clock=None):
    from CryoCore.Core.Status import StatusListener
    return StatusListener.get_status_listener(clock)


def _toUnicode(string):
    """
    Function to change a string (unicode or not) into a unicode string
    Will try utf-8 first, then latin-1.
    TODO: Is there a better way?  There HAS to be!!!
    """
    import sys
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
