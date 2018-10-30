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

global CONFIGS
CONFIGS = {}

global main_configs
main_configs = {}

LOGS = {}
LOG_DESTINATION = None
DEFAULT_LOGLEVEL = logging.DEBUG

global glblStatusReporter
glblStatusReporter = None
reporter_collection = None

__is_direct = False

api_auto_init = True


def get_status_reporter():
    global glblStatusReporter
    if not glblStatusReporter:
        from CryoCore.Core.Status.MySQLReporter import MySQLStatusReporter as DBReporter
        glblStatusReporter = DBReporter()
    return glblStatusReporter


class GlobalDBConfig:
    singleton = None

    def __init__(self):
        self.cfg = {"db_name": "cryocore",
                    "db_host": "localhost",
                    "db_user": "cc",
                    "db_password": u"Kjøkkentrappene bestyrer sørlandske databehandlingsrutiner",
                    "db_compress": False,
                    "max_connections": 5}

        # Check in the user's home dir
        userconfig = os.path.expanduser("~/.cryoconfig")
        if os.path.isfile(userconfig):
            with open(userconfig, "r") as f:
                cfg = json.loads(f.read())
            for param in self.cfg:
                if param in cfg:
                    self.cfg[param] = cfg[param]

        # Local override
        if os.path.isfile(".config"):
            with open(".config", "r") as f:
                cfg = json.loads(f.read())
            for param in self.cfg:
                if param in cfg:
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


def get_config_db():
    import copy
    return copy.copy(GlobalDBConfig.get_singleton().get_cfg())


class ReporterCollection:
    """
    This is just a class to hold remote status holders that stops and cleans
    up all reporters
    """
    singleton = "None"

    def __init__(self):
        global api_stop_event
        self.stop_event = api_stop_event
        self.lock = threading.Lock()

        self.reporters = {}

        try:
            from CryoCore.Core.Status.MySQLReporter import MySQLStatusReporter as DBReporter
            name = "System.Status.MySQL"
        except Exception as e:
            print("Exception importing MySQL destination:", e)
            from CryoCore.Core.Status.Sqlite3Reporter import DBStatusReporter as DBReporter
            name = "System.Status.Sqlite"
        self.db_reporter = DBReporter(name)

    @staticmethod
    def get_singleton():
        if ReporterCollection.singleton == "None":
            ReporterCollection.singleton = ReporterCollection()
        return ReporterCollection.singleton

    def get_db_reporter(self):
        return self.db_reporter

    def get_reporter(self, name):
        """
        @returns (was_created, reporter) where was_created is True iff the
        reporter was just created
        """
        was_created = False
        with self.lock:
            if name not in self.reporters:
                was_created = True
                from CryoCore.Core.Status.RemoteStatusReporter import RemoteStatusReporter
                self.reporters[name] = RemoteStatusReporter(name, self.stop_event)
                # Register this reporter with the UAV service
                import CryoCore.Core.timeout_xmlrpclib as xmlrpclib
                try:
                    cfg = get_config("System.Status.RemoteStatusReporter")
                    service = xmlrpclib.ServerProxy(cfg["url"], timeout=1.0)
                    error = None
                    for i in range(0, 3):
                        try:
                            service.add_holder(name, socket.gethostname(),
                                               self.reporters[name].get_port())
                            error = None
                            break
                        except socket.error as e:
                            print("Error registering status reporter:", e)
                            error = e
                            os.system("python Services/StatusService.py & ")
                            print("*** Started status service")
                            time.sleep(2)
                    if error:
                        raise error
                except:
                    get_log("API").exception("Could not register remote status reporter '%s' with service on port '%s'" % (name, self.reporters[name].get_port()))
            return (was_created, self.reporters[name])

    def __del__(self):
        self.stop_event.set()


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
    shutdown()

    global api_stop_event
    api_stop_event = threading.Event()


# @logTiming
def get_config(name=None, version="default", db_cfg=None):
    """
    Rewritten to return configWrappers, that wrap a
    configManagerClient singleton due to heavy resource usage
    """
    if db_cfg is None:
        db_cfg = GlobalDBConfig.get_singleton().cfg

    global main_configs
    if version not in main_configs:
        main_configs[version] = Configuration(stop_event=api_stop_event,
                                              version=version,
                                              db_cfg=db_cfg,
                                              is_direct=__is_direct,
                                              auto_init=api_auto_init)

    global CONFIGS
    if not (name, version) in CONFIGS:
        CONFIGS[(name, version)] = NamedConfiguration(name, version, main_configs[version])
        # CONFIGS[(name, version)] = Configuration(root=name,
        #                                         stop_event=api_stop_event,
        #                                         version=version,
        #                                         db_cfg=db_cfg)
    return CONFIGS[(name, version)]


# @logTiming
def get_log(name):
    global LOGS
    global LOG_DESTINATION
    if not LOG_DESTINATION:
        from CryoCore.Core.dbHandler import DbHandler
        LOG_DESTINATION = DbHandler()
    if name not in LOGS:
            LOGS[name] = logging.getLogger(name)
            LOGS[name].propagate = False
            LOGS[name].setLevel(DEFAULT_LOGLEVEL)
            LOGS[name].addHandler(LOG_DESTINATION)
    return LOGS[name]


# @logTiming
def get_status(name):
    holder = Status.get_status_holder(name, api_stop_event)
    try:
        holder.add_reporter(get_status_reporter())
    except Exception as e:
        print("Database reporter could not be added for %s:" % name, e)
    return holder

    (was_created, reporter) = ReporterCollection.get_singleton().get_reporter(name)
    if was_created:
        try:
            holder.add_reporter(reporter)
            holder["remote_port"] = reporter.port
        except Exception as e:
            print("Remote reporter could not be added:", e)

        try:
            holder.add_reporter(ReporterCollection.get_singleton().get_db_reporter())
        except Exception as e:
            print("Database reporter could not be added:", e)

    return holder


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
