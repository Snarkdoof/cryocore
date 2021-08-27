"""

CryoCore.Core API for all UAV based code


"""
import logging

import threading
import logging.handlers
import sys
import os
import json

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


        # Check if this is in a docker, if so, localhost should be replaced
        # with the IP of the "host"
        if os.path.exists("/.dockerenv"):
            self.cfg["db_host"] = "172.17.0.1"

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
