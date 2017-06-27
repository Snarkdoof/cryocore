#!/usr/bin/env python
from CryoCore.Core.dbHandler import DbHandler

import logging
import threading

"""
This module provides the logging service for the whole application.
It has been designed to be suited of a multi-threading environment.
It allows to save the logs generated along the application into a database,
which is managed by L{DbHandler<CryoCore.Core.dbHandler.DbHandler>}.
@todo: The logging service, so far, only keeps the logs into a local database
supported by a file whose name and path are got by parameters
but it should be picked up from the configuration service.
Shortly, the name of the database file and
its path shall be picked up from the configuration service.
See: L{ConfigManagerClient<Config.configManager.ConfigManagerClient>} or
L[BlockConfigManagerClient<Config.blockConfigManager.BlockConfigManagerClient>}.
"""

_loggers = {}
_loggers_lock = threading.Lock()
_dbhandler = DbHandler()


def resetLoggingService():
    """
    Remove all state in regards to existing loggers.  This should only be used
    if none of the existing loggers will be used later.  It is implemented due to
    the rather peculiar copying of state that happens in the multiprocess module,
    and is to be executed after a "fork" has been done.
    """

    my_loggers_lock = globals()['_loggers_lock']
    my_loggers = globals()['_loggers']

    my_loggers = {}
    my_loggers_lock = threading.Lock()


def getLoggingService(name,
                    loggerLevel=logging.DEBUG,
                    log_database_name=None,
                    handlerLevel=logging.NOTSET,
                    db_connector=None):
    """
    Retrieve a reference to a logger instance identified by its name, which is a period-separated hierarchical structure. This function will create the instance if it is necessary. Multiple calls to this function with the same name will return a reference to the same logger object. Besides it sets the right levels, for both the C{Logger} itself and the C{Handler}.
    @param name: name of the logger. The name lets the loggers be classified hierarchically. Loggers that are further down in the hierarchical list are children of loggers higher up in the list.
    @type name: C{string}
    @param loggerLevel: this is the level which is used by the logger. Only logs with more or equal level will be saved into the database. Therefore it is the first filter. Logs with less level will be dropped off quietly.
    @type loggerLevel: either C{logging.DEBUG} or C{logging.INFO} or C{logging.WARNING} or C{logging.ERROR} or C{logging.CRITICAL}
    @param log_database_name: name and the path which describe the database file.
    @type log_database_name: C{string}
    @param handlerLevel: this is the level which is used by the handler. Only logs with more or equal level than the logger, especified by I{loggerLevel},  and more and equal level than the handler, will be saved into the database. Therefore it is the second filter. Logs with less level than the logger's level and handler's level will be dropped quietly. This functionality will be helpful if there are more than one logger.
    @type handlerLevel: either C{logging.DEBUG} or C{logging.INFO} or C{logging.WARNING} or C{logging.ERROR} or C{logging.CRITICAL}
    @param db_connector: method from L{DbHandler<CryoCore.Core.dbHandler.DbHandler>} which will manage the connection to the selected database.
        - L{getLiteConnection<CryoCore.Core.dbHandler.DbHandler.getLiteConnection>}, connection to the local database which is supported by SQLite3.
        - L{getPostgreConnection<CryoCore.Core.dbHandler.DbHandler.getPostgreConnection>}, connection to the Postgre database.
    @type db_connector: L{getLiteConnection<CryoCore.Core.dbHandler.DBHandler.getLiteConnection>} or L{getPostgreConnection<CryoCore.Core.dbHandler.DBHandler.getPostgreConnection>}
    @see: This function is based on U{logging.getLogger()<http://docs.python.org/library/logging.html?highlight=logging.getlogger#logging.getLogger>}
    @see: U{Detailed description of the level system<http://docs.python.org/library/logging.html?highlight=logging#module-logging>}
    @postcondition: if the logger has been already instantiated, the reference to this object will be retrieved. But if it is the first call with I{name}, a new logger is internally registered, set accordingly and returned.
    """
    myloggers_lock = globals()['_loggers_lock']
    myloggers = globals()['_loggers']
    dbHandler = globals()['_dbhandler']

    with myloggers_lock:
        if name not in list(myloggers.keys()):
            myloggers[name] = logging.getLogger(name)
            myloggers[name].setLevel(loggerLevel)
            myloggers[name].addHandler(dbHandler)

    return myloggers[name]
