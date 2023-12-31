#!/usr/bin/env python
import logging
import logging.handlers
import threading
import os
from CryoCore.Core import API, InternalDB
import sys

if sys.version_info.major == 3:
    import multiprocessing as queue
    from queue import Empty
else:
    import Queue as queue
    from Queue import Empty

from CryoCore.Core import CCshm
import json

# dbg_flag = threading.Event()

class DbHandler(logging.Handler, InternalDB.mysql):
    """
    This class takes care of logging the events which will be generated during the mission by the threads/processes into a database.
    It inherits from C{logging.Handler}, therefore it uses the Python logging support.
    @author: Domingo Diez.
    @organization: Norut
    @date: November 2009
    @version: 1.0

    @ivar con: It keeps the connection with the database manager. This variable is initialized during the creation of the object.
    @type con: C{Connection}

    @ivar cur: It manages the database cursor to query/insert elements into the database. This variable is initialized during the creation of the object.
    @type cur: C{Cursor}

    @ivar level: It keeps the level of the C{Handler} to discard the logs with less level. This variable is initialized during the creation of the object.
    @type level: either C{logging.NOTSET} or C{logging.DEBUG} or C{logging.INFO} or C{logging.WARNING} or C{logging.ERROR} or C{logging.EXCEPTION} or C{logging.CRITICAL}


    """
    MAX_LEN = 50000

    def __init__(self, level=logging.DEBUG, aux_filename="/tmp/dbHandler_exceptions.txt"):
        """
        Sets an alternative log handler for either incidences or being used to save the events when the database connection fails, wrapping C{FileHandler} from the Python logging support.
        This method calls the inherited constructor.
        @param level: the minimun level of the logging events required to be saved into the database. The default value is C{logging.NOTSET}, this is the lowest priority logging event, therefore it will save all sort of logging events by default.
        @type level: either C{logging.NOTSET} or C{logging.DEBUG} or C{logging.INFO} or C{logging.WARNING} or C{logging.ERROR} or C{logging.EXCEPTION} or C{logging.CRITICAL}.
        @param aux_filename: name of the file where the log entry will be saved. Its default value is "./doc/failure_reports/dbHandler_exceptions.txt".
        @type aux_filename: C{string}
        @postcondition: The object variables L{con<DbHandler.con>} and L{cur<DbHandler.cur>} have been initialed properly. And by calling the base class constructor the whole new object.
        @warning: if the database connection failed, a log message would be saved into a file identify by I{aux_filename}. This file is maintained as a rotating file, therefore its name would be extended by numbers, e.g. '.1', '.2' etc.
        """
        # if dbg_flag.is_set():
        #    raise Exception("Already created one")
        # dbg_flag.set()

        self.level = level
        self._formatter = logging.Formatter()
        logging.Handler.__init__(self, self.level)

        self.cfg = API.get_config("System.LogDB")
        InternalDB.mysql.__init__(self, "SystemLog", self.cfg, can_log=False, num_connections=1)

        self.log_bus = None
        if CCshm.available:
            # Everyone must use the same size for the event bus (ie, 64K in this case). It's not clear
            # how we can make this configurable, but it should be sufficient.
            self.log_bus = CCshm.EventBus("CryoCore.API.Log", 0, 1024*64)

        # We must check if the db has the old "function" variable defined, which is reserved
        # and makes all kind of troubles
        try:
            self._execute("ALTER TABLE log change function func VARCHAR(255)", insist_direct=True)
        except:
            pass

        # We get ready to deal with problems in the database connection
        self.aux_logger = logging.getLogger('dbHandler_exception')
        if len(self.aux_logger.handlers) < 1:
            # self.aux_logger.setLevel(logging.WARNING)
            self.aux_logger.addHandler(logging.StreamHandler(sys.stdout))
            self.aux_logger.addHandler(logging.handlers.RotatingFileHandler(aux_filename,
                                       maxBytes=50000,
                                       backupCount=5))

        # Ensure that the file is accessible for other users too
        # - Some instruments run as root, but most dont
        # TODO: Do something more sensible here
        import stat
        try:
            os.chmod(aux_filename, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        except:
            pass

        self.tasks = queue.Queue()
        # We use two internal events to control the handler.
        # The stop_event is set in the handler's close() func,
        # which in turn will wait for complete_event to be set.
        self.stop_event = API.api_stop_event
        self.complete_event = threading.Event()
        # Set daemon flag to prevent need for calling logging.shutdown() in API.shutdown
        try:
            self.daemon = False
        except:
            pass

        self._async_thread = threading.Thread(target=self.run_it, args=(self.tasks,))
        self._async_thread.daemon = True
        self._async_thread.start()

    def run_it(self, taskqueue):

        init_statements = ["CREATE TABLE IF NOT EXISTS log ("
                           "id INT UNSIGNED AUTO_INCREMENT primary key, "
                           "message TEXT, "
                           "level SMALLINT UNSIGNED NOT NULL, "
                           "time DOUBLE, "
                           "msecs FLOAT, "
                           "line INTEGER UNSIGNED NOT NULL, "
                           "func VARCHAR(255), "
                           "module VARCHAR(255) NOT NULL, "
                           "logger VARCHAR(255) NOT NULL)",

                           "CREATE INDEX log_module ON log(module)",
                           "CREATE INDEX log_logger ON log(logger)"]

        if API.api_auto_init:
            self._init_sqls(init_statements)

        # update table if necessary
        try:
            self._execute("SELECT func FROM log LIMIT 1")
        except:
            self._execute("ALTER TABLE log RENAME function TO func")


        # Thread entry point
        while not self.stop_event.is_set():
            self.get_log_entry_and_insert(taskqueue, True, API.queue_timeout)
        # Insert any remaining items until self.tasks is empty
        while not self.stop_event.is_set() and self.get_log_entry_and_insert(taskqueue, False, None):
            pass
        self.complete_event.set()

    def get_log_entry_and_insert(self, taskqueue, should_block, desired_timeout):


        SQL = "INSERT INTO log (logger, level, module, line, func, time, msecs, message) VALUES "
        params = []
        while not API.api_stop_event.is_set():
            try:
                args = taskqueue.get(should_block, desired_timeout)
                SQL += "(%s, %s, %s, %s, %s, %s, %s, %s),"
                params.extend(args)
                if len(params) > 1000:
                    break  # Enough already
            except Empty:
                break
            except EOFError:
                print("--- Parent process stopped")
                # API.api_stop_event.set()
                return False
            except Exception as e:
                #print("Error getting async log messages:", e)
                import time
                time.sleep(0.1)
                return False

        if len(params) == 0:
            # Nothing yet
            return False

        # Should insert something
        try:
            SQL = SQL[:-1]
            self._execute(SQL, params, insist_direct=True)  # , log_errors=False)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print("Async exception on log posting", e)
            return False

        return True

    def close(self):
        """
        Close the C{logging.Handler} object. It closes both the C{sqlite3.Connection} L{con<DbHandler.con>} and the C{sqlite3.Cursor} L{cur<DbHandler.cur>} variables, and calls the base class close func.
        @postcondition: The object variables L{con<DbHandler.con>} and L{cur<DbHandler.cur>} are closed, therefore they are no longer available.
        """
        # self.stop_event.set()
        self.complete_event.wait()
        logging.Handler.close(self)

    def emit(self, record):
        """
        Save the log event into the table log of the database which was opened during the constructor call whether the level of the log is higher than the L{level<DbHandler.level>}. This table log is the simplest table to save the logging to speed up the response of the logging tasks. It saves:
            - Name. The name of the Logger which has generated the event. In case of not having been defined, "<no_name>" is saved. E.g. "uav.laser.common.one".
            - Level name. The name of the event level. In case of not having been defined, "<no_levelname>" is saved. This is one of this string:
                - "NOTSET"
                - "DEBUG"
                - "INFO"
                - "WARNING"
                - "ERROR"
                - "CRITICAL"
            - Line number. The line where the event was generated. In case of not having been defined, 0 is saved.
            - func name. The name of the func which called the logger. If it was called from none func, "<module>" is saved. Furthermore, in case of not having been defined, "<no_funcName>" is saved.
            - Created time. When the log event was created. This field was returned by the C{time.time()} func. Its precision is second. In case of not having been defined, 0.0 is saved.
            - Msecs time. The precise miliseconds when the log event was generated. In case of not having been defined, 0.0 is saved.
            - Message. The messages which was included when the log event was generated. In case of not having been defined, "<no_message>" is saved.
        @param record: This is the object which is provided with all the data from the logging event. The data which are accessed to be saved into the table log of the database.
        @type record: C{logging.LogRecord}
        @postcondition: It saves a new logging entry into the table log of the database which was passed in the constructor of the class if the level of the log is higher than the L{level<DbHandler.level>} which was assigned during the initialization of the object.
        @note: if the database connection failed, a warning log message would be saved into a file identified by the parameter I{aux_filename} which was passed into the object initialization.
        @warning: if the database insertion has failed, a log message would have been saved into a file identified by the parameter I{aux_filename} which was passed into the object initialization.
        """

        if record.levelno >= self.level:
            toRecord = []
            program_stack_string = ""
            if record.exc_info:
                program_stack_string = "\n" + self._formatter.formatException(record.exc_info)

            if record.name is None:
                toRecord.append('<no_name>')
            else:
                toRecord.append(record.name)

            toRecord.append(record.levelno)

            if record.module is None:
                toRecord.append('<no_module>')
            else:
                toRecord.append(record.module)

            if record.lineno is None:
                toRecord.append(0)
            else:
                toRecord.append(record.lineno)

            if record.funcName is None:
                toRecord.append('<no_funcName>')
            else:
                toRecord.append(record.funcName)

            if record.created is None:
                toRecord.append(0.0)
            else:
                toRecord.append(record.created)

            if record.msecs is None:
                toRecord.append(0.0)
            else:
                toRecord.append(record.msecs)

            if record.getMessage() is None:
                toRecord.append(program_stack_string)
            else:
                message = (str(record.getMessage()) + program_stack_string.replace("'", "\""))
                toRecord.append(message[:self.MAX_LEN])

            self.tasks.put(toRecord)

            if self.log_bus:
                names = ["logger", "level", "module", "line", "func", "time", "msecs", "message"]
                d = dict(zip(names, toRecord))
                j = json.dumps(d)
                try:
                    self.log_bus.post(j)
                except:
                    print("*** Exception posting to shared memory destination, disabling")
                    self.log_bus = None

    def flush(self):
        """
        Ensure all logging output has been flushed into the database. If there is any sql sentence left to be executed, this call will force it to be carried out.
        @postcondition: all the pending sql sentences in the cursor has been executed by the database manager.
        @warning: if the database commit operation failed, a log message would be saved into a file identified by the parameter I{aux_filename} which was passed into the object initialization.
        """
        pass
