import sqlite3
import logging
import sys

DEBUG = False


class mysql:
    """
    Dropin class for InternalDB.mysql using sqlite3
    """

    def __init__(self, name, config=None, can_log=True, db_name=None, ssl=None,
                 num_connections=3, min_conn_time=10, is_direct=False):
        """
        Generic database wrapper
        if can_log is set to False, it will not try to log (should
        only be used for the logger!)
        min_conn_time is the minimum amount of time a DB connection is allowed to live since last execute before LRU can recycle it
        """
        self._db_name = db_name
        self._my_name = name
        self._mycfg = config
        self.cursor = None
        if self._mycfg and not isinstance(config, str):
            self._mycfg.set_default("min_conn_time", 10.0)
        self._min_conn_time = min_conn_time
        self.log = logging.getLogger("sqlite")
        if len(self.log.handlers) < 1:

            hdlr = logging.StreamHandler(sys.stdout)
            # ihdlr = logging.handlers.RotatingFileHandler("UAVConfig.log",
            #                                            maxBytes=26214400)
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s')
            hdlr.setFormatter(formatter)
            self.log.addHandler(hdlr)
            self.log.setLevel(logging.DEBUG)

        if db_name is None:
            if isinstance(config, str):
                db_name = config
            else:
                db_name = "global"
        self.db = None

    def _init_sqls(self, sql_statements):
        """
        Prepare the database with the given SQL statements (statement, params)
        Errors are ignored for indexes, warnings are logged and not sent
        to the console
        """
        if DEBUG and self.log:
            self.log.debug("Initializing tables: %s" % os.getpid())

        for statement in sql_statements:
            try:
                if statement.lower().startswith("create index"):
                    ignore_error = True
                else:
                    ignore_error = False
                self._execute(statement, ignore_error=ignore_error)
            except sqlite3.Warning as e:
                if self.log:
                    self.log.warning("Preparing table '%s': %s" % (statement, e))

        if DEBUG and self.log:
            self.log.debug("Initializing tables DONE")


    def _get_cursor(self):
        if self.db is None:
            self.db = sqlite3.connect(self.name + ".sqlite3", isolation_level=None)

        return self.db.cursor()

    def _execute(self, SQL, parameters=None,
                 temporary_connection=True,
                 ignore_error=False,
                 insist_direct=False):
        """
        NEVER use insist_direct if you don't know what you are doing
        """

        # We need to replace "%s" with "?" for some crazy reason
        SQL = SQL.replace("%s", "?")

        # If there are any ON UPDATE CURRENT_TIMESTAMP we'll freak out, don't do it
        if SQL.find("ON UPDATE CURRENT_TIMESTAMP") > -1:
            print("WARNING: Automatic timestamp update ignored due to sqlite usage")
            SQL = SQL.replace("ON UPDATE CURRENT_TIMESTAMP", "")
        if SQL.find("INSERT IGNORE") > -1:
            SQL = SQL.replace(" IGNORE", "")
            ignore_error = True
        if SQL.find("AUTO_INCREMENT") > -1:
            SQL = SQL.replace("AUTO_INCREMENT", "AUTO INCREMENT")


        if parameters is None:
            parameters = []
        for i in range(10):
            try:
                cursor = self._get_cursor()
                # print("SQL:", SQL, parameters, ignore_error)
                cursor.execute(SQL, parameters)
                return cursor
            except sqlite3.OperationalError as e:
                if ignore_error:
                    return
                print("Got operational error:", e.__class__, "[%s]" % str(e))
                if str(e) == "database is locked":
                    time.sleep(0.1)  # Retry later
                elif str(e) == "cannot commit - no transaction is active":
                    return
                else:
                    print(e)
                    raise e
            except Exception as e:
                # self.cursor = None
                try:
                    self.db.close()
                except:
                    pass
                self.db = None

                if ignore_error:
                    return cursor
                raise e

        if not ignore_error and "error" in retval:
            raise Exception(retval["error"])
        return None
