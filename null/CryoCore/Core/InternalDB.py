

class mysql():
    """
    Fake mysql class using sqlite3 inmemory database
    """
    def __init__(self, name, config=None, can_log=True, db_name=None, ssl=None,
                 num_connections=3, min_conn_time=10, is_direct=False):
        pass

    def run(self):
        pass
        # self._db = sqlite3.connect(":memory:", isolation_level=None)

    def _execute(self, query, params=None):
        raise Exception("NULL DB is being used, but that doesn't work")
        if params:
            q = query.replace("%s", "?")
            return self._db.execute(q, params)
        else:
            return self._db.execute(query)

    def _init_sqls(self, sql_statements):
        """
        Prepare the database with the given SQL statements (statement, params)
        Errors are ignored for indexes, warnings are logged and not sent
        to the console
        """
        return
        for statement in sql_statements:
            try:
                if statement.lower().startswith("create index"):
                    ignore_error = True
                else:
                    ignore_error = False
                self._execute(statement, ignore_error=ignore_error)
            except Exception:
                pass
