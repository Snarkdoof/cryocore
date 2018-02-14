from __future__ import print_function
import time
import random
import json
import threading

from CryoCore import API
from CryoCore.Core.InternalDB import mysql

PRI_HIGH = 10
PRI_NORMAL = 5
PRI_LOW = 2
PRI_BULK = 0

TYPE_NORMAL = 1
TYPE_ADMIN = 2
TYPE_MANUAL = 3

STATE_PENDING = 1
STATE_ALLOCATED = 2
STATE_COMPLETED = 3
STATE_FAILED = 4
STATE_TIMEOUT = 5
STATE_CANCELLED = 6

TASK_TYPE = {
    TYPE_NORMAL: "Worker",
    TYPE_ADMIN: "AdminWorker",
    TYPE_MANUAL: "ManualWorker"
}

PRI_STRING = {
    PRI_HIGH: "high",
    PRI_NORMAL: "normal",
    PRI_LOW: "low",
    PRI_BULK: "bulk"
}


class JobDB(mysql):

    def __init__(self, runname, module, steps=1, auto_cleanup=True):

        self._runname = random.randint(0, 2147483647)  # Just ignore the runname for now
        self._actual_runname = runname
        self._module = module
        mysql.__init__(self, "JobDB", db_name="JobDB")

        if not runname and not module:
            return  # Is a worker, can only allocate/update jobs

        # Multi-insert
        self._addlist = []
        self._addtimer = None
        self._addLock = threading.Lock()

        # Add owner, comments, dates etc to run
        statements = [
            """CREATE TABLE IF NOT EXISTS runs (
                runid INT PRIMARY KEY AUTO_INCREMENT,
                runname VARCHAR(128) UNIQUE,
                module VARCHAR(256),
                steps INT DEFAULT 1
            )""",
            """CREATE TABLE IF NOT EXISTS jobs (
                    jobid BIGINT PRIMARY KEY AUTO_INCREMENT,
                    runid INT NOT NULL,
                    step INT DEFAULT 0,
                    taskid INT DEFAULT 0,
                    type TINYINT,
                    priority TINYINT,
                    state TINYINT,
                    tsadded DOUBLE,
                    tsallocated DOUBLE DEFAULT NULL,
                    expiretime SMALLINT,
                    node VARCHAR(128) DEFAULT NULL,
                    worker SMALLINT DEFAULT NULL,
                    retval TEXT DEFAULT NULL,
                    module VARCHAR(256) DEFAULT NULL,
                    modulepath TEXT DEFAULT NULL,
                    workdir TEXT DEFAULT NULL,
                    args TEXT DEFAULT NULL,
                    nonce INT DEFAULT 0,
                    itemid BIGINT DEFAULT 0,
                    tschange TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
            )""",
            """CREATE TABLE IF NOT EXISTS filewatch (
                fileid INT PRIMARY KEY AUTO_INCREMENT,
                rootpath VARCHAR(256) NOT NULL,
                relpath VARCHAR(256) NOT NULL,
                mtime DOUBLE NOT NULL,
                stable BOOL DEFAULT 0,
                public BOOL DEFAULT 0,
                done BOOL DEFAULT 0,
                runname VARCHAR(128),
                tschange TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )""",
            "CREATE INDEX job_state ON jobs(state)",
            "CREATE INDEX job_type ON jobs(type)"
        ]

        # Minor upgrade-hack
        try:
            c = self._execute("SELECT workdir FROM jobs LIMIT 1")
            c.fetchall()
        except:
            try:
                print("*** Job table is bad, dropping it")
                self._execute("DROP TABLE jobs")
            except:
                pass

        self._init_sqls(statements)

        try:
            c = self._execute("SELECT itemid FROM jobs WHERE jobid=0")
            c.fetchone()
        except:
            # Old table, upgrade it
            print("*** Updating jobdb table")
            self._execute("ALTER TABLE jobs ADD (itemid BIGINT DEFAULT 0)")

        c = self._execute("SELECT runid FROM runs WHERE runname=%s", [self._runname])
        row = c.fetchone()
        if row:
            self._runid = row[0]
            self._execute("UPDATE runs SET module=%s, steps=%s WHERE runid=%s", [module, steps, self._runid])
        else:
            c = self._execute("INSERT INTO runs (runname, module, steps) VALUES (%s, %s, %s)",
                              [self._runname, module, steps])
            self._runid = c.lastrowid

        self._cleanup_thread = None
        if auto_cleanup:
            self._cleanup_thread = threading.Timer(300, self._cleanup_timer_run)
            self._cleanup_thread.start()

    def __del__(self):
        try:
            if self._cleanup_timer:
                self._cleanup_timer.cancel()
        except:
            pass

    def _cleanup_timer_run(self):
        if API.api_stop_event.isSet():
            return  # We're done

        self.cleanup()
        self._cleanup_thread = threading.Timer(300, self._cleanup_timer_run)
        self._cleanup_thread.start()

    def add_job(self, step, taskid, args, jobtype=TYPE_NORMAL, priority=PRI_NORMAL, node=None,
                expire_time=3600, module=None, modulepath=None, workdir=None, itemid=None, multiple=False):

        if not module and not self._module:
            raise Exception("Missing module for job, and no default module!")

        if args is not None:
            args = json.dumps(args)

        if multiple:
            self._addlist.append([self._runid, step, taskid, jobtype, priority, STATE_PENDING, time.time(), expire_time, node, args, module, modulepath, workdir, itemid])
            # Set a timer for commit - if multiple ones have been added, they will be added together
            with self._addLock:
                if self._addtimer is None:
                    self._DEBUGstartts = time.time()
                    self._addtimer = threading.Timer(1.0, self.commit_jobs)
                    self._addtimer.start()
            return

        self._execute("INSERT INTO jobs (runid, step, taskid, type, priority, state, tsadded, expiretime, node, args, module, modulepath, workdir, itemid) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                      [self._runid, step, taskid, jobtype, priority, STATE_PENDING, time.time(), expire_time, node, args, module, modulepath, workdir, itemid])

    def commit_jobs(self):
        """
        TODO: Could do this more efficient if we held the lock for shorter, but it doesn't seem like a big deal for now
        """
        with self._addLock:
            self._addtimer = None  # TODO: Should likely have a lock protecting this one
            if len(self._addlist) == 0:
                # print("*** WARNING: commit_jobs called but no queued jobs")
                return

            SQL = "INSERT INTO jobs (runid, step, taskid, type, priority, state, tsadded, expiretime, node, args, module, modulepath, workdir, itemid) VALUES "
            args = []
            for job in self._addlist:
                SQL += "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),"
                args.extend(job)

                if len(args) > 1000:
                    self._execute(SQL[:-1], args)
                    SQL = "INSERT INTO jobs (runid, step, taskid, type, priority, state, tsadded, expiretime, node, args, module, modulepath, workdir, itemid) VALUES "
                    args = []
            if len(args) > 0:
                self._execute(SQL[:-1], args)
            self._addlist = []

    def cancel_job(self, jobid):
        self._execute("UPDATE jobs SET state=%d WHERE jobid=%s", (STATE_CANCELLED, jobid))

    def force_stopped(self, workerid, node):
        self._execute("UPDATE jobs SET state=%s, retval='{\"error\":\"Worker killed\"}' WHERE worker=%s AND node=%s",
                      [STATE_FAILED, workerid, node])

    def get_job_state(self, jobid):
        """
        Check that the job hasn't been updated, i.e. cancelled or removed
        """
        c = self._execute("SELECT state FROM jobs WHERE jobid=%s", [jobid])
        row = c.fetchone()
        if row:
            return row[0]
        return None

    def allocate_job(self, workerid, type=TYPE_NORMAL, node=None, max_jobs=1):
        # TODO: Check for timeouts here too?

        if 0:
            self._execute("LOCK TABLES jobs WRITE")
            try:
                args = [type, STATE_PENDING]
                SQL = "SELECT jobid, step, taskid, type, priority, args, runid, jobs.module, jobs.modulepath, workdir, itemid FROM jobs WHERE type=%s AND state=%s AND "
                if node:
                    SQL += "(node IS NULL or node=%s) "
                    args.append(node)
                else:
                    SQL += "node IS NULL "
                SQL += " ORDER BY priority DESC, tsadded LIMIT %s"
                args.append(max_jobs)
                c = self._execute(SQL, args)
                if c.rowcount == 0:
                    return []

                jobs = []
                SQL = "UPDATE jobs SET state=%s, tsallocated=%s, node=%s, worker=%s WHERE "
                params = [STATE_ALLOCATED, time.time(), node, workerid]
                for jobid, step, taskid, t, priority, args, runid, module, modulepath, workdir, itemid in c.fetchall():
                    if args:
                        args = json.loads(args)
                    jobs.append({"id": jobid, "step": step, "taskid": taskid, "type": t, "priority": priority,
                                 "args": args,  "runname": runid, "module": module, "modulepath": modulepath,
                                 "workdir": workdir, "itemid": itemid})
                    SQL += "jobid=%s OR "
                    params.append(jobid)
                if len(params) > 4:
                    self._execute(SQL[:-4], params)
            finally:
                self._execute("UNLOCK TABLES")

            return jobs

        nonce = random.randint(0, 2147483647)
        args = [STATE_ALLOCATED, time.time(), node, workerid, nonce, type, STATE_PENDING]
        SQL = "UPDATE jobs SET state=%s, tsallocated=%s, node=%s, worker=%s, nonce=%s WHERE type=%s AND state=%s AND "
        if node:
            SQL += "(node IS NULL or node=%s) "
            args.append(node)
        else:
            SQL += "node IS NULL "
        SQL += " ORDER BY priority DESC, tsadded LIMIT %s"
        args.append(max_jobs)
        c = None
        for i in range(0, 3):
            try:
                c = self._execute(SQL, args)
                break
            except:
                self.log.exception("Failed to get job, retrying")
        if not c:
            raise Exception("Failed to get job")

        ex = None
        if c.rowcount > 0:
            # We must not fail on this, so loop a few times to try to avoid it being allocated but not returned!
            for i in range(0, 10):
                try:
                    c = self._execute("SELECT jobid, step, taskid, type, priority, args, runname, jobs.module, jobs.modulepath, runs.module, steps, workdir, itemid FROM jobs, runs WHERE runs.runid=jobs.runid AND nonce=%s", [nonce])
                    jobs = []
                    for jobid, step, taskid, t, priority, args, runname, jmodule, modulepath, rmodule, steps, workdir, itemid in c.fetchall():
                        if args:
                            args = json.loads(args)
                        if jmodule:
                            module = jmodule
                        else:
                            module = rmodule
                        jobs.append({"id": jobid, "step": step, "taskid": taskid, "type": t, "priority": priority,
                                     "args": args, "runname": runname, "module": module, "modulepath": modulepath,
                                     "steps": steps, "workdir": workdir, "itemid": itemid})

                    return jobs
                except Exception as e:
                    ex = e
                    self.log.exception("Problem getting the job that I allocated, trying to get it again in a second")
                    time.sleep(1.0)
        if ex:
            raise ex
        return []

    def list_jobs(self, step=None, state=None, notstate=None, since=None):
        jobs = []
        SQL = "SELECT jobid, step, taskid, type, priority, args, tschange, state, expiretime, module, modulepath, tsallocated, node, worker, retval, workdir, itemid FROM jobs WHERE runid=%s"
        args = [self._runid]
        if step:
            SQL += " AND step=%s"
            args.append(step)
        if state:
            SQL += " AND state=%s"
            args.append(state)
        if notstate:
            SQL += " AND state<>%s"
            args.append(notstate)
        if since:
            SQL += " AND tschange>%s"
            args.append(since)

        SQL += " ORDER BY tschange"
        c = self._execute(SQL, args)
        for jobid, step, taskid, t, priority, args, tschange, state, expire_time, module, modulepath, tsallocated, node, worker, retval, workdir, itemid in c.fetchall():
            if args:
                args = json.loads(args)
            if retval:
                retval = json.loads(retval)
            job = {"id": jobid, "step": step, "taskid": taskid, "type": t, "priority": priority,
                   "node": node, "worker": worker, "args": args, "tschange": tschange, "state": state,
                   "expire_time": expire_time, "module": module, "modulepath": modulepath, "retval": retval,
                   "workdir": workdir, "itemid": itemid}
            if tsallocated:
                job["runtime"] = time.time() - tsallocated
            jobs.append(job)
        return jobs

    def clear_jobs(self):
        self._execute("DELETE FROM jobs WHERE runid=%s", [self._runid])

    def remove_job(self, jobid):
        self._execute("DELETE FROM jobs WHERE runid=%s AND jobid=%s", [self._runid, jobid])

    def update_job(self, jobid, state, step=None, node=None, args=None, priority=None, expire_time=None, retval=None):
        SQL = "UPDATE jobs SET state=%s"
        params = [state]

        if node:
            SQL += ",node=%s"
            params.append(node)
        if step:
            SQL += ",step=%s"
            params.append(step)
        if args:
            SQL += ",args=%s"
            params.append(json.dumps(args))
        if priority:
            SQL += ",priority=%s"
            params.append(priority)
        if expire_time:
            SQL += ",expire_time=%s"
            params.append(expire_time)
        if retval:
            SQL += ",retval=%s"
            params.append(json.dumps(retval))
        SQL += " WHERE jobid=%s"  # AND runid=%s"
        params.append(jobid)
        # params.append(self._runid)

        c = self._execute(SQL, params)
        if c.rowcount == 0:
            self.log.error("Error: %s(%s)" % (SQL, params))
            raise Exception("Failed to update, does the job exist or did the state change? (job %s -> %s)" % (jobid, state))

    def cleanup(self):
        """
        Remove done, expired or failed jobs that were completed at least one hour ago
        """
        self._execute("DELETE FROM jobs WHERE state>=%s AND tschange < NOW() - INTERVAL 1 hour", [STATE_COMPLETED])

    def update_timeouts(self):
        self._execute("UPDATE jobs SET state=%s WHERE state=%s AND tsallocated + expiretime < %s", [STATE_TIMEOUT, STATE_ALLOCATED, time.time()])

    def get_jobstats(self):

        steps = {}
        # Find the average processing time for completed on this each step:
        SQL = "SELECT step, AVG(UNIX_TIMESTAMP(tschange) - tsallocated) FROM jobs WHERE runid=%s AND state=%s GROUP BY step"
        c = self._execute(SQL, [self._runid, STATE_COMPLETED])
        for step, avg in c.fetchall():
            steps[step] = {"average": avg}

        # Find the total number of jobs defined pr. step
        SQL = "SELECT step, COUNT(jobid) FROM jobs WHERE runid=%s GROUP BY step"
        c = self._execute(SQL, (self._runid,))
        for step, total in c.fetchall():
            if step not in steps:
                steps[step] = {}
            steps[step]["total_tasks"] = total

        # How many jobs are incomplete
        SQL = "SELECT step, state, COUNT(jobid) FROM jobs WHERE runid=%s GROUP BY step, state"
        c = self._execute(SQL, (self._runid,))
        for step, state, count in c.fetchall():
            if step not in steps:
                steps[step] = {}
            steps[step][state] = count

        return steps

    def get_directory(self, rootpath, runname):
        SQL = "SELECT * FROM filewatch WHERE rootpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, runname))
        return c.fetchall()

    def get_file(self, rootpath, relpath, runname):
        SQL = "SELECT * FROM filewatch WHERE rootpath=%s AND relpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, relpath, runname))
        rows = c.fetchall()
        if len(rows) > 0:
            return rows[0]
        else:
            return None

    def insert_file(self, rootpath, relpath, mtime, stable, public, runname):
        SQL = "INSERT INTO filewatch (rootpath, relpath, mtime, stable, public, runname) VALUES (%s, %s, %s, %s, %s, %s)"
        c = self._execute(SQL, (rootpath, relpath, mtime, stable, public, runname))
        return c.rowcount

    def update_file(self, fileid, mtime, stable, public):
        SQL = "UPDATE filewatch SET mtime=%s, stable=%s, public=%s WHERE fileid=%s"
        c = self._execute(SQL, (mtime, stable, public, fileid))
        return c.rowcount

    def done_file(self, rootpath, relpath, runname):
        SQL = "UPDATE filewatch SET done=1 WHERE rootpath=%s AND relpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, relpath, runname))
        return c.rowcount

    def undone_files(self, rootpath, runname):
        SQL = "UPDATE filewatch SET done=0 WHERE rootpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, runname))
        return c.rowcount

    def reset_files(self, rootpath, runname):
        # SQL = "UPDATE filewatch SET done=0, public=0 WHERE rootpath=%s AND runname=%s"
        SQL = "DELETE FROM filewatch WHERE rootpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, runname))
        return c.rowcount

    def remove_file(self, fileid):
        SQL = "DELETE FROM filewatch WHERE fileid=%s"
        c = self._execute(SQL, (fileid,))
        return c.rowcount


if __name__ == "__main__":
    try:
        print("Testing")
        db = JobDB(None, None)

        # db = JobDB("test1", "hello", 2)
        # db.add_job(0, 1, {"param1":1, "param2": "param2"})
        jobs = db.allocate_job(max_jobs=2)
        for job in jobs:
            print("Got job", job)
            db.update_job(job["id"], db.STATE_COMPLETED)
    finally:
        API.shutdown()
