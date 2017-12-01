#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

from __future__ import print_function
import sys
import psutil
import time
import socket
from argparse import ArgumentParser
try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

import threading
try:
    from queue import Empty
except:
    from Queue import Empty


from CryoCore import API
from CryoCloud.Common import jobdb

import multiprocessing

try:
    import imp
except:
    import importlib as imp

sys.path.append("CryoCloud/Modules/")


modules = {}


def load(modulename):
    print("LOADING MODULE", modulename)
    # TODO: Also allow getmodulename here to allow modulename to be a .py file
    if modulename.endswith(".py"):
        import inspect
        modulename = inspect.getmodulename(modulename)

    if modulename not in modules:
        try:
            info = imp.find_module(modulename)
            modules[modulename] = imp.load_module(modulename, info[0], info[1], info[2])
        except:
            modules[modulename] = imp.import_module(modulename)

    imp.reload(modules[modulename])
    return modules[modulename]


class Worker(multiprocessing.Process):

    def __init__(self, workernum, stopevent, type=jobdb.TYPE_NORMAL):
        super(Worker, self).__init__()

        # self._stop_event = stopevent
        self._stop_event = threading.Event()
        self._manager = None
        self.workernum = workernum

        self._name = None
        self._jobid = None
        self.module = None
        self.log = None
        self.status = None
        self.inqueue = None
        self._is_ready = False
        self._type = type
        self._worker_type = jobdb.TASK_TYPE[type]
        self.wid = "%s-%s_%d" % (self._worker_type, socket.gethostname(), self.workernum)

        self._current_job = (None, None)

        print("%s %s created" % (self._worker_type, workernum))

    def _switchJob(self, job):
        if self._current_job == (job["runname"], job["module"]):
            # Same job
            # return
            pass

        print("Switching job from", self._current_job, (job["runname"], job["module"]))
        self._current_job = (job["runname"], job["module"])
        self._module = None

        try:
            self._module = job["module"]
            if self._module == "test":
                self._module = None
            else:
                self.log.debug("Loading module %s" % self._module)
                self._module = load(self._module)
                print("Loading of", self._module, "successful")
        except Exception as e:
            self._is_ready = False
            print("Import error:", e)
            self.status["state"] = "Import error"
            self.status["state"].set_expire_time(3 * 86400)
            self.log.exception("Failed to get module")
            raise e
        try:
            self.log.info("%s allocated to job %s of %s (%s)" % (self._worker_type, job["id"], job["runname"], job["module"]))
            self.status["state"] = "Connected"
            self.status["num_errors"] = 0.0
            self.status["last_error"] = ""
            self.status["host"] = socket.gethostname()
            self.status["progress"] = 0

            for key in ["state", "num_errors", "last_error", "progress"]:
                self.status[key].set_expire_time(3 * 86400)

            self._is_ready = True

        except:
            self.log.exception("Some other exception")

    def run(self):

        def sighandler(signum, frame):
            print("%s GOT SIGNAL" % self._worker_type)
            # API.shutdown()
            self._stop_event.set()

        signal.signal(signal.SIGINT, sighandler)
        self.log = API.get_log(self.wid)
        self.status = API.get_status(self.wid)
        self._jobdb = jobdb.JobDB(None, None)

        while not self._stop_event.is_set():
            try:
                jobs = self._jobdb.allocate_job(self.workernum, node=socket.gethostname(), max_jobs=1, type=self._type)
                if len(jobs) == 0:
                    time.sleep(1)
                    continue
                job = jobs[0]
                print("Got job", job)
                self._switchJob(job)
                if not self._is_ready:
                    time.sleep(1)
                    continue
                self.log.debug("Got job for processing: %s" % job)
                self._process_task(job)
            except Empty:
                self.status["state"] = "Idle"
                continue
            except KeyboardInterrupt:
                break
            except Exception as e:
                print("No job", e)
                # log.error("Failed to get job")
                self.status["state"] = "Disconnected"
                # Likely a manager crash - reconnect
                time.sleep(5)
                continue

        # print(self._worker_type, self.wid, "stopping")
        # self._stop_event.set()
        print(self._worker_type, self.wid, "stopped")
        self.status["state"] = "Stopped"

    def process_task(self, task):
        """
        Actual implementation of task processing. Update progress to self.status["progress"]
        Must return progress, returnvalue where progress is a number 0-100 (percent) and
        returnvalue is None or anything that can be converted to json
        """
        import random
        progress = 0
        while not self._stop_event.is_set() and progress < 100:
            if random.random() > 0.99:
                self.log.error("Error processing task %s" % str(task))
                raise Exception("Randomly generated error")
            time.sleep(.5 + random.random() * 5)
            progress = min(100, progress + random.random() * 15)
            self.status["progress"] = progress
        return progress, None

    def _process_task(self, task):
        self.status["state"] = "Processing"
        self.log.debug("Process task %s" % str(task))
        taskid = "%s.%s-%s_%d" % (task["runname"], self._worker_type, socket.gethostname(), self.workernum)

        print(taskid, "Processing", task)

        # Report that I'm on it
        start_time = time.time()
        self.status["state"] = "Processing"

        self.log.debug("Processing job %s" % str(task))
        self.status["progress"] = 0

        new_state = jobdb.STATE_FAILED
        try:
            if self._module is None:
                progress, ret = self.process_task(task)
            else:
                progress, ret = self._module.process_task(self, task)
            task["progress"] = progress
            if int(progress) != 100:
                raise Exception("ProcessTask returned unexpected progress: %s vs 100" % progress)
            task["result"] = "Ok"
            new_state = jobdb.STATE_COMPLETED
            self.status["last_processing_time"] = time.time() - start_time
        except Exception as e:
            print("Processing failed", e)
            self.log.exception("Processing failed")
            task["result"] = "Failed"
            self.status["num_errors"].inc()
            self.status["last_error"] = str(e)
            ret = None

        task["state"] = "Stopped"
        task["processing_time"] = time.time() - start_time

        # Update to indicate we're done
        self._jobdb.update_job(task["id"], new_state, ret)


class NodeController(threading.Thread):

    def __init__(self, options):
        threading.Thread.__init__(self)
        self._worker_pool = []
        # self._stop_event = multiprocessing.Event()
        self._stop_event = API.api_stop_event
        self._options = options
        self._manager = None

        if options.cpu_count:
            psutil.cpu_count = lambda x=None: int(self._options.cpu_count)

        # We need to start the workers before we use the API - forking seems to make a hash of the DB connections
        if options.workers:
            workers = int(options.workers)
        else:
            workers = psutil.cpu_count()
        for i in range(0, workers):
            # wid = "%s.%s.Worker-%s_%d" % (self.jobid, self.name, socket.gethostname(), i)
            print("Starting worker %d" % i)
            w = Worker(i, self._stop_event)
            # w = multiprocessing.Process(target=worker, args=(i, self._options.address, self._options.port, AUTHKEY, self._stop_event))  # args=(wid, self._task_queue, self._results_queue, self._stop_event))
            w.start()
            self._worker_pool.append(w)

        for i in range(0, int(options.adminworkers)):
            print("Starting adminworker %d" % i)
            aw = Worker(i, self._stop_event, type=jobdb.TYPE_ADMIN)
            aw.start()
            self._worker_pool.append(aw)

        self.cfg = API.get_config("NodeController")
        self.cfg.set_default("expire_time", 86400)  # Default one day expire time
        self.cfg.set_default("sample_rate", 5)
        # My name
        self.name = "NodeController." + socket.gethostname()
        # TODO: CHECK IF A NODE CONTROLLER IS ALREADY RUNNING ON THIS DEVICE (remotestatus?)

        self.log = API.get_log(self.name)
        self.status = API.get_status(self.name)

        self.status["state"] = "Idle"
        for key in ["user", "nice", "system", "idle", "iowait"]:
            self.status["cpu.%s" % key] = 0
            self.status["cpu.%s" % key].set_expire_time(self.cfg["expire_time"])
        for key in ["total", "available", "active"]:
            self.status["memory.%s" % key] = 0
            self.status["memory.%s" % key].set_expire_time(self.cfg["expire_time"])

        try:
            self.status["cpu.count"] = psutil.cpu_count()
            self.status["cpu.count_physical"] = psutil.cpu_count(logical=False)
        except:
            pass  # No info

    def run(self):
        self.status["state"] = "Running"
        while not API.api_stop_event.is_set():
            last_run = time.time()
            # CPU info for the node
            cpu = psutil.cpu_times_percent()
            for key in ["user", "nice", "system", "idle", "iowait"]:
                self.status["cpu.%s" % key] = cpu[cpu._fields.index(key)] * psutil.cpu_count()

            # Memory info for the node
            mem = psutil.virtual_memory()
            for key in ["total", "available", "active"]:
                self.status["memory.%s" % key] = mem[mem._fields.index(key)]

            # Disk space
            partitions = psutil.disk_partitions()
            for partition in partitions:
                diskname = partition.mountpoint[partition.mountpoint.rfind("/") + 1:]
                if diskname == "":
                    diskname = "root"
                diskusage = psutil.disk_usage(partition.mountpoint)
                for key in ["total", "used", "free", "percent"]:
                    self.status["%s.%s" % (diskname, key)] = diskusage[diskusage._fields.index(key)]
                    self.status["%s.%s" % (diskname, key)].set_expire_time(self.cfg["expire_time"])

            if 0:
                try:
                    job = self.get_job_description()
                    print(job)
                except:
                    self._manager = None
                    self.log.exception("Job description failed!")

            time_left = max(0, self.cfg["sample_rate"] + time.time() - last_run)
            time.sleep(time_left)

        self.status["state"] = "Stopping workers"
        self._stop_event.set()
        left = len(self._worker_pool)
        for w in self._worker_pool:
            w.join()
            left -= 1
            self.log.debug("Worker stopped, %d left" % (left))

        if self._manager:
            self._manager.shutdown()

        print("All shut down")
        self.status["state"] = "Stopped"


if __name__ == "__main__":

    parser = ArgumentParser(description="Worker node")

    parser.add_argument("-n", "--num-workers", dest="workers",
                        default=None,
                        help="Number of workers to start - default one pr virtual core")

    parser.add_argument("-a", "--num-admin-workers", dest="adminworkers",
                        default=1,
                        help="Number of admin workers to start - default one")

    parser.add_argument("--cpus", dest="cpu_count", default=None,
                        help="Number of CPUs, use if not detected or if the detected value is wrong")

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args()

    if not options.cpu_count:
        try:
            psutil.num_cpus()
        except:
            raise SystemExit("Can't detect number of CPUs, please specify with --cpus")

    import signal
    try:
        node = NodeController(options)
        node.daemon = True
        node.start()

        def sighandler(signum, frame):
            if API.api_stop_event.isSet():
                print("User insisting, trying to die")
                raise SystemExit("User aborted")

            print("Stopped by user signal")
            API.shutdown()

        signal.signal(signal.SIGINT, sighandler)

        while not API.api_stop_event.isSet():
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break
    finally:
        API.shutdown()
