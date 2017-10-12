#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

from __future__ import print_function
import sys
import time
from argparse import ArgumentParser
import threading

try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

from CryoCore import API
from CryoCloud.Common import jobdb
import CryoCloud.Common

try:
    import imp
except:
    import importlib as imp
modules = {}


def load(modulename):
    if modulename not in modules:
        if sys.version_info.major in [2, 3]:
            # f = open(modulename)
            # modules[modulename] = imp.load_module(modulename, f, modulename, (".py", "U", 1))  # info[0], info[1], info[2])
            info = imp.find_module(modulename)
            modules[modulename] = imp.load_module(modulename, info[0], info[1], info[2])
        else:
            # modules[modulename] = imp.load_module(modulename)
            modules[modulename] = imp.import_module(modulename)

    imp.reload(modules[modulename])
    return modules[modulename]


class HeadNode(threading.Thread):
    def __init__(self, options):
        threading.Thread.__init__(self)

        self.name = "%s.HeadNode" % (options.name)
        self.cfg = API.get_config(self.name, version=options.version)
        self.log = API.get_log(self.name)
        self.status = API.get_status(self.name)
        self.options = options
        self.status["state"] = "Initializing"
        self.status["tasks_created"] = 0
        self.status["tasks_allocated"] = 0
        self.status["tasks_completed"] = 0
        self.status["total_steps"] = self.options.steps
        self.status["step"] = 0
        self.log.debug("Created %s to perform %d steps of %d tasks using" %
                       (self.name, self.options.steps, self.options.tasks))

        # Load the handler
        self.handler = load(options.handler).Handler()
        self.handler.head = self
        self.step = 0

    def stop(self):
        API.api_stop_event.set()

    def create_task(self, step, task):
        """
        Overload or replace to provide the running arguments for the specified task
        """
        return {}

    def __del__(self):
        try:
            self.status["state"] = "Stopped"
        except:
            pass

    def makeDirectoryWatcher(self, dir, onAdd=None, onModify=None, onRemove=None, onError=None, stabilize=5, recursive=True):
            return CryoCloud.Common.DirectoryWatcher(self._jobdb._runid,
                                                     dir,
                                                     jobDB=self._jobdb,
                                                     onAdd=onAdd,
                                                     onModify=onModify,
                                                     onRemove=onRemove,
                                                     onError=onError,
                                                     stabilize=stabilize,
                                                     recursive=recursive)

    def add_job(self, step, taskid, args, jobtype=jobdb.TYPE_NORMAL, priority=jobdb.PRI_NORMAL, node=None, expire_time=None, module=None):
        if expire_time is None:
            expire_time = self.options.max_task_time
        self._jobdb.add_job(step, taskid, args, expire_time=expire_time, module=module)
        if step > self.status["progress"].size[0]:
            self.status.new2d("progress", (self.options.steps, self.options.tasks), expire_time=3*81600, initial_value=0)

        self.status["progress"].set_value((step - 1, taskid), 1)
        self.status["tasks_created"].inc(1)

    def requeue(self, job, node=None, expire_time=None):
        if expire_time is None:
            expire_time = expire_time = job["expire_time"]
        self._jobdb.remove_job(job["id"])
        self.add_job(job["step"], job["taskid"], job["args"], jobtype=job["type"], priority=job["priority"], node=node, expire_time=expire_time, module=job["module"])

    def remove_job(self, job):
        if job.__class__ == int:
            self._jobdb.remove(job)
        else:
            try:
                i = int(job["id"])
            except:
                raise Exception("Need either a task or a jobid to remove")
            self._jobdb.remove(i)

    def start_step(self, step):
        print("Starting step", step, "progress size is", self.status["progress"].size)

        if step > self.status["progress"].size[0]:
            print ("MUST RE-configure progress")
            self.status["total_steps"] = step

        self.step = step
        self.status["step"] = self.step

    def run(self):
        self.status.new2d("progress", (self.options.steps, self.options.tasks), expire_time=3*81600, initial_value=0)
        self.status["avg_task_time_step"] = 0.0
        self.status["avg_task_time_total"] = 0.0
        self.status["eta_step"] = 0
        self.status["eta_total"] = 0

        self._jobdb = jobdb.JobDB(options.name, options.module)

        # TODO: Option for this (and for clear_jobs on cleanup)?
        self._jobdb.clear_jobs()

        self.log.info("Starting")
        self.status["state"] = "Running"

        try:
            self.handler.onReady(self.options.args)  # .split(" "))
        except Exception as e:
            print("Exception in onReady for handler", self.options.handler)
            import traceback
            traceback.print_exc()
            self.log.exception("In onReady handler is %s" % self.options.handler)
            self.status["state"] = "Done"
            return

        print("Progress status parameter thingy with size", self.options.steps, "x", self.options.tasks)
        self.start_step(1)
        failed = False
        while not API.api_stop_event.is_set():
            try:
                # Wait for all tasks to complete
                # TODO: Add timeouts to re-queue tasks that were incomplete
                last_run = 0
                notified = False
                while not API.api_stop_event.is_set():
                    self._jobdb.update_timeouts()
                    updates = self._jobdb.list_jobs(since=last_run, notstate=jobdb.STATE_PENDING)
                    for job in updates:
                        print("Checking job update", job)
                        last_run = job["tschange"]
                        if job["state"] == jobdb.STATE_ALLOCATED:
                            self.status["progress"].set_value((job["step"]-1, job["taskid"]), 3)
                            self.handler.onAllocated(job)

                        elif job["state"] == jobdb.STATE_FAILED:
                            self.status["progress"].set_value((job["step"]-1, job["taskid"]), 2)
                            self.handler.onError(job)

                        elif job["state"] == jobdb.STATE_COMPLETED:
                            self.status["progress"].set_value((job["step"]-1, job["taskid"]), 10)

                            stats = self._jobdb.get_jobstats()
                            # print("STATS", stats)
                            if self.step in stats:
                                self.status["avg_task_time_step"] = stats[self.step]
                            # self.status["avg_task_time_total"] = sum_all / total_len
                            print("Completed job, calling handler")
                            self.handler.onCompleted(job)
                        elif job["state"] == jobdb.STATE_TIMEOUT:
                            self.status["progress"].set_value((job["step"]-1, job["taskid"]), 0)
                            self.handler.onTimeout(job)

                    # Still incomplete jobs?
                    pending = self._jobdb.list_jobs(self.step, notstate=jobdb.STATE_COMPLETED)
                    if len(pending) == 0:
                        if not notified:
                            # print("Step", step, "Completed")
                            self.handler.onStepCompleted(self.step)
                            notified = True
                    else:
                        notified = False

                    if len(updates) == 0:
                        time.sleep(1)
                    continue
            except Exception as e:
                print("Exception in HEAD (check logs)", e)
                self.log.exception("In main loop")
                failed = True
                break
            finally:
                failed = True

        if failed:
            self.status["state"] = "Done"
        else:
            print("Stopped, sending shutdown")
            # API.shutdown()  # Not really necessary
            self.status["state"] = "Done"
            self.log.info("Completed all work")

        # CLEAN UP
        try:
            self._jobdb.clear_jobs()
        except:
            print("Failed to clear jobs")


if __name__ == "__main__":
    parser = ArgumentParser(description="HEAD node for CryoCloud processing")

    parser.add_argument("--runid", dest="runid",
                        default=0,
                        help="RunID to use")

    parser.add_argument("-v", "--version", dest="version",
                        default="default",
                        help="Config version to use on")

    parser.add_argument("--name", dest="name",
                        default="ExampleProcessor",
                        help="The name of this HeadNode")

    parser.add_argument("--steps", dest="steps",
                        default=1,
                        help="Number of steps in this processing")

    parser.add_argument("--tasks", dest="tasks",
                        default=10,
                        help="Number of tasks for each step in this processing")

    parser.add_argument("--module", dest="module",
                        default=None,
                        help="The default module imported by workers to process these jobs")

    parser.add_argument("--handler", dest="handler",
                        default=None,
                        help="The handler for this master")

    parser.add_argument("--max-task-time", dest="max_task_time",
                        default=None,
                        help="Maximum time a task will be allowed to run before it is re-queued")

    parser.add_argument("--args", dest="args",
                        default=None,
                        help="Arguments passed to the Handler")

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args()
    options.steps = int(options.steps)
    options.tasks = int(options.tasks)
    if options.max_task_time:
        options.max_task_time = float(options.max_task_time)
    # if options.module is None:
    #    raise SystemExit("Need a module")
    if options.handler is None:
        raise SystemExit("Need handler")

    if options.args:
        options.args = options.args.split()  # parser.convert_arg_line_to_args(options.args)
    #if options.module:
    #    options.module = options.module.replace("/", ".").replace(".py", "")
    # if options.handler:
    #    options.handler = options.handler.replace("/", ".").replace(".py", "")

    import signal

    def handler(signum, frame):
        print("Head stopped by user signal")
        API.shutdown()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGQUIT, handler)

    try:
        headnode = HeadNode(options)
        headnode.start()

        # We create an event triggered by the head node when it's done
        stopevent = threading.Event()
        headnode.status["state"].add_event_on_value("Done", stopevent)

        print("Running, press CTRL-C to end")
        while not API.api_stop_event.is_set() and not stopevent.isSet():
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    finally:
        print("Shutting down")
        API.shutdown()
    print("Done")
