import CryoCloud
from argparse import ArgumentParser
import os.path
import random

import re

random.seed()


class Handler(CryoCloud.DefaultHandler):

    def onReady(self, args):

        self._taskid = 1

        description = """This program processes only Radarsat-2 ScanSAR products in FRED format,
and only filenames of the form
    RS2_SAR_FRED_<start datetime>_<end datetime>_...SCAN(NARROW|WIDE)
are recognized.

The processing is performed using a IDL routine called `make_rs2_rvl` and that
routine must be available in the IDL search path when IDL is called from this
program.  When a precompiled file `make_rs2_rvl.sav` is used, the easiest way
is to keep the save file and this program in the same directory.

To save time when revisiting a data repository, the program keeps track of
which files have been attempted previously and their outcomes.  The record is
kept in a file named `.rvl_state` which will exist or be created in the user's
home directory.  Unless using `-f`, files which were found previously are
ignored on subsequent attempt, regardless of processing success or failure.

With -p, only Radarsat-2 RVL products in GSAR format are recognized, and plots
of normalized radar cross section (NRCS), Doppler and radial surface velocity
(RVL) are made, using the IDL routine `make_rs2_rvl_plots`."""

        parser = ArgumentParser(description=description)

        parser.add_argument("-i", dest="input_dir",
                            default="./",
                            help="Source directory to monitor for files")

        parser.add_argument("-w", "--working-directory", dest="working_dir",
                            default="./",
                            help="Working directory")

        parser.add_argument("-o", dest="output_dir",
                            default="/data/RVL/",
                            help="Target directory")

        parser.add_argument("-r", "--recursive", action="store_true", dest="recursive", default="False",
                            help="Recursively monitor input directory for changes")

        parser.add_argument("-f", "--force", action="store_true", dest="force", default="False",
                            help="Force re-processing of all products even if they have been successfully processed before")

        parser.add_argument("-p", "--plot", action="store_true", dest="plot", default="False",
                            help="Make plots from GSAR files")

        parser.add_argument("--geo-id", dest="geoid", default="/auto/zfs01/rawdata/auxdata/dem/EGM96/")

        parser.add_argument("-c", "--caldir", dest="cal_dir",
                            default=None,
                            help="Directory to look for calibration files")

        parser.add_argument("-t", "--tempdir", dest="temp_dir",
                            default="./",
                            help="Temporary directory (on worker nodes) where data will be kept during processing")
        self.options = None
        try:
            self.options = parser.parse_args(args)

            # Compulsory stuff
            if not self.options.cal_dir:
                raise Exception("Need calibration directory, please specify")

        except Exception as e:
            print "Exception parsing options", e
            self.head.stop()
        finally:
            if not self.options:
                parser.print_help()
                self.head.stop()

        print "Monitoring directory", self.options.input_dir
        self.dir_monitor = self.head.makeDirectoryWatcher(self.options.input_dir, self.onAdd, recursive=self.options.recursive)
        self.dir_monitor.start()

    def onAdd(self, info):
        self.process(info)
        # Also create plot?

    def process(self, info):

        if not re.search(r'(NARROW|WIDE)$', info["relpath"], re.I):
            return
        fdir, fname = os.path.split(info["fullpath"])

        # print("Found file for processing", info["relpath"])

        # See if we can find a calfile to go with the data
        if self.options.cal_dir is None:
            calfile = None
        else:
            # [:28] means including HHMM but not SSss
            calfile = [x for x in os.listdir(self.options.cal_dir)
                       if fname[:28] in x and x[-4:].lower() == '.sav']
            calfile = os.path.join(self.options.cal_dir, calfile[0]) if len(calfile) > 0 else None

        # Create a job for this file
        e = ""
        for i in range(10):
            workdir = "%s/%d" % (self.options.temp_dir, random.randint(0, 4000000))
            try:
                os.makedirs(workdir)
                break
            except Exception as ex:
                e = ex

        if not os.path.exists(workdir):
            raise SystemExit("Failed to create temporary directory %s" % (workdir, e))
        # print "Creating job for file", info["fullpath"]
        idlcmd = "make_rs2_rvl, '%s', outputpath='%s'" % (info["fullpath"], workdir)
        if calfile:
            idlcmd += ", calfile='%s'" % calfile

        # if self.options.geoid:
        #    idlcmd += ", geoid='%s'" % self.options.geoid

        cmd = ["-e", idlcmd]

        self.head.add_job(1, self._taskid, {"dir": self.options.working_dir, "cmd": cmd, "filePath": info["relpath"], "level": 0, "workdir": workdir}, module="idl", priority=self.PRI_LOW)
        self._taskid += 1

    def onAllocated(self, task):
        print "Allocated", task["taskid"], "by node", task["node"], "worker", task["worker"]

    def onCompleted(self, task):
        print "Task completed:", task["taskid"], task["step"], task

        if task["args"]["level"] == 0:
            # Create plot job
            source = None
            for fn in os.listdir(task["args"]["workdir"]):
                if fn.endswith(".gsar"):
                    source = fn
                    break
            if not fn:
                print "Missing gsar file"
                self.log.error("Job done but missing .gsar file in workdir %s" % task["args"]["workdir"])
                return

            # Create the job
            print "Queuing plot job"
            idlcmd = "make_rs2_rvl_plots, '%s', outputpath='%s'" % (source, "%s/figures/%s" % (self.options.output_dir, source))
            cmd = ["-e", idlcmd]
            self.head.add_job(1, self._taskid, {"dir": self.options.working_dir, "cmd": cmd, "filePath": task["args"]["filePath"], "level": 0, "workdir": task["args"]["workdir"]}, module="idl", priority=self.PRI_NORMAL)
        elif task["args"]["level"] == 1:
            # TODO: The node should perhaps verify that a number of files exist before stuff happens
            # Create a new job for the node that did the processing - move files.
            # TODO: If copying was successful, clean up some other temporary data?
            sources = []
            for fn in os.listdir(task["args"]["workdir"]):
                if fn.find(".gsar") > -1:
                    sources.append("%s/%s" % (task["args"]["workdir"], fn))
            # sourcePath = os.path.join(self.options.temp_dir, task["args"]["filePath"])
            destPath = os.path.join(self.options.output_dir, task["args"]["filePath"])
            print "Creating move job", destPath
            # for ext in [".gsar", ".gsar.json", ".gsar.xml"]:
            #    sources.append(sourcePath + ext)
            self.head.add_job(1, task["taskid"], {"dir": self.options.working_dir, "src": sources, "dst": destPath, "level": 2}, module='move', node=task["node"], priority=self.PRI_HIGH)
        elif task["args"]["level"] == 2:
            print "Copy completed"
            os.remove(task["args"]["workdir"])
            self.dir_monitor.setDone(task["args"]["filePath"])

    def onTimeout(self, task):
        print "Task timeout out, requeue"
        self.head.requeue(task)

    def onError(self, task):
        print "Error for task", task["taskid"]
        f = open("errors.txt", "a")
        f.write(str(task["args"]) + "\n")
        f.close()
        for fn in os.listdir(task["args"]["workdir"]):
            os.remove("%s/%s" % (task["args"]["workdir"], fn))

        os.removedirs(task["args"]["workdir"])
        # Notify someone, log the file as failed, try to requeue it or try to figure out what is wrong and how to fix it
        pass

    def onStepCompleted(self, step):
        print "All OK Step", step, " - waiting for more files"
        # self.head.stop()
