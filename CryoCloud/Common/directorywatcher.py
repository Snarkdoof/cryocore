from __future__ import print_function
import pyinotify
import threading
import CryoCore
import os
import time
try:
    import Queue
except:
    import queue as Queue

import traceback
from CryoCloud.Common import jobdb

# watched events
MASK = pyinotify.IN_CREATE
MASK |= pyinotify.IN_MOVED_TO
MASK = MASK | pyinotify.IN_MODIFY
MASK = MASK | pyinotify.IN_MOVED_FROM | pyinotify.IN_DELETE
MASK = MASK | pyinotify.IN_MOVE_SELF | pyinotify.IN_DELETE_SELF


class Dispatcher(pyinotify.ProcessEvent):

    def __init__(self, path, watcher):
        self._path = path
        self._watcher = watcher

    def onAdd(self, info):
        pass

    def onModify(self, info):
        pass

    def onRemove(self, info):
        pass

    def onError(self, info):
        pass

    def _make_info(self, event):
        info = {}
        # print(event)

        info["fullpath"] = event.pathname
        info["relpath"] = event.pathname.replace(self._path, "")
        if info["relpath"] and info["relpath"][0] == "/":
            info["relpath"] = info["relpath"][1:]
        info["isdir"] = event.dir
        info["mtime"] = 0
        try:
            if os.path.exists(info["fullpath"]):
                info["mtime"] = os.stat(info["fullpath"]).st_mtime
                event.mtime = info["mtime"]
                # info["stable"] = (time.time() - info["mtime"] > self._stabilize)
        except OSError as e:
            print("OSERROR", e)
            pass
        return info

    def process_IN_CREATE(self, event):
        # print("Monitored - file added", event)
        info = self._make_info(event)

        if self._watcher.stabilize and self._watcher.stabilize > time.time() - info["mtime"]:
            self._watcher.addUnstable(event)
            print(event.pathname, "Unstable", info["mtime"])
            return

        # File added - is it done already?
        f = self._watcher._db.get_file(self._watcher.target, event.pathname, self._watcher.runid)
        if not f:

            # Is stable (enough)
            print(event.pathname, " ******* STABLE", self._watcher.stabilize, time.time() - info["mtime"], self._watcher.stabilize < time.time() - info["mtime"])
            self._watcher._db.insert_file(self._watcher.target, event.pathname, info["mtime"], True, None, self._watcher.runid)

        else:
            if info["mtime"] > f[3]:
                # print("Modified since last stable")
                self._watcher._db.update_file(self._watcher.target, event.pathname, info["mtime"], True)
                self.onModify(info)
                return

            # We have f already - is it done?
            if f[6]:
                # print("Already done")
                # Was the file modified AFTER we flagged it done?
                if info["mtime"] > f[3]:
                    # print("Done but modified since")
                    self.onModify(info)
                return
            else:
                # Only stable files are in the DB, so if this is modified later, it's MODIFIED
                if info["mtime"] > f[3]:
                    self.onModified(info)
                # print("FOUND but not done")

        self.onAdd(info)

    def process_IN_MOVED_TO(self, event):
        self.process_IN_CREATE(event)

    def process_IN_MODIFY(self, event):
        # print("Monitored - file modified", event)
        # Check if this has been processed - if so, a modify is useful
        self.process_IN_CREATE(event)
        # self.onAdd(self._make_info(event))
        # self.onModify(self._make_info(event))

    def process_IN_MOVE_SELF(self, event):
        self.process_IN_MODIFY(event)

    def process_IN_DELETE(self, event):
        # print("Monitored - file removed", event)

        f = self._watcher._db.get_file(self._watcher.target, event.pathname, self._watcher.runid)
        if f:
            self._watcher._db.remove_file(f[0])

        self.onRemove(self._make_info(event))

    def process_IN_MOVED_FROM(self, event):
        self.process_IN_DELETE(event)

    def process_IN_DELETE_SELF(self, event):
        self.process_IN_DELETE(event)


class FakeEvent():
    def __init__(self, item, name):
        self.pathname = os.path.join(item, name)
        self.dir = os.path.isdir(self.pathname)
        self.mtime = 0


class DirectoryWatcher(threading.Thread):
    def __init__(self, runid, target, onAdd=None, onModify=None, onRemove=None, onError=None, stabilize=0, recursive=False):
        threading.Thread.__init__(self)
        self.stabilize = stabilize
        self.runid = runid
        self.target = target
        self.recursive = recursive

        self._lock = threading.Lock()
        self._unstable = {}

        wm = pyinotify.WatchManager()
        self._db = jobdb.JobDB("directorywatcher", None)

        self.monitor = Dispatcher(target, self)
        if onAdd:
            self.monitor.onAdd = onAdd
        if onModify:
            self.monitor.onModify = onModify
        if onRemove:
            self.monitor.onRemove = onRemove
        if onError:
            self.monitor.onError = onError
        self.notifier = pyinotify.Notifier(wm, self.monitor)
        self.wdd = wm.add_watch(target, MASK, rec=self.recursive, auto_add=self.recursive)

        self.daemon = True

        # Go through the directory and check existing contents
        self._check_existing(self.target)

    def _check_existing(self, path):
        l = os.listdir(path)
        for name in l:
            event = FakeEvent(path, name)
            if self.recursive and event.dir:
                self._check_existing(event.pathname)
                continue

            # We add the directory itself as well as any files
            self.addUnstable(event)

    def addUnstable(self, event):
        with self._lock:
            self._unstable[event.pathname] = event

    def markDone(self, path):
        self._db.done_file(self, self.target, path, self.runid)

    def reset(self):
        """
        Reset all files
        """
        self._db.reset_files(self, self.target, self.runid)

    def run(self):
        while not CryoCore.API.api_stop_event.isSet():
            if self.notifier.check_events(timeout=0.25):
                self.notifier.read_events()
                self.notifier.process_events()
            # Do we have any unstable files?
            try:
                q = []
                with self._lock:
                    for k in self._unstable:
                        if self.stabilize and time.time() - self._unstable[k].mtime < self.stabilize:
                            continue
                        q.append((k, self._unstable[k]))
                    for k, event in q:
                        del self._unstable[k]

                for k, event in q:
                    self.monitor.process_IN_MODIFY(event)
            except Queue.Empty:
                pass

        self.notifier.stop()


if __name__ == '__main__':

    try:
        ROOTPATH = "foo"

        import sys
        if len(sys.argv) > 1:
            ROOTPATH = sys.argv[1]

        def onAdd(filepath):
            print ("--onAdd", filepath)

        def onModify(filepath):
            print ("--onModify", filepath)

        def onRemove(filepath):
            print ("--onRemove", filepath)

        def onError(message):
            print ("--onError", message)

        # watch directory
        RUNID = 1
        dw = DirectoryWatcher(RUNID, ROOTPATH, onAdd=onAdd, onModify=onModify,
                              onRemove=onRemove, onError=onError,
                              recursive=True, stabilize=3)
        dw.start()

        #dw.reset()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print()
    finally:
        CryoCore.API.shutdown()
