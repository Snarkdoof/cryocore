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
import jobdb

# watched events
MASK = pyinotify.IN_CREATE | pyinotify.IN_MOVED_TO
MASK = MASK | pyinotify.IN_MODIFY
MASK = MASK | pyinotify.IN_MOVED_FROM | pyinotify.IN_DELETE
MASK = MASK | pyinotify.IN_MOVE_SELF | pyinotify.IN_DELETE_SELF


class WatchThread(threading.Thread, pyinotify.ProcessEvent):
    """
    Thread uses pyinotify to watch for file system events.
    Events are forwarded to DirectoryWatcher main thread on a queue.
    """

    def __init__(self, queue, rootpath, rec, stop_ev):
        threading.Thread.__init__(self)
        self._rootpath = rootpath
        self._recursive = rec
        self._queue = queue
        self._stop_ev = stop_ev

    # PyInotify Callbacks
    def process_IN_CREATE(self, event):
        if not event.dir:
            self._queue.put({"type": "onAdd", "arg": {"path": event.path, "filename": event.name}})

    def process_IN_MOVED_TO(self, event):
        self.process_IN_CREATE(event)

    def process_IN_MODIFY(self, event):
        if not event.dir:
            self._queue.put({"type": "onModify", "arg": {"path": event.path, "filename": event.name}})

    def process_IN_DELETE(self, event):
        if not event.dir:
            self._queue.put({"type": "onRemove", "arg": {"path": event.path, "filename": event.name}})

    def process_IN_MOVED_FROM(self, event):
        self.process_IN_DELETE(event)

    def process_IN_DELETE_SELF(self, event):
        self._queue.put({"type": "onError", "arg": "watchdir {} deleted or removed".format(self._rootpath)})

    def process_IN_MOVE_SELF(self, event):
        self.process_IN_DELETE_SELF(event)

    def run(self):
        self._wm = pyinotify.WatchManager()
        self._notifyer = pyinotify.Notifier(self._wm, self)
        self._wdd = self._wm.add_watch(self._rootpath, MASK, rec=self._recursive, auto_add=True)
        while not self._stop_ev.isSet():
            if self._notifyer.check_events(timeout=1.0):
                self._notifyer.read_events()
                self._notifyer.process_events()
        self._notifyer.stop()


class DirectoryWatcher(threading.Thread):

    """
    Wathing a directory for events concerning individual filepaths
    - [onAdd] - file is created
    - [onModify] - file is modified
    - [onRemove] - file is deleted

    Stabilization
    Events [onAdd] and [onModify] mare not reported until after the file have become stable.
    Event onRemove is reported immediately

    - [stabilize] specifies number of seconds with inactivity required for a file to become stable.
    If stabilize == 0 implies immediately stable.
    Default value 5 sec.
    A file that is modified becomes unstable (unless stabilize == 0)
    A file that is continuously modified may never become stable.

    List operation lists watched filepaths that are both stable and public.
    - public means that is has been presented with an onAdd event
    - due to stabilization, watched filepaths will be non-public until the first onAdd event
    - due to stabilization, public filepaths may become unstable after modification. If so, they still remain public
    """

    def __init__(self, runid, rootpath, jobDB=None,
                 onAdd=None, onModify=None, onRemove=None, onError=None,
                 stabilize=5, recursive=True, liveonly=False, recall=False):
        threading.Thread.__init__(self)
        if not os.path.isdir(rootpath):
            raise Exception("Directory '%s' does not exist or is not a directory" % rootpath)

        self._runid = runid
        self._rootpath = os.path.abspath(rootpath)  # path to watch
        self._recursive = recursive  # watch recursively into subdirectories
        self._stabilize = stabilize  # time to wait for files to stabilize
        self._period = 2
        self._liveonly = liveonly
        self._recall = recall
        # callback handlers
        self.onAdd = onAdd
        self.onModify = onModify
        self.onRemove = onRemove
        self.onError = onError
        # state
        self._db = jobDB if jobDB is not None else jobdb.JobDB("directorywatcher", None)
        # queue so that ipynotify never blocks on app code.
        self._queue = Queue.Queue()
        # threads
        # self._periodicThread = PeriodicThread(self._queue, 2, CryoCore.API.api_stop_event)
        self._watchThread = WatchThread(self._queue, self._rootpath, self._recursive, CryoCore.API.api_stop_event)

    def _get_path_info(self, arg):
        """
        utility method for making sure paths are relative to rootpath
        """
        filepath = os.path.join(arg['path'], arg['filename'])
        fullpath = os.path.join(arg['path'], arg['filename'])
        relpath = os.path.relpath(filepath, self._rootpath)
        info = {
            "fullpath": fullpath,
            "relpath": relpath,
        }
        try:
            info["mtime"] = os.stat(fullpath).st_mtime
            info["stable"] = (time.time() - info["mtime"] > self._stabilize)
        except OSError:
            pass
        return info

    def _dispatch_events(self, events):
        """Dispatch events"""

        for eType, info in events:
            if eType == "onAdd" and self.onAdd:
                self.onAdd(info)
            elif eType == "onModify" and self.onModify:
                self.onModify(info)
            elif eType == "onRemove" and self.onRemove:
                self.onRemove(info)

    def _update_db(self, info, row):
        """Update database by inserting updating or removing rows"""
        if row and info is None:
            self._db.remove_file(row[0])
            return True
        elif info and row is None:
            public = info["stable"]
            self._db.insert_file(self._rootpath, info["relpath"],
                                 info["mtime"], info["stable"],
                                 public, self._runid)
            return True
        elif info and row:
            # run update in case fs has been modified
            was_public = row[5]
            info["public"] = was_public or info["stable"]
            diff = (row[3] != info["mtime"] or row[4] != info["stable"] or row[5] != info["public"])
            if diff:
                self._db.update_file(row[0], info["mtime"], info["stable"], info["public"])
            return diff
        else:
            return False

    def _generate_event(self, info, row, diff=False, live=True):
        """given fs info and row from db, generate 0 or 1 event of the correct type"""
        # - ignore files that are already done
        if row and row[6]:
            return []
        if info:
            # insert or modify
            # - ignore files that are not stable
            if not info["stable"]:
                return []
            # - recall or not
            elif not self._recall and not live:
                # onInit - no recall
                return [("onAdd", info)]
            else:
                was_public = row[5]
                if info["stable"]:
                    if was_public:
                        if diff:
                            # onModify
                            return [("onModify", info)]
                    else:
                        # onAdd
                        return [("onAdd", info)]
        else:
            # remove
            # - ignore event if row was not public
            if not row[5]:
                print ("dropped - not stable")
                return []

            # always events for live
            if live or (self._recall and not live):
                return [("onRemove", {"relpath": row[2], "rootpath": row[3]})]
        return []

    def _onInit(self, arg):
        # file info from filesystem
        idx_fs = {}
        for dirpath, dirnames, files in os.walk(self._rootpath):
            for filename in files:
                arg = {"path": dirpath, "filename": filename}
                info = self._get_path_info(arg)
                idx_fs[info["relpath"]] = info
            if not self._recursive:
                break

        # file rows from db
        idx_db = {}
        for row in self._db.get_directory(self._rootpath, self._runid):
            relpath = row[2]
            idx_db[relpath] = row

        # events
        events = []

        # ADDED OR MODIFIED FILES
        for relpath, info in idx_fs.items():
            # sync db to fs
            row = idx_db.get(relpath, None)
            diff = self._update_db(info, row)
            events += self._generate_event(info, row, diff=diff, live=False)

        # REMOVED FILES
        for relpath, row in idx_db.items():
            # sync db to fs
            info = idx_fs.get(relpath, None)
            if info is None:
                diff = self._update_db(None, row)
                events += self._generate_event(None, row, diff=diff, live=False)

        # dispatch events
        # - liveonly means that no events are dispatched from onInit
        if not self._liveonly:
            self._dispatch_events(events)

    def _onAdd(self, arg):
        """
        live file create event detected
        """
        info = self._get_path_info(arg)
        row = self._db.get_file(self._rootpath, info["relpath"], self._runid)
        if row is None:
            diff = self._update_db(info, None)
            self._dispatch_events(self._generate_event(info, None, diff=diff))
        else:
            print("warning: create file event, but file is already in database")

    def _onModify(self, arg):
        """
        live file modification event detected
        """
        info = self._get_path_info(arg)
        row = self._db.get_file(self._rootpath, info["relpath"], self._runid)
        if row is not None:
            diff = self._update_db(info, row)
            self._dispatch_events(self._generate_event(info, row, diff=diff))
        else:
            print("warning: modify file event, but file is not in database")

    def _onRemove(self, arg):
        """
        filepath removed
        clean up immediately
        """
        info = self._get_path_info(arg)
        row = self._db.get_file(self._rootpath, info["relpath"], self._runid)
        if row is not None:
            diff = self._update_db(None, row)
            self._dispatch_events(self._generate_event(None, row, diff=diff))
        else:
            print("warning: remove file event, but file is not in database")

    def _onTimeout(self, arg):
        """
        periodic timeout
        check if any of the unstable filepaths
        have become stable
        """
        for row in self._db.get_directory(self._rootpath, self._runid):
            was_stable = row[4]
            if not was_stable:
                arg = {'path': self._rootpath, 'filename': row[2]}
                info = self._get_path_info(arg)
                diff = self._update_db(info, row)
                self._dispatch_events(self._generate_event(info, row, diff=diff))

    def _onDone(self, arg):
        self._db.done_file(self._rootpath, arg["relpath"], self._runid)

    def _onReset(self, arg):
        self._db.reset_files(self._rootpath, self._runid)
        for row in self._db.get_directory(self._rootpath, self._runid):
            pass

    def _onError(self, message):
        """
        error
        """
        if self.onError:
            self.onError(message)

    def list(self):
        """
        list all filepaths that are watched
        - include only those that both stable and public
        """
        rowList = self._db.get_directory(self._rootpath, self._runid)
        return [row[2] for row in rowList if row[4] == True and row[5] == True]

    def setDone(self, relpath):
        """
        one file completed
        """
        self._queue.put({"type": "onDone", "arg": {relpath: relpath}})

    def reset(self):
        """
        all files to be reset
        """
        self._queue.put({"type": "onReset", "arg": None})

    def run(self):
        """
        main loop
        read messages from queue from periodic thread and inotify watch thread.
        """
        self._watchThread.start()

        # initialise
        self._queue.put({"type": "onInit", "arg": None})

        # listen for messages on queue
        ts = time.time()
        while not CryoCore.API.api_stop_event.isSet():
            try:
                msg = self._queue.get(True, 1)
                getattr(self, "_" + msg["type"])(msg.get("arg", None))
            except Queue.Empty:
                if time.time() - ts > self._period:
                    self._queue.put({"type": "onTimeout"})
                    ts = time.time()
                continue
            except AttributeError:
                traceback.print_exc()
                continue

        self._watchThread.join()


if __name__ == '__main__':

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
                          recursive=True, stabilize=3, liveonly=False, recall=False)
    dw.start()

    try:
        #dw.reset()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print()
    finally:
        CryoCore.API.shutdown()
