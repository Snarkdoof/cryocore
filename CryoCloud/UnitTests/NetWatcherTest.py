from __future__ import print_function

import unittest
import threading
import http.client
import time

from CryoCore import API
from CryoCloud.Common.netwatcher import *


class Listener:
    def __init__(self):
        self.added = []
        self.error = []

    def onAdd(self, info):
        self.added.append(info)
        # self.added.sort()

    def onError(self, message):
        self.error.append(message)


class PostException(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code


class Poster:
    def __init__(self, port):
        self.port = port

    def post(self, data, postJson=True):
        conn = http.client.HTTPConnection("localhost", self.port, timeout=1.0)

        if postJson:
            conn.request("POST", "/task", json.dumps(data))
        else:
            conn.request("POST", "/task", data)
        # print(dir(conn))
        response = conn.getresponse()
        if (response.code != 200):
            raise PostException("Bad return code, got %d expected 200" % response.code, response.code)
        conn.close()


class NetWatcherTest(unittest.TestCase):
    """
    Unit tests for the Directory Watcher

    """
    def setUp(self):
        self.port = 12345
        self.stop_event = threading.Event()
        self.listener = Listener()
        self.poster = Poster(self.port)
        self.nw = NetWatcher(self.port, onAdd=self.listener.onAdd,
                             onError=self.listener.onError, stop_event=self.stop_event)
        self.nw.start()

    def tearDown(self):
        self.stop_event.set()
        time.sleep(2.0)  # Let things shut down before next test

    def testBasic(self):
        # Post a task
        self.poster.post("simple string")
        time.sleep(0.1)
        self.assertEqual(["simple string"], self.listener.added)

        self.poster.post("simple string2")
        time.sleep(0.1)
        self.assertEqual(["simple string", "simple string2"], self.listener.added)

        self.poster.post("simple string3")
        time.sleep(0.1)
        self.assertEqual(["simple string", "simple string2", "simple string3"], self.listener.added)

    def testInvalid(self):
        try:
            self.poster.post("simple string", postJson=False)
            self.fail("Should get an error here")
        except PostException as e:
            # Expected error code 500
            self.assertEqual(e.code, 500)

        time.sleep(0.1)
        self.assertEqual([], self.listener.added)

        self.poster.post("simple string2")
        time.sleep(0.1)
        self.assertEqual(["simple string2"], self.listener.added)

    def testAdvancedJSON(self):
        # Post a task
        expected = []
        for item in [
            [1, 2, "3"],
            {"what": "complicated", "two": 2, "float": 0.123, "bool": True},
            {"what": "verycomplicated", "map": {"inner": "value", "int": 1}, "list": [1, 2, "3"]}
        ]:
            self.poster.post(item)
            expected.append(item)
            # expected.sort()
            time.sleep(0.1)
            self.assertEqual(self.listener.added, expected)


if __name__ == "__main__":

    print("Testing NetWatcher module")

    try:
        if 0:
            import cProfile
            cProfile.run("unittest.main()")
        else:
            unittest.main()
    finally:
        # from CryoCore import API
        API.shutdown()
        print("Shutdown")

    print("All done")
