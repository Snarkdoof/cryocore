import unittest
import time
import random
import threading

from CryoCore import API
from CryoCore.Core import InternalDB


class TestDB(InternalDB.mysql):
    def __init__(self):
        InternalDB.mysql.__init__(self, "TestDB", can_log=False, num_connections=2, min_conn_time=1)

        # Prepare a test database
        self._execute("DROP TABLE IF EXISTS __TestDB__")
        self._execute("CREATE TABLE __TestDB__ (i int)")

global results
results = []


class InternalDBTest(unittest.TestCase):
    """
    Unit tests for the Status class

    """
    def setUp(self):
        self.db = TestDB()

    def tearDown(self):
        self.db._execute("DROP TABLE IF EXISTS __TestDB__")

    # def testConnections(self):
    #    self.assertEquals(self.db.get_connection(), self.db.get_connection())

    def testInserts(self):

        for i in range(0, 100):
            self.db._execute("INSERT INTO __TestDB__ values(%s)", [i])

    def testThreading(self):
        """
        We create three threads, which are more than we have connections.
        Let them do work, sleep a bit, do more work etc, to ensure that they can all
        keep going
        """
        stop_event = threading.Event()
        self.inserted = 0

        def runpoint():
            while not stop_event.is_set() and not API.api_stop_event.is_set():
                self.db._execute("INSERT INTO __TestDB__ values(%s)", [random.randint(0, 1000000)])
                self.inserted += 1
                time.sleep(random.random() * 0.5)

        t1 = threading.Thread(target=runpoint)
        t1.start()
        t2 = threading.Thread(target=runpoint)
        t2.start()
        t3 = threading.Thread(target=runpoint)
        t3.start()
        t4 = threading.Thread(target=runpoint)
        t4.start()
        time.sleep(5)
        stop_event.set()
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        if self.inserted < 25:
            self.fail("Fewer inserts than expected")

if __name__ == "__main__":

    print("Testing internalDB module")

    try:
        unittest.main()
    finally:
        API.shutdown()

    print("All done")
