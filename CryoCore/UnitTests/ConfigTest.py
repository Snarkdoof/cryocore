import unittest
import time
import threading

from CryoCore import API
from CryoCore.Core import Config
from CryoCore.Core.Utils import logTiming

stop_event = threading.Event()


class ConfigTest(unittest.TestCase):
    """
    Unit tests for the Status class

    """
    def setUp(self):
        self.cfg = API.get_config("UnitTest", version="unittest")
        if self.cfg["TestBasic"]:
            print("Cleaning up old crud - bad cleanup?")
            self.cfg.remove("TestBasic")
        if self.cfg["TestName"]:
            print("Cleaning up old crud - bad cleanup?")
            self.cfg.remove("TestName")
        self.cfg.set_default("TestName", "TestNameValue")
        self.cfg.set_default("TestBasic.One", 1)
        self.cfg.set_default("TestBasic.Float", 3.14)
        self.cfg.set_default("TestBasic.True", True)

    def tearDown(self):
        API.get_config(version="unittest").remove("UnitTest")
        # API.get_config().delete_version("unittest")
        pass

    def testBasic(self):
        self.assertEqual(self.cfg.__class__, Config.NamedConfiguration, "Wrong ")
        self.cfg.require(["TestName", "TestBasic.One", "TestBasic.Float", "TestBasic.True"])
        try:
            self.cfg.require(["DoesNotExist", "TestName"])
            self.fail("Require does not throw exception when parameter is missing")
        except:
            pass

        # These should be ignored
        self.cfg.set_default("TestName", "TestNameFoo")
        self.cfg.set_default("TestBasic.One", 42)
        self.cfg.set_default("TestBasic.Float", 42.21)
        self.cfg.set_default("TestBasic.True", False)

        self.assertEqual(self.cfg["TestName"], "TestNameValue")
        self.assertEqual(self.cfg["TestBasic.One"], 1)
        self.assertEqual(self.cfg["TestBasic.Float"], 3.14)
        self.assertEqual(self.cfg["TestBasic.True"], True)

        # Ready to test some stuff
        cfg2 = API.get_config(version="unittest")
        self.assertEqual(self.cfg["TestName"], cfg2["UnitTest.TestName"])
        self.assertEqual(self.cfg["TestBasic.One"], cfg2["UnitTest.TestBasic.One"])
        self.assertEqual(self.cfg["TestBasic.Float"], cfg2["UnitTest.TestBasic.Float"])
        self.assertEqual(self.cfg["TestBasic.True"], cfg2["UnitTest.TestBasic.True"])

        children = self.cfg.get("TestBasic").children
        expected = ["One", "Float", "True"]
        for child in children:
            self.assertTrue(child.name in expected)
            expected.remove(child.name)

        # Let cache expire
        time.sleep(2.0)
        children = self.cfg.get("TestBasic").children
        expected = ["One", "Float", "True"]
        for child in children:
            self.assertTrue(child.name in expected)
            expected.remove(child.name)

        root_children = self.cfg.get(None).children
        expected = ["TestName", "TestBasic"]
        for child in root_children:
            self.assertTrue(child.name in expected)
            expected.remove(child.name)

        # leaves = self.cfg.get_leaves(recursive=False)
        # self.assertEqual(leaves, ["TestName"])

        # leaves = self.cfg.get_leaves(recursive=True)
        # expected = ["TestName", "TestBasic.One", "TestBasic.Float", "TestBasic.True"]
        # for l in expected:
        #    if l not in leaves:
        #        self.fail("get_leaves() returns bad, expected '%s' got '%s'" % (expected, leaves))

        # leaves = self.cfg.get_leaves("TestBasic", recursive=False)
        # expected = ["One", "Float", "True"]
        # for l in expected:
        #    if l not in leaves:
        #        self.fail("get_leaves() returns bad, expected '%s' got '%s'" % (expected, leaves))

    def testVersions(self):
        cfg2 = API.get_config(version="SecondTest")
        if cfg2["UnitTest"]:
            self.fail("SecondTest config has unittest")

    def testCleanup(self):
        cfg = API.get_config(version="unittest")
        cfg2 = API.get_config(version="SecondTest")

        if cfg2["UnitTest"]:
            print("Cleaning up old crud - bad cleanup?")
            cfg2.remove("UnitTest")

        cfg2.set_default("UnitTest.TestParam", "TestValue")
        self.assertEqual(cfg2["UnitTest.TestParam2"], None, "Have a parameter I wasn't expecting")

        cfg2["UnitTest.TestParam2"] = "TestValue2"
        self.assertEqual(cfg2["UnitTest.TestParam2"], "TestValue2", "Implicit create failed")
        self.assertEqual(cfg2["UnitTest.TestParam"], "TestValue", "Set default create failed")

        cfg2.remove("UnitTest.TestParam")
        self.assertEqual(cfg2["UnitTest.TestParam"], None, "Remove of subtree failed: %s" % cfg2["UnitTest.TestParam"])

        cfg2.remove("UnitTest")
        self.assertEqual(cfg2["UnitTest.TestParam2"], None, "Remove of folder failed (subelem exists)")
        try:
            cfg2.get("UnitTest")
            self.fail("Remove of folder failed")
        except:
            pass

        try:
            cfg.get("UnitTest")
        except Exception as e:
            print("EXCEPTION", e)
            self.fail("Cleaning does not respect versions")

    def testChange(self):

        last_val = {}

        def callback(param):
            last_val[param.get_full_path()] = (param.get_value(), param.comment)

        self.cfg.add_callback(["TestBasic.One", "TestBasic.Float"], callback)
        self.cfg["TestBasic.One"] = 2
        self.cfg.get("TestBasic.Float").set_value(2.3)
        self.cfg.get("TestBasic.One").set_comment("A comment")

        time.sleep(1.0)  # Allow a bit of time for async callbacks

        # Verify
        self.assertTrue("UnitTest.TestBasic.One" in last_val, "Missing change on One")
        self.assertTrue("UnitTest.TestBasic.Float" in last_val, "Missing change on Float")

        self.assertEqual(last_val["UnitTest.TestBasic.One"][0], 2)
        self.assertEqual(last_val["UnitTest.TestBasic.Float"][0], 2.3)
        self.assertEqual(last_val["UnitTest.TestBasic.One"][1], "A comment")

    def testSearch(self):

        # Search is actually a bit strange, as it always searches from the absolute root.
        expected = ["UnitTest", "UnitTest.TestBasic", "UnitTest.TestName", "UnitTest.TestBasic.One", "UnitTest.TestBasic.True"]
        elems = self.cfg.search("e")
        res = [elem.get_full_path() for elem in elems]
        res.sort()
        expected.sort()
        self.assertEqual(res, expected)
        for elem in elems:
            path = elem.get_full_path()
            self.assertTrue(path in expected, "'%s' was not expected" % path)

        try:
            self.cfg.search("'except")
            self.fail("Not escaping search strings")
        except:
            pass

    def testLookupSpeed(self):
        time.sleep(1)  # Let cache expire
        start_time = time.time()
        self.cfg["TestBasic.One"]
        first_lookup = time.time() - start_time

        # The next ones should be much, much quicker
        start_time = time.time()
        for i in range(0, 100):
            self.cfg["TestBasic.One"]
        second_lookup = time.time() - start_time
        # print(first_lookup, "vs", second_lookup)
        self.assertTrue(first_lookup > (second_lookup / 10))

if __name__ == "__main__":

    print("Testing Configuration module")

    try:
        if 0:
            import cProfile
            cProfile.run("unittest.main()")
        else:
            unittest.main()
    finally:
        # from CryoCore import API
        stop_event.set()
        API.shutdown()

    print("All done")
