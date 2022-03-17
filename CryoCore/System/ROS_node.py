
import threading

import rospy
import std_msgs.msg as msg

from CryoCore import API
from CryoCore.Core.Status import StatusListener


class ROS_inputnode:
   """
   Provides ROS integration with CryoCore. Can log or store status info based
   on config
   """

   def __init__(self, name="ROS"):
      self.name = name

      self.cfg = API.get_config(self.name)
      self.status = API.get_status(self.name)
      self.log = API.get_log(self.name)

      self.run()

   def run(self):

      print("Starting inputnode")
      def callback_log(data, args):
         line = "[%s] %s"  % (args["source"], data)
         lvl = args["loglevel"]
         if lvl == "debug":
            self.log.debug(line)
         elif lvl == "info":
            self.log.info(line)
         elif lvl == "warning":
            self.log.warning(line)
         elif lvl == "error":
            self.log.error(line)
         elif lvl == "fatal":
            self.log.fatal(line)
         else:
            self.log.warning("BAD LOG LEVEL '%s'" % lvl)
            self.log.warning(line)

      def callback_status(data, source):
         self.status[source] = data.data

      def callback_chaindb(data, source):
         print("DB data", data)

      print("Rospy initializing rospy")
      # rospy.init_node(self.name, disable_signals=True)

      print("Subscribing")

      # Subscribe
      if self.cfg.get("topic") is None:
         print("No topics")
         self.log.error("Missing 'topic' for subscriptions")
      else:
         for c in self.cfg.get("topic").get_children():
            print("Subscribing to %s %s" % (c.name, c.value))
            self.log.debug("Subscribing to %s %s" % (c.name, c.value))
            if c.value.startswith("log"):
               if c.value.find(",") > -1:
                  lvl = c.value[c.value.find(",")+1:].lower()
               else:
                  lvl = "info"
               rospy.Subscriber(c.name, msg.String, callback_log, {"source": c.name, "loglevel": lvl})
            if c.value == "status":
               rospy.Subscriber(c.name, msg.String, callback_status, c.name)
            if c.value == "db":
               rospy.Subscriber(c.name, msg.String, callback_chaindb, c.name)


class ROS_outputnode(threading.Thread):

   def __init__(self, name="ROS_out"):
      threading.Thread.__init__(self)
      self.name = name

      self.topics = {}
      self.cfg = API.get_config(self.name)
      self.status = API.get_status(self.name)
      self.log = API.get_log(self.name)

      try:
         if self.cfg.get("topic") is None:
            print("No output topics")
            self.log.error("Missing 'topic' for out node %s" % self.name)
         else:
            self.start()
      except:
            print("No topics")
            self.log.error("Missing 'topic' for out node %s" % self.name)         

   def run(self):
      self.statuslistener = StatusListener.get_status_listener()

      self.monitors = {}
      for c in self.cfg.get("topic").get_children():
         chan = self.cfg["topic.%s.chan" % c.name]
         param = self.cfg["topic.%s.param" % c.name]
         options = self.cfg["topic.%s.options" % c.name]

         print("Publishing [%s] %s to %s with options %s" % (chan, param, c.name, options))
         monitors[(chan, param)] = {"target": c.name, "options": options}

      self.statuslistener.add_monitors(list(self.monitors))


      # We now must wait to get stuff
      while not API.api_stop_event.is_set():
         self.statuslistener.wait(1)

         updates = self.statuslistener.get_last_values()

         for key in updates:
            opts = monitors[key]
            self.publish(opts["target"], updates[key])


   def publish(self, topic, value, type=msg.String, queue_size=10):

      if (topic, type) not in self.topics:
         self.topics[(topic, type)] = rospy.Publisher(topic, type, queue_size=queue_size)

      self.topics[(topic, type)].publish(value)




if __name__ == "__main__":
   # TEST

   rospy.init_node("ROS", disable_signals=True)

   cfg = API.get_config("ROS")
   cfg.set_default("topic.ros_status", "status")
   cfg.set_default("topic.ros_log", "log")
   cfg.set_default("topic.ros_db", "db")
   print("Creating inputnode")
   innode = ROS_inputnode()

   print("Creating outputnode")
   outnode = ROS_outputnode("ROS_SOURCE")

   import time
   try:
      i = 0
      while not API.api_stop_event.is_set():
         # outnode.publish("ros_status", "Status_%d" % i)
         # outnode.publish("ros_log", "Log_%d" % i)
         # outnode.publish("ros_db", "DB_%d" % i)
         i += 1
         try:
            time.sleep(1)
         except:
            print("Stopping")
            API.shutdown()

   finally:
      API.shutdown()
