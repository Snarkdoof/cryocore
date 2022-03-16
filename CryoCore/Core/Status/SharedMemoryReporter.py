import time

from CryoCore.Core import Status
from CryoCore.Core import CCshm
import json

available = CCshm.available

class SimpleEvent:
    def __init__(self, channel, ts, name, value):


        class FakeStatusHolder:
            def __init__(self, name):
                self.name = name

            def get_name(self):
                return self.name

        self.status_holder = FakeStatusHolder(channel)
        self.ts = ts
        self.name = name
        self.value = value

    def get_timestamp(self):
        return self.ts

    def get_name(self):
        return self.name

    def get_value(self):
        return self.value


class SharedMemoryReporter(Status.OnChangeStatusReporter):
    def __init__(self, name="System.Status.SharedMemoryReporter"):
        Status.OnChangeStatusReporter.__init__(self, name)
        try:
            self.bus = CCshm.EventBus("CryoCore.API.Status", 0, 1024 * 256)
        except:
            print("Warning: Shared memory module not compiled, using polling")
            self.bus = None
    
    def add_element(self, element):
        # We override add_element to get full-throttle change callbacks.
        element.add_immediate_callback(self.report)
    
    def can_report(self):
        return self.bus != None
    
    def report(self, event):
        if not self.bus:
            return
        val = event.get_value()
        params = (event.status_holder.get_name(), event.get_timestamp(), event.get_name(), val)
        if isinstance(val, int):
            data = """{"channel":"%s","ts":%.8f,"name":"%s","value":%d}""" % params
        elif isinstance(val, float):
            data = """{"channel":"%s","ts":%.8f,"name":"%s","value":%.8f}""" % params
        else:
            data = """{"channel":"%s","ts":%.8f,"name":"%s","value":"%s"}""" % params
        
        #data = json.dumps({ "channel" : event.status_holder.get_name(),
        #                    "ts" : event.get_timestamp(),
        #                    "name" : event.get_name(),
        #                    "value" : event.get_value()})
        self.bus.post(data)
