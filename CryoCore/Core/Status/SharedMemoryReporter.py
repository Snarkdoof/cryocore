import time

from CryoCore.Core import Status
from CryoCore.Core import CCshm
import json

available = CCshm.available

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
        element.add_callback(self.report)
    
    def can_report(self):
        return self.bus != None
    
    def report(self, event):
        if not self.bus:
            return
        data = json.dumps({ "channel" : event.status_holder.get_name(),
                            "ts" : event.get_timestamp(),
                            "name" : event.get_name(),
                            "value" : event.get_value()})
        self.bus.post(data)
