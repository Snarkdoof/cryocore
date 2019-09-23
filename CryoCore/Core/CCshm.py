import sys
import errno
import os

available = False
CEventBus = None

try:
    if sys.version_info[0] >= 3:
        import CCshm_py3
        if CCshm_py3.version < 1:
        	raise Exception("CCshm version is too old - please pull & recompile")
        CEventBus = CCshm_py3.EventBus
        available = True
    else:
        import CCshm_py2
        if CCshm_py2.version < 1:
        	raise Exception("CCshm version is too old - please pull & recompile")
        CEventBus = CCshm_py2.EventBus
        available = True
except:
    import traceback
    print("Shared memory module failed to import!")
    traceback.print_exc()

def make_identity_file(name):
    path = "/tmp/cryocore/"
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    fd_path = os.path.join(path, name)
    fd = open(fd_path, "w")
    fd.close()
    return fd_path
    
    
class EventBus:
    def __init__(self, name, num_items, item_size):
        self.path = make_identity_file(name)
        if not CEventBus:
            raise Exception("Shared memory module is not available")
        self.bus = CEventBus(self.path, num_items, item_size)
    
    def post(self, msg):
        self.bus.post(msg)
    
    def get(self):
        return self.bus.get()
    
    def get_many(self):
        return self.bus.get_many()
    
