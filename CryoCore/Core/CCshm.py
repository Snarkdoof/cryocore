import sys
import errno
import os
import traceback

available = False
CEventBus = None
REQUIRED_CCHSM_VERSION = 3
try:
    if sys.version_info[0] >= 3:
        import CCshm_py3
        if CCshm_py3.version < REQUIRED_CCHSM_VERSION:
            raise Exception("CCshm version is too old - please pull & recompile")
        CEventBus = CCshm_py3.EventBus
        available = True
    else:
        import CCshm_py2
        if CCshm_py2.version < REQUIRED_CCHSM_VERSION:
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
        if e.errno == errno.EEXIST or e.errno == errno.EPERM:
            pass
        else:
            raise
    try:
        old_mask = os.umask(0o000)
        try:
            os.chmod(path, 0o777);
        except:
            #traceback.print_exc()
            #print("Failed to change ownership mask on identity path")
            pass
        os.umask(old_mask)
    except:
        #traceback.print_exc()
        print("Failed to set/reset umask")
    fd_path = os.path.join(path, name)
    try:
        fd = open(fd_path, "w")
        fd.close()
        old_mask = os.umask(0o000)
        try:
            os.chmod(fd_path, 0o777);
        except:
            pass
        os.umask(old_mask)
    except:
        # We want this to raise an exception. If we can't create the file, and we can't stat it, we can't work.
        os.stat(fd_path)
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
    
    def get_head(self):
        return self.bus.get_head()
    
