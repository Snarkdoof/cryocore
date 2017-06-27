
from threading import Condition


class Full(Exception):
    """
    Exception raised by Queue.put(block=0)/put_nowait().
    """
    pass


class Empty(Exception):
    """
    Exception raised by Queue.get(block=0)/get_nowait().
    """
    pass


class NoMatch(Exception):
    pass


class Queue:
    """
    The Python Queue does not allow me to peek into it, so here is one that
    does.  It is also threadsafe and can be used just like a python Queue
    """

    def __init__(self, maxsize=0):

        self.items = []
        self.max_size = maxsize
        self.lock = Condition()

    def put(self, obj, block=True, timeout=None):
        with self.lock:
            looped = False
            while True:
                if self.max_size and len(self.items) >= self.max_size:
                    if not block or looped:
                        raise Full()
                    self.lock.wait(timeout)
                    looped = True
                    continue
                self.items.append(obj)
                self.lock.notify()
                return

    def empty(self):
        with self.lock:
            return len(self.items) == 0

    def get(self, block=True, timeout=None):
        """
        Get the object
        """
        with self.lock:
            looped = False
            while True:
                if len(self.items) == 0:
                    if not block or looped:
                        raise Empty()
                    self.lock.wait(timeout)
                    looped = True
                    continue
                obj = self.items.pop(0)
                self.lock.notify()
                return obj

    def get_on_content(self, block=True, timeout=None, func=None):
        """
        The first object in the list that triggers 'func' to return
        True will be removed and returned from the queue
        """
        if not func:
            return self.get(block, timeout)

        with self.lock:
            looped = False
            while True:
                if len(self.items) == 0:
                    if not block or looped:
                        raise Empty()
                    self.lock.wait(timeout)
                    looped = True
                    continue
                for item in self.items:
                    if func(item):
                        self.items.remove(item)
                        return item
                if not block:
                    raise NoMatch()

                self.lock.wait(timeout)  # No match, wait for changes to the queue
