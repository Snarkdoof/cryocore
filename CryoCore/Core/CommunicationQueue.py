
from . import Queue


PRIO_ANY = 0
PRIO_HIGH = 1
PRIO_MED = 2
PRIO_LOW = 3
PRIO_BULKDATA = 4

Priorities = [PRIO_HIGH, PRIO_MED, PRIO_LOW, PRIO_BULKDATA]
PriorityString = {PRIO_ANY: "any", PRIO_HIGH: "high", PRIO_MED: "medium",
                  PRIO_LOW: "low", PRIO_BULKDATA: "bulk data"}


class CommunicationQueue:
    """
    The communcation queue is a threadsafe, multi-priority queue to hold
    messages from one or more providers and send it to one receiver.
    """

    def __init__(self, logger):
        """
        Logs will be sent to the given logger
        """
        self.log = logger

        # Thread safe queues
        self.queue = {}
        for priority in Priorities:
            self.queue[priority] = Queue.Queue()

    def postMessage(self, priority, message, timeout=None):
        """
        Add a message with the given priority.
        This funciton will block until there is room on the queue if
        no timeout is given.  If the method times out, a timeout
        exception is thrown.
        """
        assert priority in Priorities

        # This will block if the queue is full - should we rather use a timeout?
        self.queue[priority].put(message, timeout=timeout)

    def getMessage(self, priority=PRIO_ANY, timeout=None):
        """
        Return a message, possibly limited by prioirty.  If any
        priority is accepted, the highest priority with messages left
        will be returned.  Returns None if no message was present
        within the given time.

        If timeout is None or priority is ANY, it returns immediately.
        """

        if priority == PRIO_ANY:
            for priority in Priorities:
                try:
                    return self.queue[priority].get(block=False)
                except:
                    # No message
                    self.log.exception("No message of priority %s" % priority)
        else:
            try:
                if timeout:
                    return self.queue[priority].get(timeout=timeout)
                else:
                    return self.queue[priority].get(block=False)
            except:
                self.log.debug("No message of level %s" % priority)

        return None
