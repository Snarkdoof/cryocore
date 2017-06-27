
"""
A small token bucket implementation to do rate limitation
"""
import threading as threading
import time

class TokenBucket:
    """
    Thread-safe token bucket
    """
    def __init__(self, capacity, fill_rate):
        """
        capacity = Max tokens
        fill_rate = new tokens/second
        """
        self._lock = threading.RLock()
        self.capacity = capacity
        self.fill_rate = float(fill_rate)
        self._tokens = capacity
        
    def consume(self, num_tokens, block=False):
        """
        Returns True iff there was enough tokens.
        If block, the function will block until it the number of tokens are present.
        Note that it will spin while blocking, sleeping a bit, so it is not
        particularly efficient!
        """
        while True:
            with self._lock:
                if self._get_tokens() >= num_tokens:
                    self._tokens -= num_tokens
                    return True
                if not block:
                    return False
            time.sleep(0.2)

    def num_tokens_available(self):
        with self._lock:
            return self._get_tokens()

    def _get_tokens(self):
        now = time.time()
        if self._tokens < self.capacity:
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
        self.timestamp = now
        return self._tokens
    
    def set_fill_rate(self, fill_rate):
        with self.lock:
            self.fill_rate = float(fill_rate)
        
    def set_capacity(self, capacity):
        with self.lock:
            self.capacity = capacity
