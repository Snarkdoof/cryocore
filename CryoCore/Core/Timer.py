"""
Provide a repeating timer. Tries to run every interval regardless of
how long the funcion takes to return, but always serial.  If interval
is shorter than the execution time, it will be run continuously.

"""
import time

import threading as threading

class RepeatingTimer(threading.Thread):
    
    def __init__(self, interval, func, stop_event = None, error_handler = None):
        """
        Interval is a float (seconds),
        func is the function to call and
        if given, the stop_event will make the timer stop
        """
        threading.Thread.__init__(self)

        self._interval = interval
        self.func = func
        self._internal_stop_event = threading.Event()
        if not stop_event:
            self.stop_event = self._internal_stop_event
        else:
            self.stop_event = stop_event
        self._error_handler = error_handler

        
        self._last_run = 0
        
    def stop(self):
        self._internal_stop_event.set()
        
    def run(self):
        """
        Loop and execute the function on time
        """
        while not self._internal_stop_event.is_set() or self.stop_event.is_set():
            
            # Should run?
            now = time.time()
            d = (self._last_run + self._interval) - now # Should run in d seconds
            if d > 0:
                time.sleep(min(1, d))
                continue
            
            self._last_run = time.time()
            
            try:
                self.func()
            except Exception as e:
                try:
                    if self._error_handler:
                        self._error_handler(e)
                    else:
                        print("[TIMER ERROR]:", e)
                except Exception as e2:
                    print("[TIMER]: Error handler failed:", e2)
                
            
