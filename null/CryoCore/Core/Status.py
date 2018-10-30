from __future__ import print_function

import time
import sys


class StatusObject:
    def __init__(self, root, name, value):
        self.root = root
        self.name = name
        self.value = value
        self._last_reported_num_left = 0
        self._last_reported_ts = 0
        self._limit_num_changes = 0
        self._limit_cooldown = 0

    def inc(self, amount=1):
        self.value += amount

    def dec(self, amount=1):
        self.value -= amount

    def __str__(self):
        return self.name + "=" + self.value

    def downsample(self, num_changes=None, cooldown=None):
        self._limit_num_changes = num_changes
        self._limit_cooldown = cooldown

    def _report(self):
        if self._last_reported_num_left > 0:
            self._last_reported_num_left -= 1
            return
        else:
            self._last_reported_num_left = self._limit_num_changes

        if self._limit_cooldown:
            if time.time() - self._last_reported_ts < self._limit_cooldown:
                return
            else:
                self._last_reported_ts = time.time()

        # Use a bit of color to make it PRETTY! :D
        def colored(text, color):
            colors = {"green": "\033[92m",
                      "red": "\033[91m",
                      "yellow": "\033[93m",
                      "blue": "\033[94m",
                      "gray": "\033[90m",
                      "black": "\33[98m"}
            return colors[color] + text + "\033[0m"

        print("%s [%s] % 10s: " % (colored(time.ctime(), "yellow"), colored(self.root, "red"), colored(self.name, "blue")), self.value)
        sys.stdout.flush()


class Status:
    def __init__(self, root):
        self.root = root
        self._values = {}

    def __setitem__(self, key, value):
        if key not in self._values:
            self._values[key] = StatusObject(self.root, key, value)
        else:
            self._values[key].value = value
        self._values[key]._report()

    def __getitem__(self, key):
        return self._values[key]
