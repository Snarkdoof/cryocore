"""
Some utilities for common use
"""


import time
import os
import os.path
import math


def hostname_to_camel_case(hostname):
    """
    Convert a hostname to camelcase, e.g. www.norut.no -> WwwNorutNo
    """
    return "".join(t.capitalize() for t in hostname.split("."))


def template_to_str(template, t=None):
    """
    Convert a template (file name) to a string.  Supported variables:

    $YEAR, $MONTH, $DAY, $HOUR, $Minute

    If t(ime) is not given, the current time is used
    """

    s = template

    for (a, b) in [
        ("$YEAR", "%Y"),
        ("$MONTH", "%m"),
        ("$WEEK", "%U"),
        ("$DAY", "%d"),
        ("$HOUR", "%H"),
        ("$MINUTE", "%M")
    ]:
            s = s.replace(a, b)

    if not t:
        t = time.gmtime()

    return time.strftime(s, t)


def prepare_file(filepath):
    """
    Create all necessary directories for the given file to be created
    """
    path = os.path.dirname(filepath)
    if path and not os.path.exists(path):
        os.makedirs(path)


def list_tty_devices():
    """
    Use udev to discover tty devices.  This should most likely only
    be used when detecting ID_MODEL of new equipment - thus only during
    development, not in production!
    Returns a list of udev-devices (device.keys() gives all info)
    Useful keys: ID_MODEL,
    """
    import pyudev

    context = pyudev.Context()
    devices = context.list_devices()
    retval = []
    for device in devices.match_subsystem('tty'):
        if "ID_MODEL" in list(device.keys()):
            retval.append(device)
            # for key in device:
            #    print key, device[key]
    return retval


def detect_tty_device(id_model):
    """
    Use udev to discover the wanted device.
    If the id_mode is in the form name:number the number'th entry of the list is returned.
    Empty list if too few devices are found.
    Returns a list of possible devices, an empty list if no devices
    are discovered
    """
    import pyudev

    if id_model.find(":") > -1:
        id_model, idx = id_model.split(":")
        idx = int(idx)
    else:
        idx = None

    context = pyudev.Context()
    devices = context.list_devices()
    retval = []
    for device in devices.match_subsystem('tty'):
        if "ID_MODEL" in list(device.keys()) and device["ID_MODEL"].lower() == id_model.lower():
            s = str(device["DEVLINKS"]) 
            if s.count(" ") > 0:
                retval.append(s.split(" ")[1])
            else:
                retval.append(s)

    if idx is not None:
        if idx > len(retval) - 1:
            return []
        return [retval[idx]]

    return retval


def fulldetect_tty_device(id_model):
    """
    Use udev to discover the wanted device.
    Returns a list of possible devices, an empty list if no devices
    are discovered
    """
    import pyudev

    context = pyudev.Context()
    devices = context.list_devices()
    retval = []
    for device in devices.match_subsystem('tty'):
        if "ID_MODEL" in list(device.keys()) and device["ID_MODEL"].lower() == id_model.lower():
            retval.append({"port": str(device["DEVLINKS"].split(" ")[1]),
                "device": device})
    return retval


def detect_usb_device(device_string):
    """
    Check the dmesg messages for given strings

    Returns a list of possible devices, possibly empty.
    """
    devices = {}
    import subprocess
    p = subprocess.Popen(["dmesg"], stdout=subprocess.PIPE)
    (output, nothing) = p.communicate()
    for line in output.split("\n"):
        import re
        m = re.search(".*] (.*): .*" + device_string + ".* now attached to (ttyUSB\d*)", line)
        if m:
            id, device = m.groups()
            devices[id] = device
        else:
            m = re.search(".*] (.*): .* disconnected from (ttyUSB\d*)", line)
            if m:
                id, device = m.groups()
                for id in list(devices.keys()):
                    if devices[id] == device:
                        del devices[id]

    return list(devices.values())[:]


def logTiming(func):
    """
    Time a function and log the result.
    Use by putting @logTiming on the line over the function to time
    """
    def wrapper(*arg):
        t1 = time.time()
        res = func(*arg)
        t2 = time.time()

        # log = API.get_log("Profiling")
        # log.info('%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0))
        print('%s took %0.3f ms' % (func.__name__, (t2 - t1) * 1000.0))
        return res
    return wrapper


def get_distance(source, destination):
    """
    Calculate the absolute distance in meters from source to destination,
    to be used by sqlite, expect parameters to be radians.
    Thanks to Yngvar for this custom job :)
    """
    lat0, lon0 = source
    lat1, lon1 = destination
    try:
        grad2m = 1852 * 60
        x0 = lon0 * math.cos(lat1 * (math.pi / 180)) * grad2m
        x1 = lon1 * math.cos(lat1 * (math.pi / 180)) * grad2m
        y0 = lat0 * grad2m
        y1 = lat1 * grad2m

        dist = math.sqrt(math.pow(x1 - x0, 2) + math.pow(y1 - y0, 2))
        return dist
    except Exception as e:
        print("Oops:", e)
        return 0


def set_system_time(epoc):
    import ctypes
    import ctypes.util
    librt = ctypes.CDLL(ctypes.util.find_library("rt"))

    class timespec(ctypes.Structure):
        _fields_ = [("tv_sec", ctypes.c_long),
                    ("tv_nsec", ctypes.c_long)]
    ts = timespec()
    ts.tv_sec = int(epoc)
    ts.tv_nsec = int(100000000000 * (epoc % 1))
    librt.clock_settime(0, ctypes.byref(ts))
