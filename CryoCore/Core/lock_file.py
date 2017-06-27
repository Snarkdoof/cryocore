"""
Small "hack" to limit auto-detect while I rewrite a system-wide
auto detect process
"""

import os


class AlreadyLockedException(Exception):
    pass


def lock(device):
    """
    Try to lock a device.  If it is already locked,
    AlreadyLockedException is thrown.
    If successful, the lock is yours!
    """

    if not os.path.exists("locks"):
        os.mkdir("locks")
        os.chmod("locks", 0o777)

    # Remove leading directories
    file_name = device[device.rfind("/") + 1:]
    file_name = file_name.encode("string_escape").replace(" ", "_")

    p = os.path.join("./locks/", file_name)
    if os.path.exists(p):
        raise AlreadyLockedException()
    pid = str(os.getpid())
    f = open(p, "a")
    f.write(pid)
    f.close()

    # Check if we got it
    os.chmod(p, 0o777)

    f = open(p, "r")
    if f.read() != pid:
        raise AlreadyLockedException()


def unlock(device):
    """
    Unlock the lock. This does not check if you actually had the lock!
    """
    file_name = device[device.rfind("/") + 1:]
    file_name = file_name.encode("string_escape").replace(" ", "_")

    p = os.path.join("./locks/", file_name)
    if os.path.exists(p):
        os.remove(p)


def unlock_all():
    """
    Erase all locks
    """
    if os.path.exists("locks/"):
        files = os.listdir("locks/")
        for f in files:
            unlock(f)
