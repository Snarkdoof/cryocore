"""
The remote status object is a wrapper to access remote status
objects. It works between processes and multiple computers and
will only transfer data when needed.

It supports a very similar interface as the normal status object.

"""

import threading as threading

from CryoCore.Core import Status

import time
import socket


class Connector:
    """
    The connector creates a connection to a remote
    status object. It will automatically connect and
    reconnect, but will throw exceptions if reconnection is not
    possible
    """
    def __init__(self, addr, default_timeout=2.0,
                 max_reconnects=10):
        self.addr = addr
        self.default_timeout = default_timeout
        self.connected = False

        self.max_reconnects = max_reconnects
        self._reconnects = 0

    def _connect(self):
        for res in socket.getaddrinfo(self.addr[0],
                                      int(self.addr[1]),
                                      socket.AF_UNSPEC,
                                      socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            self.socket = socket.socket(af, socktype, proto)
            self.socket.settimeout(self.default_timeout)
            # self.socket.setblocking(False)
            try:
                self.socket.connect(sa)
                print("Connected to remote status at", self.addr)
                self.connected = True
                return self.socket
            except:
                pass

        raise Exception("Could not connect to %s" % str(self.addr))

    def recv(self, bytes, timeout=None):
        error = None
        for i in range(0, self.max_reconnects):
            try:
                if not self.connected:
                    last_op = "connecting"
                    self._connect()
                if timeout:
                    self.socket.settimeout(timeout)
                else:
                    self.socket.settimeout(self.default_timeout)

                last_op = "receiving"
                return self.socket.recv(bytes)
            except socket.timeout as e:
                return ""
            except Exception as e:
                error = str(e)
                # Lost connection, try to reconnect
                print("Lost connection %s, retrying (%s)" % (last_op, str(e)))
                self.close()

        raise Exception("Network connectivity issues receiving from %s: %s" %
                        (self.addr, error))

    def send(self, buffer, timeout=None):
        error = None
        for i in range(0, self.max_reconnects):
            try:
                if not self.connected:
                    last_op = "connecting"
                    self._connect()

                if timeout:
                    self.socket.settimeout(timeout)
                else:
                    self.socket.settimeout(self.default_timeout)

                last_op = "sending"
                return self.socket.send(buffer)
            except Exception as e:
                error = str(e)
                # Lost connection, try to reconnect
                print("Lost connection %s, retrying (%s)" % (last_op, str(e)))
                self.close()
        raise Exception("Network connectivity issues sending to %s: %s" %
                        (self.addr, error))

    def close(self):
        try:
            self.connected = False
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except:
            pass


class RemoteStatusHolder(Status.StatusHolder, threading.Thread):

    def __init__(self, name, address, stop_event=None):
        """
        Create a remote status holder for the given address.
        Note that clients should not use this function directly
        but rather request the remote status of a module from
        the status service which will instantiate this object.
        """
        threading.Thread.__init__(self)
        if not stop_event:
            self.stop_event = threading.Event()
        else:
            self.stop_event = stop_event
        Status.StatusHolder.__init__(self, name, self.stop_event)

        self.conn = Connector(address)
        # Lock access to the connector
        self.conn_lock = threading.Lock()

        self.start()

    def stop(self):
        self.stop_event.set()

    def get_or_create_status_element(self, key):
        """
        Overload the get function to get a remote status element.
        This function is not recommended used over shaky networks.
        It will subscribe to all changes, and might therefore take
        quite a bit of network resources too.

        It will block until the status element is available, so
        only use this if you are absolutely certain that it
        does!
        """
        self._subscribe(key)

        # Block until the status element is available
        while key not in self.elements and not self.stop_event.is_set():
            time.sleep(0.1)

        return self.elements[key]

    def _subscribe(self, _key, _type="onchange", interval=""):
        """
        Subscribe to updates of a given key.
        type can be "once", "onchange" or "periodic".
        "periodic" also needs an interval
        """
        msg = ["SUBSCRIBE", _key, _type, str(interval)]
        self.conn.send("|".join(msg) + "\n")

    def _unsubscribe(self, element):
        """
        Stop subscribing to updates.  Typically called by
        RemoteElement.__del__
        """
        msg = ["UNSUBSCRIBE", key]
        self.conn.send("|".join(msg) + "\n")

    def get_remote(self, key, type="onchange", interval="", timeout=None):
        """
        Get access to a remote status element.
        Subscribe to updates of a given key.
        type can be "once", "onchange" or "periodic".
        "periodic" also needs an interval.

        Blocks until the status element is available or until timeout
        seconds have expired. If None, block for ever
        """
        if timeout:
            end_by = time.time() + timeout

        self._subscribe(key, type, interval)

        while key not in self.elements and not self.stop_event.is_set():
            if timeout and time.time() > end_by:
                raise NoSuchElementException("Status element %s not available" % key)
            time.sleep(0.1)

        return self.elements[key]

    def list_status_elements(self):
        """
        Return a list of all known status elements. This call is
        not particularly efficient, so it should be used as seldom
        as possible.
        """
        elements = []
        self.conn_lock.acquire()
        try:
            self.conn.send("LIST\n")
            buf = ""
            while not self.stop_event.is_set():
                buf += self.conn.recv(1024)
                if not buf:
                    break
                lines = buf.split("\n")
                for line in lines:
                    if not line.strip():
                        # Empty line - we're done
                        continue
                    elem = self.deserialize(line)
                    if elem:
                        elements.append(elem.get_name())

                buf = ""
                if len(lines) > 0 and len(lines[-1]) > 0:
                    if lines[-1][-1] != "\n":  # Incomplete last line
                        buf = lines[-1]
        finally:
            self.conn_lock.release()
        return elements

    def stop(self):
        self.stop_event.set()

    def run(self):

        while not self.stop_event.is_set():

            self.conn_lock.acquire()
            try:
                buf = self.conn.recv(10240, timeout=0.5)
                if not buf:
                    continue

                for line in buf.split("\n"):
                    if not line.strip():
                        continue
                    elem = self.deserialize(line)
                    self._add_element(elem)
            # except:
            #    self.log.exception("Ignoring exception in main-loop")

            finally:
                self.conn_lock.release()
                time.sleep(0.1)  # Let someone else get the lock
        self.conn.close()
