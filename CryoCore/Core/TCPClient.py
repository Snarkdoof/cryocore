import socket


class TCPClient:

    def __init__(self, address=None, timeout=None, auto_reconnect=False):
        self.sock = None
        self.address = address
        self.timeout = timeout
        self.auto_reconnect = auto_reconnect
        self.is_connected = False

    def is_connected(self):
        return self.is_connected

    def connect(self, address=None):
        if not address and not self.address:
            raise Exception("Need address to connect to")
        if not address:
            address = self.address

        for res in socket.getaddrinfo(address[0],
                int(address[1]),
                socket.AF_UNSPEC,
                socket.SOCK_STREAM, 0):

            try:
                af, socktype, proto, canonname, sa = res
                self.sock = socket.socket(af, socktype, proto)
                print("SOCKET TIMEOUT", self.timeout)
                self.sock.settimeout(self.timeout)
                self.sock.connect(sa)
                self.is_connected = True
                break
            except Exception as e:
                print("Could not connect to %s:" % str(sa), e)
                self.sock = None

        if not self.sock:
            raise Exception("Could not connect")

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        self.is_connected = False
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
                self.sock.close()
                self.sock = None
            except:
                pass

    def send(self, buffer, is_retry=False):
        if not self.sock:
            if self.auto_reconnect:
                if is_retry:
                    time.sleep(1.0)
                self.connect()
                return self.send(buffer, True)
            raise Exception("Cannot send - not connected")
        try:
            self.sock.send(buffer)
        except Exception as e:
            self.is_connected = False
            self.sock = None
            raise e

    def recv(self, bytes=None, timeout=None):
        if not self.sock:
            if self.auto_reconnect:
                self.connect()
            else:
                raise Exception("Cannot receive - not connected")

        if timeout:
            (r, w, e) = select.select([self.sock], [], [], timeout)
            if len(r) == 0:
                return None
        try:
            if bytes:
                return self.sock.recv(bytes)
            else:
                return self.sock.recv()
        except socket.timeout as e:
            raise e
        except Exception as e:
            self.is_connected = False
            self.sock = None
            raise e
