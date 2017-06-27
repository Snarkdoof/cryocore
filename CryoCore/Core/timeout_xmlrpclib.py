import xmlrpc.client
import http.client

from CryoCore.Core import API


def Server(url, timeout=None):
    """
    An XML-RPC server that can time out
    """
    t = TimeoutTransport()
    t.timeout = timeout
    t.set_default_timeout(timeout)
    t.set_timeout(timeout)

    server = xmlrpc.client.Server(url, timeout=t)
    return server


def ServerProxy(url, timeout=None):

    t = TimeoutTransport()
    t.set_default_timeout(timeout)
    t.set_timeout(timeout)

    serverProxy = xmlrpc.client.ServerProxy(url, transport=t)

    return serverProxy


class TimeoutTransport(xmlrpc.client.Transport):
    """
    A non-blocking XML-RPC transport class
    """

    default_timeout = None
    timeout = None

    def make_connection(self, host):
        # if common.get_config(Cfg["SSL"]:
        #    conn = TimeoutHTTPS(host)
        # else:
        conn = TimeoutHTTP(host)
        conn.set_timeout(self.timeout)
        return conn

    def set_default_timeout(self, timeout):
        self.default_timeout = timeout

    def set_timeout(self, timeout):
        self.timeout = timeout

    def reset_timeout(self):
        self.timeout = self.default_timeout


class TimeoutHTTPConnection(http.client.HTTPConnection):
    """
    A HTTP connection class that can time out
    """
    def connect(self):
        http.client.HTTPConnection.connect(self)

    def set_timeout(self, timeout):
        if self.sock:
            self.sock.settimeout(timeout)


class TimeoutHTTPSConnection(http.client.HTTPSConnection):
    """
    A HTTPS connection class that can time out
    """

    def __init__(self, host, port=None, strict=0):

        # TODO: Why is port part of the host?
        pos = host.rfind(":")
        if pos != -1:
            _host = host[:pos]
        cert_file = None  # http.get_cert_file(_host)

        http.client.HTTPSConnection.__init__(self, host, port=port,
                                         key_file=Cfg["SSL.KeyFile"],
                                         cert_file=cert_file)

    def connect(self):
        http.client.HTTPSConnection.connect(self)

    def set_timeout(self, timeout):
        if self.sock:
            self.sock.settimeout(timeout)


class TimeoutHTTP(http.client.HTTP):
    """
    An HTTP class that can time out...
    """
    _connection_class = TimeoutHTTPConnection

    def set_timeout(self, timeout):
        self._conn.set_timeout(timeout)

    def reset_timeout(self):
        self._conn.reset_timeout()


class TimeoutHTTPS(http.client.HTTP):
    """
    An HTTP class that can time out...
    """
    _connection_class = TimeoutHTTPSConnection

    def set_timeout(self, timeout):
        self._conn.set_timeout(timeout)

    def reset_timeout(self):
        self._conn.reset_timeout()


# TEST CODE
if __name__ == "__main__":

    kwargs = {'timeout': 10}

    s = ServerProxy("http://time.xmlrpc.com/RPC2")

    print(s.currentTime.getCurrentTime())
