from CryoCore import API

import http.server
import socketserver
import threading
import queue
import json


class MyWebServer(socketserver.TCPServer):
    """
    Non-blocking, multi-threaded IPv6 enabled web server
    """
    allow_reuse_address = True


class RequestHandler(http.server.BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        try:
            API.get_log("NetWatcher").info(format % args)
        except:
            print("Failed to log", format, args)

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write("OK".encode("utf-8"))

    def do_POST(self):

        try:
            data = self.rfile.read(int(self.headers["Content-Length"]))
            if len(data) == 0:
                return self.send_error(500, "Missing body")
            info = json.loads(data.decode("utf-8"))
        except:
            self.server.inQueue.put(("error", "Bad JSON"))
            self.server.log.exception("Getting JSON post")
            return self.send_error(500, "Bad JSON")

        self.server.inQueue.put(("add", info))
        self.send_response(200)
        self.end_headers()
        self.flush_headers()


class NetWatcher(threading.Thread):
    def __init__(self, port=None, onAdd=None, onError=None, stop_event=None):
        threading.Thread.__init__(self)
        if stop_event:
            self._stop_event = stop_event
        else:
            self._stop_event = threading.Event()
        self.log = API.get_log("NetWatcher")

        if onAdd:
            self.onAdd = onAdd
        if onError:
            self.onError = onError

        self.handler = RequestHandler
        self.handler.server = self

        self.server = MyWebServer(("", port), self.handler)
        # self.server = socketserver.TCPServer(("", port), self.handler)
        self.server.timeout = 1.0
        self.server.log = self.log
        self.server.inQueue = queue.Queue()

        t = threading.Thread(target=self._handle_requests)
        t.start()
        self._handlethread = t

    def _handle_requests(self):
        while not self._stop_event.isSet() and not API.api_stop_event.isSet():
            try:
                self.server.socket.settimeout(1.0)
                self.server.handle_request()
            except:
                time.sleep(0.1)  # No request
                pass
        try:
            self.server.socket.close()
        except:
            pass

    def stop(self):
        self._stop_event.set()

    def add_callback(self, func):
        self.callbacks.append(func)

    def remove_callback(self, func):
        if func in self.callbacks:
            self.callbacks.remove(func)
        else:
            raise Exception("Callback not registered")

    def onAdd(self, info):
        raise Exception("onAdd not implemented")

    def onError(self, message):
        pass

    def run(self):
        while not self._stop_event.isSet() and not API.api_stop_event.isSet():
            try:
                what, info = self.server.inQueue.get(block=True, timeout=1.0)
                try:
                    if what == "add":
                        self.onAdd(info)
                    elif what == "error":
                        self.onError(info)
                except:
                    self.log.exception("Exception in callback")
            except queue.Empty:
                continue

if __name__ == "__main__":
    try:
        nw = NetWatcher(12345)
        nw.start()
        import time
        while True:
            time.sleep(10)
    finally:
        API.shutdown()
