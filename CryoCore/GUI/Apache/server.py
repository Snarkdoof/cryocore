import http.server
import select
import JSON
import io
import gzip
import urllib
import urllib
import inspect
import mimetypes
import os
import json

from CryoCore import API

# We also allow shared memory status listener if possible
try:
    from CryoCore.Core.Status.StatusListener import StatusListener
    use_status_listener = True 
    import threading
except:
    use_status_listener = False 

functions = {o[0]: o[1] for o in inspect.getmembers(JSON) if inspect.isfunction(o[1])}

from SimpleWebSocketServer import SimpleWebSocketServer, WebSocket



class MyHandler(http.server.SimpleHTTPRequestHandler):
    server_version = "UAV Onboard/0.1"

    def log_message(self, format, *args):
        """
        Override message logging - don't want reverse DNS lookups
        or output to stderr

        The first argument, FORMAT, is a format string for the
        message to be logged.  If the format string contains
        any % escapes requiring parameters, they should be
        specified as subsequent arguments (it's just like
        printf!).

        The client host and current date/time are prefixed to
        every message.

        """
        pass
        # if self.server.cfg["log_requests"]:
        # self.get_log().debug(format%args)

    def getPath(self, strip_args=False):
        """
        Get the unicode, unquoted path of the request
        """
        path = urllib.parse.unquote(self.path)

        if strip_args and path.find("?") > -1:
            path = path[:path.find("?")]
        return path

    def _get_params(self):
        path = self.getPath()
        if path.find("?") > -1:
            args = urllib.parse.parse_qs(path[path.find("?") + 1:])
            for key in args:
                args[key] = args[key][0]
            path = path[:path.find("?")]
        else:
            args = {}
        return args

    def _send_text(self, text, mimetype="application/json", response=200, content_range=None):
        # Only compress text if > 100 bytes
        if self._compress and len(text) > 100:
            encoding = "gzip"
            compressed = io.BytesIO()
            zipped = gzip.GzipFile(mode="w", fileobj=compressed,
                                   compresslevel=9)
            zipped.write(text.encode("utf-8"))
            zipped.close()
            text = compressed.getvalue()
        else:
            encoding = None
            text = text.encode("utf-8")

        self.prepare_send(mimetype, len(text), encoding=encoding,
                          response=response, content_range=content_range, cache="no-cache")
        self.wfile.write(text)
        # self.wfile.close()

    def prepare_send(self, type, size=None, response=200, encoding=None, content_range=None, cache=None):
        try:
            self.send_response(response)
        except Exception as e:
            print("Error sending response: %s" % e)
            # self.get_log().warning("Error sending response: %s"%e)
            return

        self.send_header("server", self.server_version)
        if type:
            self.send_header("Content-Type", type)

        self.send_header("Access-Control-Allow-Origin", "*")
        if content_range:
            self.send_header("Content-Range", "bytes %d-%d/%d" % content_range)

        self.send_header("Accept-Ranges", "bytes")
        if size:
            self.send_header("Content-Length", size)
        if encoding:
            self.send_header("Content-Encoding", encoding)
        if cache:
            self.send_header("Cache-Control", cache)
        self.end_headers()

    def failed(self, code, message=None):
        """
        Request failed, return error
        """
        try:
            if message:
                self.send_error(code, str(message))
            else:
                self.send_error(code)
        except Exception as e:
            print("Could not send error:", e)
        return False

    def do_GET(self):
        path = self.getPath(strip_args=True)
        args = self._get_params()

        self._compress = False
        if "Accept-Encoding" in self.headers:
            if "gzip" in self.headers["Accept-Encoding"]:
                self._compress = True

        # Check if we provide the requested method
        if path.startswith("/JSON.py/"):
            # print(" *** GOT ARGS **", args)
            path = path[9:]
            try:
                if path in functions:
                    ret = functions[path](self, **args)
                    self._send_text(ret)
                    return  # ALL OK
                else:
                    return self.failed(404)
            except Exception as e:
                print(e)
                import traceback
                traceback.print_exc()
                return self.failed(500)

        # Not a JSON request, check if it's a file within our webroot
        cfg = API.get_config("System.WebServer")
        p = os.path.join(cfg["web_root"], path[1:])
        if p.endswith("/"):
            p += "index.html"
        if os.path.exists(p):
            print("SERVING", p)
            with open(p, "rb") as f:
                data = f.read()
                ftype = mimetypes.guess_type(p)[0]
                self.prepare_send(ftype, len(data))
                self.wfile.write(data)
            return
        print("File does not exist:", p)

        return self.failed(404)


class MyWebServer(http.server.HTTPServer):
    # Override that blasted blocking thing!
    def get_request(self):
        """Get the request and client address from the socket.
        Override to allow non-blocking requests.

        WARNING: This will make "serve_forever" and "handle_request"
        throw exceptions and stuff! Serve_forever thus does not work!
        """
        if select.select([self.socket], [], [], 1)[0]:
            return self.socket.accept()
        else:
            return None



class FakeStatusListener:
    """
    Just for debugging
    """
    monitoring = {}
    import threading
    lock = threading.Condition()

    def add_monitors(self, stuff):

        for s in stuff:
            if not s in self.monitoring:
                self.monitoring[s] = None

        print("FakeStatusListener monitoring", self.monitoring)


    def wait(self, timeout):
        with self.lock:
            self.lock.wait(timeout)

    def get_last_values(self, items):

        print("--- Getting last values for", items)
        # We're totally faking everything!
        import random
        vals = {item: (random.random(), time.time()) for item in items}

        # vals = {vals[item]: monitoring[item] for item in items}
        return vals


livethreads = []
global statusListener
statusListener = None

def getStatusListener():
    global statusListener
    if not statusListener:
        # statusListener = FakeStatusListener()
        statusListener = StatusListener()
    return statusListener


class LiveHandler(WebSocket):
    """
    Handle requests for live updates
    """
    last_vals = {}
    stopped = False

    def monitorThread(self):
        # last_vals is a map with {(chan, param): (value, timestamp)}

        print(" *** Monitor thread started", self.last_vals.keys())

        # Check for all of the chan, param subscriptions and send any updates
        listener = getStatusListener()
        while not self.stopped:

            tosend = []

            vals = listener.get_last_values(self.last_vals.keys())
            for i in vals:
                val = vals[i]
                if val is None:
                    continue

                if i in self.last_vals and self.last_vals[i] and self.last_vals[i]["ts"] < val["ts"]:
                    tosend.append(val)
                    # updated
                    # tosend["|".join(i)] = val
                    self.last_vals[i] = val

            if len(tosend) > 0:
                self.sendMessage(json.dumps({"type": "update", "values": tosend}))

            listener.wait(3.0)  # Wait for any updates

    def handleMessage(self):
        print("GOT LIVE REQUEST", self.data)
        try:
            req = json.loads(self.data)
        except Exception as e:
            print("Bad live request, not json?", self.data, e)
            self.sendMessage(json.dumps({"type": "error", "msg": "Expected JSON request"}))
            # API.get_log("System.WebServer").error("Bad live request")
            return

        try:
            if req["type"] == "subscribe":
                items = []
                for chan in req["channels"]:
                    print("Channel", chan)
                    for param in req["channels"][chan]:
                        print("Param", param, req["channels"][chan]);
                        print("Subscribe to", chan, param)
                        items.append((chan, param))
                        self.last_vals[(chan, param)] = {"ts": 0}
                if len(items) > 0:
                    getStatusListener().add_monitors(items)

            elif req["type"] == "unsubscribe":
                items = []
                for chan in req["channels"]:
                    print("Channel", chan)
                    for param in req["channels"][chan]:
                        if (chan, param) in self.last_vals:
                            del self.last_vals[(chan, param)]

                    # StatusListener don't support removing things yet
                    # getStatusListener().remove_monitors(items)


        except Exception as e:
            print("Badly shaped live request", req, e)
            self.sendMessage(json.dumps({"type": "error", "msg": "Badly shaped request"}))

    def handleConnected(self):
        print("Got LIVE connect from", self.address)
        self.last_vals = {}

        # Start a thread to check for changes
        t = threading.Thread(target=self.monitorThread)
        t.start()

    def handleClose(self):
        print("Closed LIVE connection from", self.address)
        # The statuslistener doesn't support unsubscribing, otherwise we'd clean up here
        self.stopped = True

try:
    cfg = API.get_config("System.WebServer")
    cfg.set_default("port", 8080)
    cfg.set_default("web_root", "./")

    cfg.set_default("live_port", 8081)
    cfg.set_default("enable_live", False)


    log = API.get_log("System.WebServer")
    if cfg["enable_live"]:
        if not use_status_listener:
            print("ERROR: Live is enabled, but no shared memory status listener is available")
            log.error("Live is enabled, but no shared memory status listener is available")
        else:
            import time
            def run_until_stopped(s):
                while not API.api_stop_event.isSet():
                    s.serveonce()
                    time.sleep(0.050)

            # Start the web socket server
            server = SimpleWebSocketServer('', int(cfg["live_port"]), LiveHandler)
            t = threading.Thread(target=run_until_stopped, args=(server,))
            t.start()

            print("Started live server on port %s" % cfg["live_port"])
            log.info("Started live server on port %s" % cfg["live_port"])

    httpd = http.server.HTTPServer(("", cfg["port"]), MyHandler)
    print("Serving on port", cfg["port"])
    log.info("Serving on port %s" % cfg["port"])

    while True:
        httpd.serve_forever()
except KeyboardInterrupt:
    pass
finally:
    API.shutdown()

# with MyWebServer(("", PORT), MyHandler) as httpd:
 #   print("serving at port", PORT)
 #   while True:
 #       httpd.handle_request()
    # httpd.serve_forever()
