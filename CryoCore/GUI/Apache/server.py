import http.server
import select
import JSON
import io
import gzip
import urllib
import cgi
import inspect
import mimetypes
import os

from CryoCore import API

functions = {o[0]: o[1] for o in inspect.getmembers(JSON) if inspect.isfunction(o[1])}


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
            args = cgi.parse_qs(path[path.find("?") + 1:])
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
            path = path[9:]
            try:
                if path in functions:
                    ret = functions[path](self, **args)
                    self._send_text(ret)
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

try:
    cfg = API.get_config("System.WebServer")
    cfg.set_default("port", 8080)
    cfg.set_default("web_root", "./")
    httpd = http.server.HTTPServer(("", cfg["port"]), MyHandler)
    print("Serving on port", cfg["port"])
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
