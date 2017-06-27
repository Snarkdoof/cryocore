import sys
import select
import socket
import os
import os.path
import mimetypes
import threading
import time
import re
import random
import gzip
import io
import json
import cgi
import hashlib
import base64
if sys.version_info.major == 3:
    import http.server as BaseHTTPServer
    import socketserver as SocketServer
    import urllib.parse as urllib
else:
    import BaseHTTPServer
    import SocketServer
    import urllib
    from StringIO import StringIO

# import MySQLdb
import mysql.connector as MySQLdb

from CryoCore import API
from CryoCore.Core.PrettyPrint import *
from CryoCore.Core.Utils import *
from CryoCore.Tools import TailLog
try:
    from CryoCore.Tools import HUD
except:
    pass  # No HUD support for Google Glass

from CryoCore.Core.InternalDB import mysql as sqldb

# Verbose error messages from CGI module
import cgitb
cgitb.enable()

DB_TYPE = "mysql"

global channel_ids
global param_ids
global disablePic
channel_ids = {}
param_ids = {}
disablePic = False
WebSocketMagic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

DEBUG = False


def toUnicode(string):
    """
    Function to change a string (unicode or not) into a unicode string
    Will try utf-8 first, then latin-1.
    TODO: Is there a better way?  There HAS to be!!!
    """
    if sys.version_info.major == 3:
        if string.__class__ == str:
            return string
        try:
            return str(string, "utf-8")
        except:
            pass
        return str(string, "latin-1")
    if string.__class__ == unicode:
        return string
    try:
        return unicode(string, "utf-8")
    except:
        pass
    return unicode(string, "latin-1")


def toBytes(val):
    if sys.version_info.major == 3:
        if val.__class__ == str:
            return val.encode("utf-8")
        return val
    if val.__class__ == unicode:
        return val.encode("utf-8")
    return val


def microsec_to_string(microsec):
    """
    Convert from million seconds to a decent string
    """

    str = ""

    sec = int(microsec) / 1000000000
    # Now make this into "hours" "minutes" "seconds"
    hours = sec / 3600
    if hours:
        sec -= hours * 3600
        str = "%d h " % hours

    minutes = sec / 60
    if minutes:
        sec -= minutes * 60
        str += "%d min " % minutes

    str += "%d sec" % sec

    return str


class LogDB(TailLog.TailLog):

    def __init__(self):
        TailLog.TailLog.__init__(self)

    def get_changes_by_time(self, args,
                            start_time,
                            stop_time):
        # TODO: Also do sessions, times and stuff here
        if args:
            sql = "AND " + args
        else:
            sql = ""

        rows = 0
        cursor = self._execute("SELECT * FROM log WHERE timestamp>? AND timestamp<? AND %s ORDER BY id" % sql,
                               [start_time, stop_time])
        return cursor

    def get_changes_by_id(self, args, min_id, limit):
        """
        return updates since last time and
        """
        _last_id = min_id
        if not _last_id:
            cursor = self._execute("SELECT MAX(id) FROM log")
            row = cursor.fetchone()
            if row[0]:
                _last_id = max(row[0] - 10, 0)

        rows = 0
        SQL = "SELECT * FROM log WHERE id>? "
        params = [_last_id]
        if len(args) > 0:
            SQL += "AND %s " % args[0]
            params += args[1]
        SQL += "ORDER BY id DESC "
        if limit:
            SQL += "LIMIT %s"
            params.append(int(limit))
        cursor = self._execute(SQL, params)
        return cursor

    def get_modules(self):
        """
        Return a list of modules that have been logging
        """
        cursor = self._execute("SELECT DISTINCT(module) FROM log")
        return cursor

    def get_log_levels(self):
        """
        Return a list of textual log levels that are available
        """
        return list(API.log_level_str.keys())


class DBWrapper:
    def __init__(self, status_db="System.Status.MySQL"):
        self.status_db = status_db
        self._dbs = {}

    def get_db(self, db_name):
        if db_name not in list(self._dbs.keys()):
            self._dbs[db_name] = StatusDB(db_name=db_name, status_db=self.status_db)

        return self._dbs[db_name]


class StatusDB(sqldb):

    def __init__(self, name="WebGui.StatusDB", db_name=None, status_db="System.Status.MySQL"):

        self._cached_params = {}
        self.name = name
        self.log = API.get_log(self.name)
        cfg = API.get_config(status_db)
        if not cfg["db_name"]:
            cfg = API.get_config("System.InternalDB")
        sqldb.__init__(self, name, cfg, db_name=db_name)

        init_sqls = ["""CREATE TABLE IF NOT EXISTS items (
item_key VARCHAR(255) PRIMARY KEY,
value TEXT)"""]
        self._init_sqls(init_sqls)

    def _get_param_info(self, paramid):
        """
        Get the channel and name of a parameter
        """
        if paramid not in self._cached_params:
            SQL = "SELECT status_channel.name,status_parameter.name FROM status_channel,status_parameter "\
                "WHERE status_parameter.chanid=status_channel.chanid AND paramid=%s"
            cursor = self._execute(SQL, [paramid])
            row = cursor.fetchone()
            if not row:
                SQL = "SELECT status_channel.name,status_parameter2d.name, sizex, sizey FROM status_channel,status_parameter2d "\
                    "WHERE status_parameter2d.chanid=status_channel.chanid AND paramid=%s"
                cursor = self._execute(SQL, [paramid])
                row = cursor.fetchone()
                if not row:
                    raise Exception("No parameter '%s'" % paramid)
            return {"channel": row[0],
                    "name": row[1],
                    "type": "2d",
                    "size": (sizex, sizey)}
            # self._cached_params[paramid] = {"channel": row[0],
            #                                "name": row[1],
            #                                "type": "2d",
            #                                "size": (sizex, sizey)}
        return self._cached_params[paramid]

    # ########## Available interface ############

    def get_last_status_values(self, session, max_time=None):
        """
        Return the last (paramid, id, timestamp, value) of all parameters of a session
        """
        res = []
        SQL = "SELECT paramid FROM status_view_mapping WHERE viewid=%s"
        cursor = self._execute(SQL, [session["id"]])
        for row in cursor.fetchall():
            SQL2 = "SELECT id, timestamp, value FROM status WHERE paramid=%s "
            paramid = row[0]
            params = [paramid]
            if max_time:
                SQL2 += "AND timestamp<%s "
                params.append(max_time)
            SQL2 += "ORDER BY timestamp DESC LIMIT 1"
            cursor2 = self._execute(SQL2, params)
            for num, ts, val in cursor2.fetchall():
                res.append((num, ts, paramid, val))
        return res

    def get_timestamps(self):
        """
        Return the smallest and largest timestamp (for relative clocks)
        """
        cursor = self._execute("SELECT MIN(timestamp), MAX(timestamp) FROM status")
        try:
            row = cursor.fetchone()
            return {"min": row[0], "max": row[1]}
        except:
            return {}

    def get_op_clock(self, session, max_time=None, since=0):
        # Must get the max op_clock of all params of a session
        params = [since]
        if max_time:
            m = "timestamp < %s AND "
            params.append(max_time)
        else:
            m = ""
        SQL = "SELECT MAX(id) FROM status,status_view_mapping WHERE id>%s AND " + m + "viewid=%s AND status_view_mapping.paramid=status.paramid"
        params.append(session["id"])

        cursor = self._execute(SQL, params)
        row = cursor.fetchone()
        if not row:
            return since
        if row[0] is None:
            return since
        return int(row[0])

    def get_channels(self):
        cursor = self._execute("SELECT chanid, name FROM status_channel ORDER BY name")
        global channel_ids
        channels = []
        for row in cursor.fetchall():
            if row[1] not in channel_ids:
                channel_ids[row[1]] = row[0]
            channels.append(row[1])
        return channels

    def get_params(self, channel):
        if channel not in channel_ids:
            self.get_channels()

        cursor = self._execute("SELECT paramid, name FROM status_parameter WHERE chanid=? ORDER BY name", [channel_ids[channel]])
        if DEBUG:
            self.log.debug("SELECT paramid, name FROM status_parameter WHERE chanid=? ORDER BY name" + str([channel_ids[channel]]))
        params = []
        global param_ids
        for row in cursor.fetchall():
            fullname = channel + "." + row[1]
            if fullname not in param_ids:
                param_ids[fullname] = row[0]
            params.append(row[1])

        cursor = self._execute("SELECT paramid, name FROM status_parameter2d WHERE chanid=? ORDER BY name", [channel_ids[channel]])
        for row in cursor.fetchall():
            fullname = channel + "." + row[1]
            if fullname not in param_ids:
                param_ids[fullname] = row[0]
            params.append(row[1])

        return params

    def get_params_with_ids(self, channel):
        if channel not in channel_ids:
            self.get_channels()

        cursor = self._execute("SELECT paramid, name FROM status_parameter WHERE chanid=? ORDER BY name", [channel_ids[channel]])
        if DEBUG:
            self.log.debug("SELECT paramid, name FROM status_parameter WHERE chanid=? ORDER BY name" + str([channel_ids[channel]]))
        params = []
        global param_ids
        for row in cursor.fetchall():
            fullname = channel + "." + row[1]
            if fullname not in param_ids:
                param_ids[fullname] = row[0]
            params.append((row[0], row[1]))

        cursor = self._execute("SELECT paramid, name, sizex, sizey FROM status_parameter2d WHERE chanid=? ORDER BY name", [channel_ids[channel]])
        if DEBUG:
            self.log.debug("SELECT paramid, name FROM status_parameter2d WHERE chanid=? ORDER BY name" + str([channel_ids[channel]]))
        for row in cursor.fetchall():
            fullname = channel + "." + row[1]
            if fullname not in param_ids:
                param_ids[fullname] = row[0]
            params.append(("2d%d" % row[0], row[1], (row[2], row[3])))

        return params

    def get_data(self, params, start_time, end_time, since=0, since2d=0, aggregate=None):
        if len(params) == 0:
            raise Exception("No parameters given")
        print("Getting data between %s and %s (%s)" % (float(start_time) - time.time(), float(end_time) - time.time(), start_time))
        dataset = {}
        params2d = []
        max_id = since
        p = [start_time, end_time, since]
        SQL = "SELECT id, paramid, timestamp, value FROM status WHERE timestamp>%s AND timestamp<%s AND id> %s AND ("
        for param in params:
            if param.startswith("2d"):
                params2d.append(param[2:])
                continue
            SQL += "paramid=%s OR "
            p.append(param)
        if aggregate is not None:
            SQL = SQL[:-4] + ") GROUP BY paramid, timestamp DIV %d" % aggregate
        else:
            SQL = SQL[:-4] + ") ORDER BY paramid, timestamp"
        if len(p) > 3:
            cursor = self._execute(SQL, p)
            for i, p, ts, v in cursor.fetchall():
                max_id = max(max_id, i)
                if p not in dataset:
                    dataset[p] = []

                try:
                    v = float(v)
                except:
                    pass
                dataset[p].append((ts, v))

        max_id2d = since2d
        if len(params2d) > 0:
            p = [start_time, end_time, since2d]
            SQL = "SELECT id, paramid, timestamp, value, posx, posy FROM status2d WHERE timestamp>%s AND timestamp<%s AND id> %s AND ("
            for param in params2d:
                SQL += "paramid=%s OR "
                p.append(param)
            if aggregate is not None:
                SQL = SQL[:-4] + ") GROUP BY paramid, timestamp DIV %d" % aggregate
            else:
                SQL = SQL[:-4] + ") ORDER BY paramid, id"

            cursor = self._execute(SQL, p)
            for i, p, ts, v, posx, posy in cursor.fetchall():
                p = "2d%d" % p
                max_id2d = max(max_id2d, i)
                if p not in dataset:
                    dataset[p] = []

                try:
                    v = int(v)
                except:
                    pass
                dataset[p].append((ts, v, (posx, posy)))

        return max_id, max_id2d, dataset

    def get_max_data(self, params, start_time, end_time, since=0, aggregate=None):
        if len(params) == 0:
            raise Exception("No parameters given")

        if not aggregate:
            raise Exception("Need aggregate value")

        p = [start_time, end_time, since]
        SQL = "SELECT timestamp, max(value) FROM status WHERE timestamp>%s AND timestamp<%s AND id> %s AND ("
        for param in params:
            SQL += "paramid=%s OR "
            p.append(param)
        SQL = SQL[:-4] + ") GROUP BY timestamp DIV %d" % aggregate

        API.get_log("WS").debug(SQL + "," + str(p))
        cursor = self._execute(SQL, p)
        dataset = {}
        max_id = since
        for ts, v in cursor.fetchall():
            try:
                v = float(v)
            except:
                pass
            dataset[ts] = v
        return 0, dataset

    def add_item(self, key, item):
        SQL = "REPLACE INTO items (item_key, value) VALUES (%s, %s)"
        self._execute(SQL, [key, item])

    def get_item(self, key):
        SQL = "SELECT value FROM items WHERE item_key=%s"
        cursor = self._execute(SQL, [key])
        row = cursor.fetchone()
        if row:
            return row[0]
        return ""

    def list_items(self):
        SQL = "SELECT item_key FROM items"
        cursor = self._execute(SQL)
        res = []
        for row in cursor.fetchall():
            res.append(row[0])
        return res


class MyWebServer(BaseHTTPServer.HTTPServer):
    """
    Non-blocking, multi-threaded IPv6 enabled web server
    """
    # Override to avoid blocking
    def get_request(self):
        """Get the request and client address from the socket.
        Override to allow non-blocking requests.

        WARNING: This will make "serve_forever" and "handle_request"
        throw exceptions and stuff! Serve_forever thus does not work!
        """

        # Use select for non-blocking IO
        if select.select([self.socket], [], [], 1)[0]:
            return self.socket.accept()
        else:
            return None

# TODO: Implement class ThreadPoolMixIn  defining
# def process_request(self, request, client_address):
# Which queues requests on a queu and executes the handler
# with requests, client_address as parameter
#        """Start a new thread to process the request."""
#        t = threading.Thread(target = self.process_request_thread,
#                             args = (request, client_address))
#        if self.daemon_threads:
#            t.setDaemon (1)
#        t.start()
# where process_request_thread(self): is
#        try:
#            self.finish_request(request, client_address)
#            self.close_request(request)
#        except:
#            self.handle_error(request, client_address)
#            self.close_request(request)


class MyWebHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    Handle requests
    """

    server_version = "UAV Onboard/0.1"

    # Python3 seems to get I/O operation errors all the time on flush, could
    # be that it expects longer connections?
    def handle_one_request(self):
        try:
            BaseHTTPServer.BaseHTTPRequestHandler.handle_one_request(self)
        except ValueError as e:
            if str(e) == "I/O operation on closed file.":
                return
            raise e

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

        if self.server.cfg["log_requests"]:
            self.get_log().debug(format % args)

    def getPath(self, strip_args=False):
        """
        Get the unicode, unquoted path of the request
        """
        path = urllib.unquote(self.path)
        if sys.version_info.major == 2:
            path = unicode(path, "utf-8", "replace")

        if strip_args and path.find("?") > -1:
            path = path[:path.find("?")]
        return path

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

    def get_log(self):
        return API.get_log("WebGUI.Handler")

    def prepare_send(self, type, size=None, response=200, encoding=None, content_range=None):
        try:
            self.send_response(response)
        except Exception as e:
            self.get_log().warning("Error sending response: %s" % e)
            return

        self.send_header("server", self.server_version)
        self.send_header("Access-Control-Allow-Origin", "*")

        # Send type
        if type:
            self.send_header("Content-Type", type)

        if content_range:
            self.send_header("Content-Range", "bytes %d-%d/%d" % content_range)

        self.send_header("Accept-Ranges", "bytes")

        if size:
            self.send_header("Content-Length", size)

        if encoding:
            self.send_header("Content-Encoding", encoding)

        self.end_headers()

    def _to_file(self, path):
        """
        Return a valid file, send error and return None if not allowed
        """
        if not path:
            return self.failed(404, "No file specified")

        if not self.server.cfg["web_root"]:
            self.server.log.error("Internal error: 'web_root' config parameter does not exist")
            return self.failed(400)

        file_path = os.path.join(self.server.cfg["web_root"], path[1:])

        if not os.path.abspath(file_path).startswith(os.path.abspath(self.server.cfg["web_root"])):
            self.failed(403)
            return None

        if not os.path.exists(file_path):
            self.failed(404, "No such file '%s'" % file_path)
            return None
        return file_path

    def _send_image(self, path, fullpath=False):
        if not path:
            return self.failed(404, "No file specified")
        if fullpath:
            p = path
        else:
            p = self._to_file(path)
        if not p:
            self.server.log.info("Got request for '%s' which turned into None" % path)
            return self.failed(404, "Nothing know about '%s'" % path)

        if os.path.exists(p):
            data = open(p, "r").read()
            self.prepare_send("image/jpeg", len(data))
            self.wfile.write(toBytes(data))
            self.wfile.close()
            return
        self.failed(404, "Missing file '%s'" % path)

    def do_POST(self):
        path = self.getPath()
        if path.startswith("/set"):
            length = int(self.headers.getheader('content-length'))
            print("POST data", length)
            try:
                data = self.rfile.read(length)
                d = json.loads(urllib.unquote(data))
                print("Data is:", d)
            except:
                self.failed(400, "Bad request")
            # Should insert into database
            db = self.get_db()
            for key in d:
                c = db._execute("SELECT name FROM status_parameter WHERE paramid=%s", [key])
                row = c.fetchone()
                c.close()
                if not row:
                    print("Bad parameter", key)
                    return self.failed(400, "Bad parameter: %s" % key)

            # Have all keys, insert
            print("WILL INSERT")
            for key in d:
                params = []
                SQL = "INSERT INTO status (timestamp, paramid, value) VALUES "
                for ts, value in d[key]:
                    SQL += "(%s,%s,%s),"
                    params.append(ts)
                    params.append(key)
                    params.append(value)
                SQL = SQL[:-1]
                c = db._execute(SQL, params)

            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            return

        if path != "/set_config":
            return self.failed(404)

        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        if ctype == 'multipart/form-data':
            form = cgi.parse_multipart(self.rfile, pdict)
        elif ctype == 'application/x-www-form-urlencoded':
            length = int(self.headers.getheader('content-length'))
            form = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
        else:
            form = {}
        self.server.log.debug("set config: " + str(list(form.keys())))

        if "version" not in form:
            return self.send_error(400, "Missing version")
        if "json" not in form:
            return self.send_error(400, "Missing config")

        if "root" in form:
            root = form["root"].value
        else:
            root = None

        version = form["version"][0]
        serialized = form["json"][0]

        if path.startswith("/set_config"):
            try:
                self.server.log.info("Should save config" + " root:" + str(root) + ", version:" + str(version))

                self.server.log.info("Saving config '%s'" % version)

                cfg = API.get_config()

                # First we add the version if it doesn't already exist
                try:
                    cfg.add_version(version)
                except:
                    pass  # Ignore any errors at this point
                cfg.deserialize(serialized,
                                root=root,
                                version=version,
                                overwrite=True)
                self.server.log.debug("Saved OK")
                return self.send_response(200)
            except Exception as e:
                self.server.log.exception("Setting config '%s'" % path)
                return self.failed(404, "Failed to save:" + str(e))

        return self.failed(300, "Should never get here")

    def _get_arg(self, args, name, default):
        if name in args:
            return args[name]
        return default

    def get_db(self):
        return self.server.db.get_db(self._get_params()["db"])

    def _do_GET_pic(self, path, args):
        """
        Divided into two different variants - if a timestamp is given,
        the image id to the correct image is given.  If img has a
        value, that image is returned with the correct quality

        Try to get a picture based on a timestamp, according to the
        given quality parameters
        - quality (typical values 0.4-0.9), default 0.9, 0 is thumbnail
        - scale (up to 1.0), default 1.0
        - crop (left,top,right,bottom), default whole image
        """
        global disablePic
        if disablePic:
            return self.failed(403, "Pictures disabled on basestation")

        if "t" not in args:
            if "img" not in args:
                return self.failed(400, "Need timestamp or picture id")

        if "t" in args:
            timestamp = float(args["t"])
            instrumentid = self._get_arg(args, "i", None)
            try:
                info = self.server.picture_supplier.get_image(timestamp,
                                                              instrumentid)
            except:
                return self.failed(404, "No image for given time")

            data = json.dumps(
                {"img": info["id"], "i": [info["timestamp"],
                 info["instrument"], info["lat"], info["lon"], info["alt"],
                 info["q0"], info["q1"], info["q2"], info["q3"],
                 info["roll"], info["pitch"], info["yaw"], info["alt"]]})
            return self._send_html(data)

        img_id = args["img"]
        if (self._get_arg(args, "o", False)):
            # Request for the unmodified original
            try:
                info = self.server.picture_supplier.get_image_by_id(img_id)
                self._send_image(info["path"], fullpath=True)
            except:
                self.failed(404)
            return

        quality = float(self._get_arg(args, "q", 0.9))
        scale = float(self._get_arg(args, "s", 1.0))
        crop = self._get_arg(args, "crop", None)
        histogram = self._get_arg(args, "h", None)
        headers_only = self._get_arg(args, "p", False)
        cached = self._get_cached(img_id, quality, scale, crop, histogram)
        if cached:
            return self._send_img_result(cached, headers_only)

        rotation = self.server.picture_supplier.get_rotation(img_id)

        crop_box = None
        if crop:
            c = crop.split(",")
            crop_box = (int(c[0]), int(c[1]), int(c[2]), int(c[3]))

        try:
            info = self.server.picture_supplier.get_image_by_id(img_id)
        except Exception as e:
            return self.failed(404, e)

        header = [info["timestamp"], info["instrument"],
                  info["lat"], info["lon"], info["alt"],
                  info["q0"], info["q1"], info["q2"], info["q3"]]
        if histogram:
            data = self.server.picture_supplier.get_histogram(info["path"])
            content_type = "application/binary"
        else:
            content_type = "image/png"
            # Got file info, prepare header and get data

            try:
                if quality == 0:
                    inf, data = self.server.picture_supplier.get_thumbnail(info["path"], rotation)
                    if len(data) == 0:
                        inf, data = self.server.picture_supplier.resample(info["path"], scale=0.2, rotation=rotation, filetype="png")
                else:
                    inf, data = self.server.picture_supplier.resample(
                        info["path"], scale=scale,
                        quality=quality, crop_box=crop_box, rotation=rotation, filetype="png")
            except Exception as e:
                self.server.log.exception("Getting picture")
                return self.failed(404, e)

            self.server.log.debug("Sending picture %s (%d bytes)" % (info["path"], len(data)))

        # We also send the physical size of the image as well as the
        # lat,lon of the boundingbox for it.  These are crude
        # estimations atm
        additional_info = {}
        for key in ["bb_size", "bb_sw", "bb_ne"]:
            if key in inf:
                additional_info[key] = inf[key]

        # Send data
        result = {
            "content-type": content_type,
            "content-length": len(data),
            "i": json.dumps(header),
            "a": json.dumps(additional_info),
            "data": data}
        self._cache_result(img_id, quality, scale, crop, histogram, result)
        self._send_img_result(result, headers_only)

    def _send_img_result(self, result, headers_only=False):
        self.send_response(200)
        for header in ["content-type", "content-length", "i", "a"]:
            if header in result:
                if headers_only and header == "content-length":
                    continue
                self.send_header(header, result[header])
        self.end_headers()
        if not headers_only:
            self.wfile.write(toBytes(result["data"]))
        self.wfile.close()
        return

    def _cache_result(self, img_id, quality, scale, crop, histogram, result):
        # Quick and dirty
        if not os.path.exists("/tmp/cache"):
            os.mkdir("/tmp/cache")

        cache_file = "/tmp/cache/%s_%s_%s_%s_%s" % (img_id, quality, scale, crop, histogram)
        f = open(cache_file, "w")
        f.write(toBytes(result["data"]))
        f.close()
        res = {}
        for k in list(result.keys()):
            if k == "data":
                continue
            res[k] = result[k]
        f = open(cache_file + ".nfo", "w")
        f.write(toBytes(json.dumps(res)))
        f.close()

    def _get_cached(self, img_id, quality, scale, crop, histogram):
        cache_file = "/tmp/cache/%s_%s_%s_%s_%s" % (img_id, quality, scale, crop, histogram)

        if os.path.exists(cache_file):
            f = open(cache_file + ".nfo", "r")
            result = json.loads(f.read())
            f.close()
            f = open(cache_file, "r")
            result["data"] = f.read()
            f.close()
            return result
        return None

    def _get_additional_info(self, rotation, inf):
        """
        Physical size of bounding box + lat_lon of bounding box
        """
        additional = {}

        # We expect square pixels
        additional["px_size"] = rotation["image_width"] / inf["new_size"][0]

        # bounding-box size
        additional["bb_size"] = (inf["bb_size"][0] * additional["px_size"],
                                 inf["bb_size"][1] * additional["px_size"])

        # Lat-lon top left and bottom right corners
        # Latitude: 1 deg = 110.54 km
        # Longitude: 1 deg = 111.320*cos(latitude) km
        additional["bb_sw"] = (rotation["lat"] - (additional["bb_size"][0] / (2 * 110540.0)),
                               rotation["lon"] - (additional["bb_size"][1] / (2 * 112320.0 * math.cos(math.radians(rotation["lat"])))))
        additional["bb_ne"] = (
            rotation["lat"] + (additional["bb_size"][0] / (2 * 110540.0)),
            rotation["lon"] + (additional["bb_size"][1] / (2 * 112320.0 * math.cos(math.radians(rotation["lat"])))))

        return additional

    def processItemRequest(self, path, args):
        data = None
        if path == "/item/list":
            l = self.get_db().list_items()
            data = json.dumps({"keys": l})
        elif path == "/item/add":
            self.get_db().add_item(args["key"], args["item"])
            data = ""
        elif path == "/item/get":
            data = self.get_db().get_item(args["key"])
        if data is None:
            return self.failed(404, "Missing data")
        self._send_html(data)

    def processConfigRequest(self, path, args):
        cfg = self.server.root_cfg
        res = None
        try:
            if path == "/cfg/get":
                res = cfg[args["param"]]
            elif path == "/cfg/set":
                try:
                    if args["value"].isdigit():
                        val = int(args["value"])
                    elif args["value"].replace(".", "").isdigit():
                        val = float(args["value"])
                    else:
                        val = args["value"]
                except:
                    val = args["value"]
                cfg.set(args["param"], val, check=False)
                res = []
            elif path == "/cfg/isupdated":
                if "since" not in args:
                    return self.failed(400, "Bad request, missing 'since'")
                since = float(args["since"])
                last_updated = cfg.last_updated()
                res = {"updated": last_updated > since + 0.00001, "last_updated": float(last_updated)}

            elif path == "/cfg/versions":
                cfg = API.get_config()
                versions = cfg.list_versions()
                serialized = json.dumps(versions)
                self._send_html(serialized)
                return

            elif path == "/cfg/serialize":
                if "root" in args:
                    config_root = args["root"]
                else:
                    config_root = None
                if "version" in args:
                    version = args["version"]
                else:
                    version = "default"

                cfg = API.get_config(config_root, version=version)
                serialized = cfg.serialize()
                return self._send_html(serialized)
        except:
            self.server.log.exception("Config request failed: %s=%s" % (path, args))
            return self.failed(400, "Bad request")
        if res is None:
            return self.failed(404, "No such elmement %s" % path)
        self._send_html(json.dumps(res))

    def do_GET(self):
        try:
            self._do_GET()
        except:
            self.server.log.exception("In GET")

    def _get_params(self):
        path = self.getPath()
        if path.find("?") > -1:
            args = cgi.parse_qs(path[path.find("?") + 1:])
            for key in args:
                args[key] = args[key][0]
            path = path[:path.find("?")]
        else:
            args = {}

        if "ts" not in args:
            args["ts"] = None

        if "db" not in args:
            args["db"] = None
        return args

    def _do_ws_request(self):
        """
        Process a web socket request
        """
        headers = self.headers
        self.server.log.debug("Processing web socket request")
        # valid = True
        if "upgrade" in list(headers.keys()):
            if headers["upgrade"] == "websocket":
                self.send_response(101)
                # Handshake
                if "cookie" in list(headers.keys()):
                    self.send_header("Cookie", headers["cookie"])
                self.send_header("Connection", "Upgrade")
                self.send_header("Upgrade", "websocket")
                self.send_header("Sec-WebSocket-Version", 13)
                the_string = headers["sec-websocket-key"] + WebSocketMagic
                m = hashlib.sha1()
                m.update(the_string)
                self.send_header("Sec-WebSocket-Accept", base64.b64encode(m.digest()))
        else:
            return self.error(400, "Unsupported request")

        path = self.getPath(strip_args=True)
        if path == "/hud":
            # Head up display
            class fakeit:
                def __init__(self, rfile, wfile):
                    self.rfile = rfile
                    self.wfile = wfile

                def send(self, data):
                    return self.wfile.write(toBytes(data))

                def recv(self, maxlen):
                    return str(self.rfile.read(maxlen), "utf-8")

            f = fakeit(self.rfile, self.wfile)
            hud = HUD.HUDProvider(f)
            hud.start()
            return

    def _do_GET(self):

        if "upgrade" in list(self.headers.keys()):
            return self._do_ws_request()

        self._compress = False
        if "Accept-Encoding" in self.headers:
            if "gzip" in self.headers["Accept-Encoding"]:
                self._compress = True

        path = self.getPath(strip_args=True)
        args = self._get_params()

        if path.startswith("/cfg"):
            return self.processConfigRequest(path, args)

        if path.startswith("/feedback"):
            if ("text" not in args or "ts" not in args):
                return self.failed(400, "Need both text and ts arguments")
            self.server.status["feedback"].set_value(args["text"], timestamp=args["ts"])
            return self.send_response(200)

        # Picture request?
        try:
            if path.startswith("/pic"):
                return self._do_GET_pic(path, args)
        except:
            self.server.log.exception("Getting picture")
            return self.failed(500, "unknown error")

        if path.startswith("/item/"):
            return self.processItemRequest(path, args)

        if path.startswith("/server_time"):
            self._send_html(json.dumps({"time": time.time()}))
            return

        if path.startswith("/ts"):
            info = self.get_db().get_timestamps()
            self._send_html(json.dumps(info))
            return

        if path.startswith("/img/"):
            return self._send_image(path)

        if path.startswith("/list_channels_and_params_full"):
            return self._list_channels_and_params_full()

        if path.startswith("/list_channels_and_params"):
            return self._list_channels_and_params()

        if path.startswith("/list_channels"):
            return self._list_channels()

        if path.startswith("/list_params/"):
            channel = path[13:]
            return self._list_params(channel)

        if path.startswith("/get_trios_plot/"):
            html = self._get_trios_plot()
            if html:
                self._send_html(html)
            else:
                self.failed(404)
            return

        if path.startswith("/list_log_modules"):
            html = self._list_log_modules()
            if html:
                self._send_html(html)
            else:
                self.failed(404)
            return

        if path.startswith("/list_log_levels"):
            html = self._list_log_levels()
            if html:
                self._send_html(html)
            else:
                self.failed(404)
            return

        if path.startswith("/get_logs/"):
            if "st" not in args:
                args["st"] = None
            elems = path[1:].split("/")
            if len(elems) > 1:
                if not elems[1].upper() in API.log_level_str:
                    level = API.log_level_str["DEBUG"]
                else:
                    level = API.log_level_str[elems[1].upper()]
            if len(elems) > 2:
                module = str(elems[2])
            else:
                module = ""
            if len(elems) > 3:
                since = str(elems[3])
            else:
                since = 0

            html = self._get_logs(level, module, since,
                                  min_time=args["st"], max_time=args["ts"])
            if html:
                self._send_html(html)
            else:
                self.failed(404)
            return

        if path.startswith("/get"):
            # List of parameters to get, and a time window is required
            if "params" not in args:
                return self.failed(500, "Missing parameter list")

            if "start" not in args:
                return self.failed(500, "Missing start")

            if "end" not in args:
                return self.failed(500, "Missing end")

            try:
                params = json.loads(args["params"])
                if len(params) == 0:
                    return self.failed(500, "Missing parameters")
            except:
                return self.failed(500, "Bad parameter list")
            if "since" in args:
                since = int(args["since"])
            else:
                since = 0
            if "since2d" in args:
                since2d = int(args["since2d"])
            else:
                since2d = 0
            if "aggregate" in args:
                aggregate = int(args["aggregate"])
            else:
                aggregate = None
            # Now get the data!
            if (path.startswith("/getmax")):
                ret = self._get_max_data(params, args["start"], args["end"], since, since2d, aggregate)
            else:
                ret = self._get_data(params, args["start"], args["end"], since, since2d, aggregate)
            if ret:
                ret["ts"] = time.time()
                self._send_html(json.dumps(ret))
            else:
                self.failed(404)
            return

        # Just look for the file
        if path == "/":
            file_path = self._to_file("/index.html")
        else:
            file_path = self._to_file(path)

        if file_path:
            response = 200
            content_range = None
            ftype = mimetypes.guess_type(file_path)[0]
            if "Range" in self.headers:
                response = 206
                r = self.headers["Range"]
                if not r.startswith("bytes="):
                    self.failed(400, "Bad request range")
                start, end = r[6:].split("-")
                f = open(file_path, "br")
                f.seek(int(start))
                fsize = os.path.getsize(file_path)
                if not end:
                    content = f.read()
                    content_range = (int(start), len(content) + int(start) - 1, fsize)
                else:
                    content = f.read(int(end) - int(start))
                    content_range = (int(start), int(end), fsize)
            else:
                try:
                    content = open(file_path, "rb").read()
                    self.server.log.debug("Read %d bytes from file %s" % (len(content), file_path))
                    if ftype == "text/html" or ftype == "application/javascript":
                        content = toUnicode(content)
                        content = self._replace_base_path(content)
                except:
                    self.server.log.exception("Failed to read file %s" % file_path)
                    return self.failed(500, "Failed to read file")

            self._send_html(content, ftype, response=response, content_range=content_range)

    def _replace_base_path(self, content):
        """
        Allow config of base-path, allowing us to move some library stuff
        to another machine if we want to
        """
        content = toUnicode(content)
        if self.server.cfg["base_path"]:
            return content.replace("_BASE_PATH_", self.server.cfg["base_path"])
        return content.replace("_BASE_PATH_", "")

    def _list_channels_and_params(self):
        """
        Return all channels and their parameters as one giant json dump.
        """
        channelsAndParams = {}
        for channel in self.get_db().get_channels():
            params = self.get_db().get_params(channel)
            channelsAndParams[channel] = params
        data = json.dumps({"channels": channelsAndParams})
        self._send_html(data)

    def _list_channels_and_params_full(self):
        """
        Return all channels and their parameters as one giant json dump.
        """
        channelsAndParams = {}
        for channel in self.get_db().get_channels():
            params = self.get_db().get_params_with_ids(channel)
            channelsAndParams[channel] = params
        data = json.dumps({"channels": channelsAndParams})
        self._send_html(data)

    def _list_channels(self):
        """
        Create HTML of the channels - use a table
        """
        channels = []
        for channel in self.get_db().get_channels():
            channels.append(channel)
        self._send_html(json.dumps({"channels": channels}))

    def _list_params(self, channel):
        """
        Create HTML of the params of a channel - will be put into a table
        """
        self._send_html(json.dumps({"params": self.get_db().get_params(channel)}))
        return
        html = ""
        for param in self.get_db().get_params(channel):
            html += "<div class='param'><a href='javascript:add_param(\"%s\",\"%s\")'>%s</a></div>\n" % (channel, param, param)
        self._send_html(html)

    def _get_data(self, params, start_time, end_time, since=0, since2d=0, aggregate=None):
        """
        Return the dataset of the given parameters
        """
        max_id, max_id2d, dataset = self.get_db().get_data(params, start_time, end_time, since, since2d, aggregate)
        return {"max_id": max_id, "max_id2d": max_id2d, "data": dataset}

    def _get_max_data(self, params, start_time, end_time, since=0, aggregate=None):
        """
        Return the max values of the given parameters
        """
        max_id, dataset = self.get_db().get_max_data(params, start_time, end_time, since, aggregate)
        return {"max_id": max_id, "data": dataset}
        return json.dumps({"max_id": max_id, "data": dataset})

    def _get_trios_plot(self):
        """
        Return the last samples of the two TriOS spectrometers.

        Format returned is "up/down|timestamp|integrationtime|val1|val2|...|val255\n"
        Notice that value 0 is not available, it's always 0 anyways
        """
        if not self.server.trios:
            raise Exception("Missing Trios")

        dataset = {}
        for (instrument, timestamp, integration_time, values) in self.server.trios.get_last_samples():
            if instrument not in dataset:
                dataset[instrument] = {}
            dataset[instrument]["integration_time"] = integration_time
            dataset[instrument]["timestamp"] = time.ctime(timestamp)
            dataset[instrument]["data"] = []
            i = 0
            for value in values:
                dataset[instrument]["data"].append((i, value))
                i += 1
        return json.dumps(dataset)

    def _list_log_modules(self):
        cursor = self.server._log_db.get_modules()
        modules = []
        for row in cursor:
            modules.append(row[0])

        return json.dumps(modules)

    def _list_log_levels(self):
        levels = self.server._log_db.get_log_levels()
        return json.dumps(levels)

    def _get_logs(self, log_level=0, module="", since=0,
                  limit=1000, min_time=None, max_time=None):
        """
        Return log messages of the given log level.
        if module is given, only log messages from that module (and of the correct
        log levels) will be returned.

        TODO: start-time, end_time.
        """

        # If log_level is a string, convert it
        if log_level in list(API.log_level_str.keys()):
            log_level = API.log_level_str[log_level]
        elif log_level not in list(API.log_level.keys()):
            log_level = API.log_level_str["DEBUG"]  # DEFAULT show all

        dataset = []
        query = "level >= %s"
        params = [log_level]
        if module:
            query += " AND module=%s"
            params.append(module)

        if min_time:
            query += " AND time>%s"
            params.append(min_time)
        if max_time:
            query += " AND time<%s"
            params.append(max_time)
        cursor = self.server._log_db.get_changes_by_id((query, params), since, limit)
        largest_id = 0
        for row in cursor.fetchall():
            if row[TailLog.ID] > largest_id:
                largest_id = row[TailLog.ID]
            dataset.append((row[TailLog.ID],
                            row[TailLog.TEXT],
                            API.log_level[row[TailLog.LEVEL]],
                            time.ctime(row[TailLog.TIMESTAMP]),
                            row[TailLog.LINE],
                            row[TailLog.FUNCTION],
                            row[TailLog.MODULE],
                            row[TailLog.LOGGER]))
        dataset.reverse()
        return json.dumps({"max_id": largest_id, "data": dataset})

    def _send_html(self, html, mimetype="text/html", response=200, content_range=None):
        # Only compress text if > 100 bytes
        if mimetype and mimetype.startswith("text") and self._compress and len(html) > 100:
            encoding = "gzip"
            if sys.version_info.major == 3:
                compressed = io.BytesIO()
            else:
                compressed = StringIO()
            zipped = gzip.GzipFile(mode="w", fileobj=compressed,
                                   compresslevel=9)
            zipped.write(toBytes(html))
            zipped.close()
            html = compressed.getvalue()
        else:
            encoding = None

        data = toBytes(html)
        self.prepare_send(mimetype, len(data), encoding=encoding,
                          response=response, content_range=content_range)

        self.wfile.write(data)
        self.wfile.close()


class WebServer(threading.Thread):

    def __init__(self, port, db, stop_event):
        threading.Thread.__init__(self)
        print("Starting WebServer on port %s" % port)

        self.stop_event = stop_event

        self.log = API.get_log("WebGUI")
        self.cfg = API.get_config("System.WebServer")
        self.cfg.require(["web_root"])

        self.log.debug("Initializing WebServer")
        self.server = MyWebServer(('0.0.0.0', int(port)), MyWebHandler)
        self.server.cfg = self.cfg
        self.server.root_cfg = API.get_config()
        self.server.status = API.get_status("System.WebServer")
        self.server.db = db
        self.server.picture_supplier = None  # PictureSupplier()

        try:
            from Instruments.TriOS.TriOS import DBDump
            self.server.trios = DBDump("TriOS")
        except:
            self.server.trios = None

        self.server._log_db = LogDB()
        self.server.log = self.log

        self.log.debug("WebServer initialized OK")

    def run(self):
        while not self.stop_event.is_set():
            try:
                self.server.handle_request()
            except Exception:
                # Ignore these, Just means that there was no request
                # waiting for us
                pass

        self.log.info("Stopped")

    def stop(self):
        print("Stopping")
        self.running = False

        self.server.socket.close()


if __name__ == "__main__":

    socket.setdefaulttimeout(5.0)
    stop_event = threading.Event()
    if len(sys.argv) > 1:
        db = DBWrapper(status_db=sys.argv[1])
    else:
        db = DBWrapper()
    ws = WebServer(4321, db, stop_event)
    if "--disable-pic" in sys.argv:
        disablePic = True
    else:
        disablePic = False
    ws.start()

    try:
        while True:
            if sys.version_info.major == 3:
                cmd = input("WebServer running on port 4321, press Q to stop it ")
            else:
                cmd = raw_input("WebServer running on port 4321, press Q to stop it ")
            if cmd.strip().lower() == "q":
                break
    except:
        pass

    print("Stopping server")
    stop_event.set()

    API.shutdown()
