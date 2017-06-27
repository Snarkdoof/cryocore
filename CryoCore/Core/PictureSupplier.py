import math
import os
import struct

import sys
try:
    import numpy
except:
    print("Missing numpy, some picture stuff might not work")
from . import Queue
import threading
import pyexiv2
from PIL import Image

from CryoCore.Core import API, Utils, InternalDB, Quaternion


def get_distance_x_y(source, destination):
    """
    Return the X and Y distance separately in meters as a touple
    """
    lat0, lon0 = source
    lat1, lon1 = destination

    grad2m = 1852 * 60
    x0 = lon0 * math.cos(lat0 * (math.pi / 180)) * grad2m
    x1 = lon1 * math.cos(lat1 * (math.pi / 180)) * grad2m
    y0 = lat0 * grad2m
    y1 = lat1 * grad2m

    return (x1 - x0, y1 - y0)


def to_lat_lon(source, xm, ym):
    lat0, lon0 = source

    grad2m = 1852 * 60.
    x0 = lon0 * math.cos(lat0 * (math.pi / 180)) * grad2m
    y0 = lat0 * grad2m

    x1 = xm + x0
    y1 = ym + y0

    lat1 = y1 / grad2m
    lon1 = x1 / (math.cos(lat1 * (math.pi / 180)) * grad2m)

    return (lat1, lon1)

try:
    from GUI.opengl.renderer import OpenGLRenderer
    opengl = True
    print("OpenGL support is present")
except Exception as e:
    print("No OpenGL")
    opengl = False


def quaternionToYawPitchRoll(q):
    ypr = [0, 0, 0]
    # Yaw:
    ypr[0] = math.degrees(math.atan2(2.0 * (q[0] * q[3] + q[1] * q[2]), 1. - 2. * (q[2] * q[2] + q[3] * q[3])))  # +90
    # Pitch:
    ypr[1] = math.degrees(math.asin(2.0 * (q[0] * q[2] - q[3] * q[1])))
    # Roll:
    ypr[2] = math.degrees(math.atan2(2.0 * (q[0] * q[1] + q[2] * q[3]), 1. - 2. * (q[1] * q[1] + q[2] * q[2])))
    for i in range(0, 3):
        if ypr[i] < 0.:
            ypr[i] += 360
        elif ypr[i] > 360:
            ypr[i] -= 360
    return ypr


def find_coeffs(pa, pb):
    matrix = []
    for p1, p2 in zip(pa, pb):
        matrix.append([p1[0], p1[1], 1, 0, 0, 0, -p2[0] * p1[0], -p2[0] * p1[1]])
        matrix.append([0, 0, 0, p1[0], p1[1], 1, -p2[1] * p1[0], -p2[1] * p1[1]])

    A = numpy.matrix(matrix, dtype=numpy.float)
    B = numpy.array(pb).reshape(8)

    res = numpy.dot(numpy.linalg.inv(A.T * A) * A.T, B)
    return numpy.array(res).reshape(8)


class PictureSupplier(InternalDB.mysql):
    def __init__(self):
        InternalDB.mysql.__init__(self, "PictureSupplier")
        self._camera_cfg = API.get_config("Instruments.Camera")
        global opengl
        if opengl:
            try:
                self.inqueue = Queue.Queue()
                self.outqueue = Queue.Queue()
                self._renderer = OpenGLRenderer(self.inqueue, self.outqueue)
                t = threading.Thread(target=self._renderer.run)
                t.start()
            except:
                self.log.exception("No OpenGL support")
                opengl = False

    def _resolve_params(self):
        query = "SELECT paramid from status_parameter,status_channel where status_parameter.chanid=status_channel.chanid AND status_channel.name=%s AND status_parameter.name=%s"
        if 0:  # IMU
            for i in ["lat", "lon", "alt", "hdg", "pitch", "roll", "yaw"]:
                cursor = self._execute(query, ["Instruments.IMU", i])
                self.params[i] = cursor.fetchone()[0]
        else:  # GPS
            for i in [("lat", "lat"), ("lon", "lon"), ("alt", "alt"), ("hdg", "yaw")]:
                cursor = self._execute(query, ["Instruments.GPS", i[0]])
                self.params[i[1]] = cursor.fetchone()[0]

    def get_rotation(self, img_id):
        data = {}
        if 1:
            # Use quaternions
            c = self._execute("SELECT timestamp, q0, q1, q2, q3, lat, lon, alt, roll, pitch, yaw FROM sample WHERE id=%s", [img_id])
            ts, q0, q1, q2, q3, lat, lon, alt, roll, pitch, yaw = c.fetchone()
            if not (roll and pitch and yaw):
                print("Calculating yaw pitch roll", [q0, q1, q2, q3])
                # Calculate based on quaternion values
                yaw, pitch, roll = quaternionToYawPitchRoll(Quaternion.normalize([q0, q1, q2, q3]))
            data["ts"] = ts
            data["roll"] = roll
            data["pitch"] = pitch
            data["yaw"] = yaw
            data["lat"] = lat
            data["lon"] = lon
            data["alt"] = alt
        else:
            c = self._execute("SELECT timestamp FROM sample WHERE id=%s", [img_id])
            ts = c.fetchone()[0]

            # Get the info
            sql = "SELECT value FROM status WHERE paramid=%s AND timestamp<%s ORDER BY timestamp DESC LIMIT 1"
            data = {"ts": ts}
            for i in list(self.params.keys()):
                data[i] = float(self._execute(sql, [self.params[i], ts]).fetchone()[0])

        # Get this info from somewhere - it's all about the camera
        # that was used - what can be determined from exif and what
        # must be "guessed" from the model and stuff
        sensorX = 22.2
        sensorY = 14.8
        focalLength = 32  # This was default set to 28, is that right?
        angleX = math.atan(sensorX / (2 * focalLength))
        angleY = math.atan(sensorY / (2 * focalLength))
        data["image_width"] = image_width = 2 * math.tan(angleX) * data["alt"]  # in meters
        data["image_height"] = image_height = 2 * math.tan(angleY) * data["alt"]  # in meters
        # We use degrees for all other stuff, so be consistent
        data["sensorAX"] = math.degrees(angleX)
        data["sensorAY"] = math.degrees(math.atan(sensorY / (2 * focalLength)))

        return data

    def get_images(self, min_timestamp, max_timestamp=None, instrumentid=None, limit=10):
        """
        Return a set of images
        """
        elems = ["id", "timestamp", "instrument", "path",
                 "q0", "q1", "q2", "q3",
                 "roll", "pitch", "yaw",
                 "lat", "lon", "alt"]

        params = [min_timestamp]
        SQL = "SELECT " + ",".join(elems) + " FROM sample WHERE timestamp>=%s "
        if max_timestamp:
            SQL += "AND timestamp<%s "
            params.append(max_timestamp)
        if instrumentid:
            SQL += "AND instrument=%s "
            params.append(instrumentid)
        SQL += "ORDER BY timestamp "
        if limit > 0:
            SQL += "LIMIT %d" % limit

        cursor = self._execute(SQL, params)

        ret = []
        for row in cursor.fetchall():
            info = {}
            for i in range(0, len(row)):
                if row[i] is not None:
                    info[elems[i]] = row[i]
            ret.append(info)
        return ret

    def get_image(self, timestamp, instrumentid=None):
        """
        Guess which image is correct for a given time
        """
        elems = ["id", "timestamp", "instrument", "path",
                 "q0", "q1", "q2", "q3", "roll", "pitch", "yaw",
                 "lat", "lon", "alt"]
        if not instrumentid:
            SQL = "SELECT " + ",".join(elems) + " FROM sample WHERE timestamp<=%s ORDER BY timestamp DESC LIMIT 1"
            cursor = self._execute(SQL, [timestamp])
        else:
            SQL = "SELECT " + ",".join(elems) + " FROM sample WHERE instrument=%s AND timestamp<=%s ORDER BY timestamp DESC LIMIT 1"
            cursor = self._execute(SQL, [instrumentid, timestamp])

        for row in cursor.fetchall():
            info = {}
            for i in range(0, len(row)):
                info[elems[i]] = row[i]
            return info

        raise Exception("No image found")

    def get_image_by_id(self, image_id):
        """
        Return info for the given image
        """
        elems = ["id", "timestamp", "instrument", "path",
                 "q0", "q1", "q2", "q3", "roll", "pitch", "yaw",
                 "lat", "lon", "alt"]
        SQL = "SELECT " + ",".join(elems) + " FROM sample WHERE id=%s"
        cursor = self._execute(SQL, [image_id])
        for row in cursor.fetchall():
            info = {}
            for i in range(0, len(row)):
                info[elems[i]] = row[i]
            return info

        raise Exception("No image found")

    def get_histogram(self, filepath):
        """
        Returns the histogram of a file
        """

        i = Image.open(filepath)
        i = i.convert("L")  # Convert to grayscale
        histogram = i.histogram()

        # In order to compress the histogram, we calculate the largest
        # value in the histogram and let that be represented by 255
        # (max single char value)
        max_value = 0
        for j in range(len(histogram)):
            max_value = max(max_value, histogram[j])
        factor = max_value / 255
        binary = struct.pack("!I", max_value)
        # factor allows us to compress the histogram evenly.  Flats out on 255
        for j in range(len(histogram)):
            binary += struct.pack("!c", chr(histogram[j] / factor))

        return binary

    def get_thumbnail(self, filepath, rotation=None):
        """
        Returns the thumbnail as data
        """
        if not os.path.exists(filepath):
            raise Exception("No such file '%s'" % filepath)

        version = 0
        try:
            version = pyexiv2.exiv2_version_info[1]
        except:
            pass

        try:
            if version >= 21:
                # No file needed
                image = pyexiv2.ImageMetadata(filepath)
                image.read()
                return {}, image.exif_thumbnail.data

            image = pyexiv2.Image(filepath)
            image.readMetadata()
            t, d = image.getThumbnailData()  # returns (type, data)
            # In order to support rotation, we need to actually load this image
            # if (rotation):
            #    d = self._rotate(d, rotation)
            return {}, d
        except:
            self.log.exception("Getting thumbnail info from %s" % filepath)
        return None

    def resample(self, filename, scale=1.0, quality=0.9, use_thumbnail=True,
                 crop_box=None, rotation=None, filetype="jpeg"):
        """
        Resample an image and return the data.  One or both of the parameters
        can be given. crop_box is a touple (left, top, right, bottom) pixels.

        """
        if rotation and opengl:
            # We use opengl for rotation, at least for now
            for i in ["yaw", "lat", "lon", "roll", "alt", "pitch"]:
                print(i, rotation[i])
            print()

            rotation["path"] = filename
            rotation["real_width"] = rotation["image_width"]
            rotation["real_height"] = rotation["image_height"]

            jobid = OpenGLRenderer.addJob(self.inqueue, rotation)
            result = OpenGLRenderer.getResult(self.outqueue, jobid, 5)
            if not result:
                raise Exception("Failed: No image within given time")
            if not result["result"].startswith("success"):
                raise Exception("Error rotating image: " + result["result"])
            print("ALT:", rotation["alt"])
            # We got the image, rotation and all - now we must
            # estimate the bounding box size and location
            inf = {}
            # Estimated size of a single pixel osize is physical size,
            # result is pixels
            if 0:
                osize = math.sqrt(math.pow(rotation["image_width"], 2) +
                                  math.pow(rotation["image_height"], 2))
                nsize = math.sqrt(math.pow(result["size"][0], 2) +
                                  math.pow(result["size"][1], 2))
                # Relative change (rough estimate unfortunately)
                guesstimate = osize / nsize

                # Altitude in pixels
                h = rotation["alt"] / guesstimate
                inf["bb_size"] = (result["size"][0] * guesstimate,
                                  result["size"][1] * guesstimate)

            yaw = rotation["yaw"]
            print("Crop: ", rotation["crop_box"], rotation["render_size"])
            r = math.radians((rotation["yaw"] + 90) % 360)
            try:
                # How much must we move the whole image due to roll
                r_roll = rotation["alt"] * math.tan(math.radians(rotation["roll"]))
                r_pitch = rotation["alt"] * math.tan(math.radians(rotation["pitch"]))
                d = math.sqrt(math.pow(r_roll, 2) + math.pow(r_pitch, 2))
                print("Calculate ajustment: rotation:", rotation["yaw"], "d:", d, "r roll,pitch:", (r_roll, r_pitch))

                # Skew in meters - negate this as we're actually
                # calculating where the new center of the image is on
                # the map
                s_lat = math.cos(r) * d
                s_lon = math.sin(r) * d
                if rotation["yaw"] <= 90:
                    s_lat = -s_lat
                elif rotation["yaw"] <= 180:
                    s_lon = -s_lon
                    pass
                elif rotation["yaw"] <= 270:
                    s_lat = s_lat
                    pass
                else:
                    pass

                print("Adjust by", (s_lat, s_lon))
            except:
                self.log.exception("Bummer: yaw:%.4f r:%.2f" % (rotation["yaw"], r))
            # Calculate the size of the bounding box - this is not
            # quite correct as the bb is larger than the image was (in size)
            bb_h = abs(result["image_width"] * math.cos(r)) + abs(result["image_height"] * math.sin(r))
            # We use width to keep aspect ratio - these are estimates anyway
            bb_w = abs(result["image_height"] * math.cos(r)) + abs(result["image_width"] * math.sin(r))
            print("Physical image size", result["image_width"], result["image_height"], "->", bb_h, bb_w)
            inf["bb_sw"] = to_lat_lon((rotation["lat"], rotation["lon"]),
                            s_lon - bb_w / 2., s_lat - bb_h / 2.)
            inf["bb_ne"] = to_lat_lon((rotation["lat"], rotation["lon"]),
                            s_lon + bb_w / 2., s_lat + bb_h / 2.)
            print(inf)
            # Physical size according to lat,lon
            print("Physical BB X:", get_distance_x_y(inf["bb_sw"], (inf["bb_sw"][0], inf["bb_ne"][1]))[0])
            print("Physical BB Y:", get_distance_x_y(inf["bb_sw"], (inf["bb_ne"][0], inf["bb_sw"][1]))[1])

            # Done
            return inf, result["img"]

        i = Image.open(filename)
        # i = i.transpose(Image.ROTATE_180)

        inf = {"orig_size": (i.size[0], i.size[1])}
        if scale != 1.0:
            if i.size[0] == 0 or i.size[1] == 0:
                self.log.warning("Image '%s' has size 0, ignoring" % filename)
                return None

            if not use_thumbnail:
                i = i.resize((max(64, int(i.size[0] * scale)), max(64, int(i.size[1] * scale))))
            else:
                i.thumbnail((max(64, int(i.size[0] * scale)), max(64, int(i.size[1] * scale))), resample=0)
        if (crop_box):
            crop_box = (min(max(crop_box[0], 0), i.size[0]),
                        min(max(crop_box[1], 0), i.size[1]),
                        max(min(crop_box[2], i.size[0]), 0),
                        max(min(crop_box[3], i.size[1]), 0))
            i = i.crop(crop_box)

        inf["new_size"] = (i.size[0], i.size[1])

        rotation = None
        if rotation:
            more_info, i = self._rotate(i, rotation)
            inf.update(more_info)

        # save it (to convert to JPEG)
        import io
        f = io.StringIO()
        i.save(f, format=filetype, quality=int(quality * 100), transparent=0)  # , optimize=True), progressive=True)
        inf["bb_size"] = (i.size[0], i.size[1])

        if (rotation):
            inf["px_size"] = rotation["image_width"] / inf["new_size"][0]

            # bounding-box size
            inf["bb_size"] = (i.size[0] * inf["px_size"],
                              i.size[1] * inf["px_size"])

            # Lat-lon top left and bottom right corners
            # Latitude: 1 deg = 110.54 km
            # Longitude: 1 deg = 111.320*cos(latitude) km
            # We must move the reference point as it is not the lat,lon of the plane but the center of the rotated image..  oh dear...
            yaw = rotation["yaw"]
            if yaw > 180:  # Keep yaw within -180,180
                yaw -= 360
            # If we're higher than 90 degrees absolute value, we must "invert"
            if abs(yaw) > 90:
                yaw += 90 if yaw < 0 else -90
            r = math.radians(yaw)

            try:
                d = rotation["height_px"] * math.tan(math.radians(rotation["roll"]))
                print("Calculate ajustment: yaw:", rotation["yaw"], "d:", d, "r:", r)
                lat = math.asin(r) * d / (2 * 110540.0)
                lon = math.acos(r) * d / (2 * 112320.0 * math.cos(math.radians(rotation["lat"])))
                print("Adjust by", (lat, lon))
            except:
                self.log.exception("Bummer: yaw:%.4f r:%.2f" % (rotation["yaw"], r))
                lat = 0
                lon = 0

            print("bb_size:", inf["bb_size"], lat, rotation)

            inf["bb_sw"] = (lat + rotation["lat"] - (inf["bb_size"][0] / (2 * 110540.0)),
                            lon + rotation["lon"] - (inf["bb_size"][1] / (2 * 112320.0 * math.cos(math.radians(rotation["lat"])))))
            inf["bb_ne"] = (lat + rotation["lat"] + (inf["bb_size"][0] / (2 * 110540.0)),
                            lon + rotation["lon"] + (inf["bb_size"][1] / (2 * 112320.0 * math.cos(math.radians(rotation["lat"])))))

        return inf, f.getvalue()

    def _rotate(self, img, rotation):
        print("Rotating image", rotation)
        # We try to rotate this better - first we transform the image according to the roll of the plane
        if rotation["roll"] > 180:
            r = math.radians(rotation["roll"] - 360)
        else:
            r = math.radians(rotation["roll"])
            ax = math.radians(rotation["sensorAX"])
            ay = math.radians(rotation["sensorAY"])
            # In order to rotate properly we must convert the heigt to the
            # equivalent in pixels, not meters - we luckily already have
            # the physical size of the picture as rotation["image_width"]
            h_m = rotation["alt"]
            pixel_size = rotation["image_width"] / img.size[0]
            h = h_m / float(pixel_size)
            rotation["height_px"] = h

            # To transform we need all four corners before and after
            # perspective change - the first corners are just the size of
            # the image, the other ones we need some math for.  Do only X for now
            # Center of image
            xc = img.size[0] / 2
            print("xc=", xc, "; r=", r, "; ax=", ax, "; h=", h)
        xl = 0  # int(xc - h * math.tan(ax - r))
        xr = int(h * (math.tan(ax + r) + math.tan(ax - r)))  # int(xc + h * math.tan(ax + r))
        print("xl: %d xr: %d" % (xl, xr))
        # xl = xc - h*2*math.tan(r)
        # xr = xc - h*math.tan(r) + h * math.tan(r+ax)
        # destination is top left, bottom left, bottom right, top right
        # Need to base these on the distance from the location of the plane
        dxl = h * math.tan(ax - r)
        dxr = dxl + xr  # img.size[0] - xr
        ytl = 0 - dxl * math.tan(ay)
        ybl = img.size[1] + dxl * math.tan(ay)
        ybr = img.size[1] + dxr * math.tan(ay)
        ytr = 0 - dxr * math.tan(ay)
        src = [(0, 0), (0, img.size[1]), (img.size[0], img.size[1]),
               (img.size[0], 0)]
        dst = [(xl, ytl), (xl, ybl), (xr, ybr), (xr, ytr)]
        print("DST:", dst)
        print("Image size:", img.size)

        # New image size - big enough for the whole thing
        sizex = max(img.size[0], xr) - xl
        txl = 0  # transposed postions
        txr = sizex

        sizey = int(max(img.size[1], max(ybl, ybr)))
        if (min(ytl, ytr) < 0):
            deltay = int(-min(ytl, ytr))
            sizey += deltay
        else:
            deltay = 0

        print("Image must be resized to", (sizex, sizey), " deltay:", deltay)
        # In order to get this right, I seem to have to create a new
        # image, put the old one inside it and then transform it -
        # transforming it in one operation seems to at least not work
        # with my current find_coeffs...  Probably could do this using
        # math, but at this point I'm just doing whatever really.
        # TODO: Check out OpenGL - this cannot possibly be anywhere
        # close to efficient
        new_i = Image.new("RGBA", (sizex, sizey))
        dx = (sizex - img.size[0]) / 2
        dy = (sizey - img.size[1]) / 2
        print(dx, dy, deltay)
        new_i.paste(img, (dx, dy))
        # We must change a bit around reflect the larger output image
        src = [(dx, dy), (dx, dy + img.size[1]),
               (dx + img.size[0], dy + img.size[1]),
               (dx + img.size[0], dy)]
        dst = [(txl, int(ytl + deltay)), (txl, int(ybl + deltay)),
               (txr, int(ybr + deltay)), (txr, int(ytr + deltay))]
        print("New src:", src)
        print("New dst:", dst)
        i2 = new_i.transform((sizex, sizey), Image.PERSPECTIVE,
                             find_coeffs(dst, src))
        # Rotate image based on yaw
        rot = i2.convert("RGBA").rotate(-rotation["yaw"], expand=True)
        # Calculate more info - image size, bounding box size and the
        # lat-lon of the bounding box (must compensate for perspective
        # change)
        additional = {}

        return additional, rot

if __name__ == "__main__":
    p = PictureSupplier()
    import sys
    if len(sys.argv) < 3:
        raise Exception("Missing args pich yaw rll")
    rotation = p.get_rotation(3108)

    rotation.update({
        "pitch": float(sys.argv[1]),
        "yaw": float(sys.argv[2]),
        "roll": float(sys.argv[3])})
    i = p.get_image_by_id(3108)
    rotation["lat"] = i["lat"]
    rotation["lon"] = i["lon"]
    rotation["alt"] = i["alt"]
    inf, data = p.resample(i["path"], scale=0.2, rotation=rotation, filetype="png")
    open("/tmp/test.png", "w").write(data)
    raise SystemExit(0)

    info = p.get_image(1347452203.9861712)
    print("INFO:", info)
    filename = info["path"]

    histogram = p.get_histogram(filename)
    print("Histogram: ", len(histogram))
    import time

    t = time.time()
    data = p.get_thumbnail(filename)
    print("Thumbnail:", time.time() - t, len(data), "bytes")
    open("/tmp/thumb.jpg", "w").write(data)

    t = time.time()
    data = p.resample(filename, scale=0.04, quality=0.5)
    print("Thumbnail:", time.time() - t, len(data), "bytes")
    open("/tmp/thumb_2.jpg", "w").write(data)

    t = time.time()
    data = p.resample(filename, quality=0.3)
    print("Q .3:", time.time() - t, len(data), "bytes")
    open("/tmp/low_q.jpg", "w").write(data)

    t = time.time()
    data = p.resample(filename, scale=0.5)
    print("Scale .5:", time.time() - t, len(data), "bytes")
    open("/tmp/half_thumb.jpg", "w").write(data)

    t = time.time()
    data = p.resample(filename, scale=0.5, use_thumbnail=False)
    print("Scale .5 nothumb:", time.time() - t, len(data), "bytes")
    open("/tmp/half.jpg", "w").write(data)

    t = time.time()
    data = p.resample(filename, scale=0.2, quality=0.3)
    print("Scale .2 Q .3:", time.time() - t, len(data), "bytes")
    open("/tmp/small.jpg", "w").write(data)
