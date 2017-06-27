"""
Small program to handle cryocore data
It can dump, export and clear databases, and perform
full transfers of all data
"""
import sys
import os
import os.path
import subprocess
import time

from optparse import OptionParser

from CryoCore import API
from CryoCore.Core import InternalDB

usage = """usage: %prog [options] [command]
  Commands:
    export <item1> [item2] ... - export data (as NetCDF)
    dump  <item1> [item2] ...  - dump data (native dumps)
    clear <item1> [item2] ...  - clear data
    restore <item1> [item2] ... - restore a datadump
    repair - Repair all tables (if possible)
    """

parser = OptionParser(usage=usage)
parser.add_option("-d", "--destination", dest="destination",
                  help="Target directory for export/dumps")

parser.add_option("-s", "--source", dest="source",
                  help="Source directory for restoring dumps")

parser.add_option("-v", "--verbose", action="store_true", default=False,
                  help="Verbose mode - say more about what's going on")

parser.add_option("-c", "--compress", action="store_true", default=False,
                  help="Compress database files after export")

parser.add_option("", "--clear", action="store_true", default=False,
                  help="Clear the selected items after successful export")

parser.add_option("", "--yes", action="store_true", default=False,
                  help="Always answer 'yes'. THIS IS DANGEROUS!")

parser.add_option("-u", "--user", dest="db_user",
                  help="User to run postgres as",
                  default="pilot")


def _usage():
    parser.print_help()
    raise SystemExit()


def check_destination(destination):
    """
    Check that the given destination exists and is writeable
    """
    if not os.path.exists(destination):
        os.makedirs(destination)


def export_databases(options, items):
    """
    Export data (as NetCDF)
    """
    # Possible for export:
    supported_items = ["imu", "gps", "trios", "metpack"]
    if "all" in items:
        items = supported_items

    for item in items:
        item = item.lower()
        if item not in supported_items:
            raise Exception("Can't export '%s', only %s and 'all' is supported" %
                                (item, supported_items))
        print("Exporting", item)

        target = os.path.join(options.destination, item + ".nc")
        if os.path.exists(target):
            raise Exception("File %s already exists" % target)
        if options.verbose:
            print("Exporting to", target)
        if item == "trios":
            from Instruments.TriOS.NetCDFExport import NetCDFExport
            e = NetCDFExport(target)
        elif item == "imu":
            from Instruments.IMU.NetCDFExport import NetCDFExport
            e = NetCDFExport(target)
        elif item == "gps":
            from CryoCore.Status.NetCDFExport import NetCDFExport
            e = NetCDFExport(target, "Instruments.GPS")
        elif item == "metpack":
            from CryoCore.Status.NetCDFExport import NetCDFExport
            e = NetCDFExport(target, "Instruments.MetPack")
        else:
            print("Don't support exporting", item)
            continue

        num = e.export_samples()
        if options.verbose:
            print("Exported", num, "samples")

        if options.compress:
            compress(options, target)

        if item not in ["gps"]:
            if options.clear:
                print("Clearing database for", item)
                clear_databases(options, [item])


def compress(options, filename):
    if options.verbose:
        print("Compressing", filename)
    retval = subprocess.call(["gzip", filename])
    if retval != 0:
        print("Failed to compress", filename)


def dump_databases(options, items):
    """
    Perform SQL dumps
    """
    # Just dump all
    supported_items = ["db"]
    if 0:
        supported_items = ["status", "log", "imu", "trios", "laser"]
        if "all" in items:
            items = supported_items
        for item in items:
            item = item.lower()

            if item not in supported_items:
                raise Exception("Can't dump '%s', only %s or 'all' is supported" %
                                    (item, supported_items))

    print("Exporting database")

    target = os.path.join(options.destination, "cryocore.sql")
    if os.path.exists(target):
        raise Exception("File %s already exists" % target)

    if options.verbose:
        print("Exporting to", target)

    # database name should be item + "db"
    cfg = API.get_config("System.InternalDB")
    t = open(target, "w")
    p = subprocess.Popen(["mysqldump",
                          "-u", cfg["db_user"],
                          "-p%s" % cfg["db_password"],
                          cfg["db_name"]], stdout=t)
    while p.poll() is None:
        time.sleep(0.2)
    retval = p.poll()
    if retval != 0:
        print(" *** Error dumping database return value", retval)
        return

    if options.compress:
        compress(options, target)

    if options.clear:
        print("Clearing database for", item)
        clear_databases(options, [item])


def restore_databases(options, items):
    """
    Perform SQL dumps
    """
    supported_items = ["db"]
    # supported_items = ["status", "log", "imu", "trios", "laser"]
    if "all" in items:
        items = supported_items
    for item in items:
        item = item.lower()
        if item not in supported_items:
            raise Exception("Can't restore '%s', only %s or 'all' is supported" %
                            (item, supported_items))

        print("Restoring", item)
        if item == "db":
            item = "cryocore"
        source = None

        if os.path.isfile(options.source):
            source = options.source
        else:
            for ext in [".sql", ".sql.gz", ".sql.bz2"]:
                source = os.path.join(options.source, item + ext)
                if os.path.exists(source):
                    break
                source = None

        if not source:
            print("Could not find database dump to import for", item)
            continue

        if options.verbose:
            print("Restoring from", source)

        # database name should be item + "db"
        db = item + "db"

        if source[-4:] == ".bz2":
            import bz2
            sf = bz2.BZ2File(source)
        elif source[-3:] == ".gz":
            import gzip
            sf = gzip.GzipFile(source)
        else:
            sf = open(source, "r")

        # TODO: CLEAR THE DATABASE FIRST?
        print("TIP: Did you clear the database first?")

        cfg = API.get_config("System.InternalDB")
        user = cfg["db_user"]
        if not user:
            user = input("Database user:").strip()
        passwd = cfg["db_password"]
        db_name = cfg["db_name"]
        if not db_name:
            db_name = "cryocore"
        if not passwd:
            passwd = input("Database password:").strip()

        p = subprocess.Popen(["mysql",
                              "-u", user,
                              "-p%s" % passwd,
                              db_name],
                             stdin=subprocess.PIPE)

        # Dump the file to the database
        while True:
            buf = sf.read(10240)
            if len(buf) == 0:
                break
            p.stdin.write(buf)
        p.stdin.close()

        if options.verbose:
            print("Waiting for DB do complete")

        while p.poll() is None:
            time.sleep(0.2)

        retval = p.poll()
        if retval != 0:
            print(" *** Error restoring", item, " return value", retval)
            continue

        print("Restore completed")


def _should_delete(options, item):
    if options.yes:
        return True

    print("Are you sure you want to DELETE ALL DATA from %s (yes/no)?" % item)
    answer = None
    while True:
        answer = raw_input().lower()
        if answer not in ["yes", "no"]:
            print("Please answer 'yes' or 'no'")
            continue
        break
    if answer == "no":
        if options.verbose:
            print("Not clearing", item)
        return False
    return True


def clear_databases(options, items):
    """
    Clear the given databases
    """

    supported_items = ["status", "log", "imu", "trios", "laser", "sample", "arduimu"]
    if "all" in items:
        items = supported_items
    for item in items:
        item = item.lower()

        if item not in supported_items:
            raise Exception("Can't clear '%s', only %s or 'all' is supported" %
                                (item, supported_items))

        if not _should_delete(options, item):
            continue

        # CLEAR DATABASE
        # Get the correct config for the databas
        configs = {"status": "System.Status.Postgres",
                   "imu": "Instruments.IMU",
                   "trios": "Instruments.Trios",
                   "log": "System.LogDB"}
        cfg = API.get_config("System.InternalDB")
        db = InternalDB.mysql("ManageData", cfg)
        # Should be ready to clear the tables...
        sqls = {"status": ["truncate table status",
                           "delete from status_parameter",
                           "delete from status_channel"],
                "imu": ["delete from imu"],
                "trios": ["delete from instrument",
                          "truncate table sample "],
                "log": ["truncate table log"],
                "laser": ["truncate table laser",
                "truncate table laserscanner"],
                "arduimu": ["truncate table arduimu"],
                "sample": ["delete from sample"]}
        try:
            for SQL in sqls[item]:
                db._execute(SQL)
        except Exception as e:
            print(" *** Error clearing %s: %s" % (item, e))
            continue

        if options.verbose:
            print("Cleared database", item)


def copy_images(options, items):
    """
    Export images
    """
    # TODO: Do something about items here
    if len(items) > 0:
        raise Exception("Image export got some items (%s), don't know what to do about that" % str(items))

    # Get image directory
    cfg = API.get_config("Instruments.Camera")
    source = cfg["picture_directory"]
    if not os.path.exists(source):
        raise Exception("Can't export images from '%s', it doesn't exist!" %
                            source)

    target = os.path.join(options.destination, "images")
    print("Exporting images from", source, "to", target)

    proc = subprocess.call(["cp", "-p", source, target])
    if proc != 0:
        print(" *** Error copying files from", source, "to", target)
        return

    if options.clear:
        delete_images(options, source)


def delete_images(options, from_where=None):
    """
    Delete all images.
    THIS IS DANGEROUS AND REQUIRES THAT THE CAMERA DOES NOT RUN!
    """

    if not _should_delete(options, "images"):
        print("NOT deleting images")
        return

    # Get image directory
    cfg = API.get_config("Instruments.Camera")
    source = cfg["picture_directory"]
    if not os.path.exists(source):
        raise Exception("Can't delete images from '%s', it doesn't exist!" % source)

    if not _should_delete(options, source):
        return

    proc = subprocess.call(["rm", "-r", source])
    if proc != 0:
        print(" *** Error deleting files from", source)
        return


def repair():
    db = InternalDB.mysql("repair", can_log=False)
    c = db._execute("SHOW TABLES")
    for row in c.fetchall():
        c = db._execute("REPAIR TABLE " + row[0])
        print(c.fetchone())

if __name__ == "__main__":
    if len(sys.argv) == 1:
        _usage()

    (options, args) = parser.parse_args()

    if args[0] in ["export", "dump"] and not options.destination:
        raise SystemExit("Missing destination")

    if args[0] in ["restore"] and not options.source:
        raise SystemExit("Missing source")

    try:
        if options.destination:
            check_destination(options.destination)

        supported_ops = ["export", "dump", "clear", "restore", "repair"]
        op = args[0]
        if op not in supported_ops:
            raise SystemExit("Bad command '%s', must be one of %s" %
                                 (op, str(supported_ops)))
        if op == "repair":
            repair()
            raise SystemExit(0)

        items = args[1:]
        if len(items) == 0:
            if op in ["dump", "restore"]:
                items = ["db"]
            else:
                items = ["nothing"]

        if len(items) == 0:
            raise SystemExit("Need something to %s" % op)

        if 0 and op != "restore":  # No idea what this does, removed it for now
            if "images" in items or "all" in items:
                if "images" in items:
                    items.remove("images")

                # perform op on images
                if op in ["export", "dump"]:
                    copy_images(options, [])
                elif op == "clear":
                    try:
                        delete_images(options)
                    except Exception as e:
                        print("Could not remove images:", e)

        if len(items) > 0:
            if op == "export":
                export_databases(options, items)
            elif op == "dump":
                dump_databases(options, items)
            elif op == "clear":
                clear_databases(options, items)
            elif op == "restore":
                restore_databases(options, items)
    finally:
        API.shutdown()
        time.sleep(2)
