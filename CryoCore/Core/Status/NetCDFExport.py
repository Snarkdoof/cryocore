"""
Export status measurements to NetCDF.
Requires python-netcdf
"""

import socket
import time
from Scientific.IO.NetCDF import NetCDFFile

from CryoCore.Core.Status.PostgresReporter import PostgresStatusReporter

from CryoCore.Core import API


class NetCDFExport(PostgresStatusReporter):

    def __init__(self, filename=None, channel=None, name=None):
        """
        Export status variables.
        If channel and/or name is given, only status updates
        that match will be dumped. If both is None, everything
        is dumped!
        if filename is not given, nothing can be saved, but queries
        can be done towards the database

        The format is: an unlimited dimension (c_time),
        the channel (e.g. the instrument, "Instrument.GPS"), as a string
        the name (the actual parameter, "lon"), as a string
        str_value, the value, if it is a string
        int_value, the value, if it is an integer
        double_value, the value, if it is a float or double.

        """
        PostgresStatusReporter.__init__(self)

        self._select_channel = channel
        self._select_name = name

        if not filename:
            return

        # Put in hostname and time for export
        self.target = NetCDFFile(filename, "w",
                                 "Created " + time.ctime() +
                                 " by " + socket.gethostname())

        title = "Status data dump"
        if channel:
            title += " of channel " + channel
        if name:
            title += " of variable " + name

        self.target.title = title
        self.target.version = 1

        # Only time dimension
        self.target.createDimension('time', None)
        self.target.createDimension('s128', 128)
        self.target.createDimension('s256', 256)

        # Variables
        self._timestamp = self.target.createVariable('c_time', 'd', ('time',))
        self._variables = {}

    def __del__(self):
        try:
            self.target.close()
        except:
            pass

    def get_channels(self):
        """
        Provide a list of all known channels
        """
        cursor = self._execute("SELECT DISTINCT(channel) FROM status")
        channels = []
        for row in cursor.fetchall():
            channels.append(row[0])
        return channels

    def get_parameters(self, channel):
        """
        Return a list of all parameters of a channel
        """
        # TODO: Escape
        cursor = self._execute("SELECT DISTINCT(name) FROM status WHERE channel='%s'", [channel])
        params = []
        for row in cursor.fetchall():
            params.append(row[0])
        return params

    def export_samples(self):
        """
        Export all values from the database to the netcdf file
        """

        SQL = "SELECT id, timestamp, channel, name, value FROM status"

        if self._select_channel or self._select_name:
            SQL += " WHERE "
            if self._select_channel:
                SQL += " channel='%s'" % self._select_channel
                if self._select_name:
                    SQL += " AND"
            if self._select_name:
                SQL += " name='%s'" % self._select_name

        cursor = self._get_db().cursor()
        cursor.execute(SQL)
        i = 0
        should_exit = False
        for (id, timestamp, channel, name, value) in cursor.fetchall():
            self._timestamp[i] = timestamp

            illegal_chars = {" ": "_",
                             "*": "x"}
            full_name = (channel + "." + name)
            for c in illegal_chars:
                full_name = full_name.replace(c, illegal_chars[c])

            val = cast(value)

            if full_name not in self._variables:
                print("Creating variable", full_name)
                if val.__class__ == float:
                    self._variables[full_name] = self.target.createVariable(full_name, 'd', ('time',))
                elif val.__class__ == int:
                    self._variables[full_name] = self.target.createVariable(full_name, 'i', ('time',))
                else:
                    self._variables[full_name] = self.target.createVariable(full_name, 'c',
                                                                            ('time', 's128',))
            try:
                if self._variables[full_name].typecode() in ["i", "d"]:
                    if val.__class__ == str:
                        if len(val) == 0:
                            val = 0
                        else:
                            print("Should be an integer or float, is a string '%s'" % val)
                    self._variables[full_name][i] = val
                else:
                    self._variables[full_name][i] = to_str(val, 128)
            except Exception as e:
                print("ERROR setting", full_name, "to", val, e)
                print("Typecode:", self._variables[full_name].typecode())
                print("Class:", value.__class__, "cast to", val.__class__)

            i += 1

        cursor.close()
        return i


def cast(string):
    """
    Convert the string, when this is possible, into basic types. These are:
    - C{int}
    - C{float}
    If the conversion was not successful the same I{string} is returned.
    @param string: string to be converted into one of the above enummerated types.
    @type string: C{string}
    @note: in the case of C{bool}, the string is converted into boolean if the string is in both capital o small letters.
    @return: the value of the string as: C{int}, C{float}, C{bool} if the conversion was right, or C{string} otherwise
    @rtype: C{int}, C{float} or C{string}
    """
    if string:
        if string.isdigit():
            try:
                return int(string)
            except:
                return string
        elif string.count('.') == 1:
            try:
                return float(string)
            except:
                return string
        elif string.lower() == "true":
            return True
        elif string.lower() == "false":
            return False
    return string


def to_str(value, length):
    if value is None:
        print("Warning: got value 'None'")
        return array(" " * length)

    from numpy import array
    val = list(value)
    if len(val) < length:
        val += " " * (length - len(val))
    return array(val)


def usage():
    import sys
    print("Usage:")
    print("   ", sys.argv[0], "<filename> [channel] [parameter]")
    print("   --list-channels")
    print("   --list-parameters <channel>")
    print()
    raise SystemExit("Missing parameters")

if __name__ == "__main__":

    try:
        import sys
        if len(sys.argv) < 2:
            usage()

        channel = None
        name = None

        if len(sys.argv) > 2:
            channel = sys.argv[2]

        if len(sys.argv) > 3:
            name = sys.argv[3]

        if sys.argv[1].startswith("--"):
            if sys.argv[1] == "--list-channels":
                n = NetCDFExport()
                print("Known channels:")
                for channel in n.get_channels():
                    print("   ", channel)

            elif sys.argv[1] == "--list-parameters":
                if channel is None:
                    print("Missing channel")
                    usage()
                else:
                    print("Known parameters for %s:" % channel)
                    n = NetCDFExport()
                    for param in n.get_parameters(channel):
                        print("   ", param)
            else:
                print("Bad command '%s'" % sys.argv[1])
                usage()
            raise SystemExit()

        n = NetCDFExport(sys.argv[1], channel, name)
        n.export_samples()
    finally:
        API.shutdown()
