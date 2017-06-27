import sys
import time

import sqlite3

ID = 0
TIMESTAMP = 1
LAT = 2
LON = 3
CHANNEL = 4
NAME = 5
VALUE = 6


class BasicDb:
    """

    Tail a status update database and dump the results as text.
    Allow some basic filtering too
    """

    def __init__(self, db_file, default_show=True):
        """
        default_show: should I print new messages by default
        """
        self.db_file = db_file
        self.conn = sqlite3.connect(self.db_file)
        self.filters = []
        self.default_show = default_show
        
    def _execute(self, SQL, params=[]):
        for i in range(0,3):
            try:
                
                cursor = self.conn.cursor()
                return cursor.execute(SQL, params)
            except sqlite3.OperationalError as e:
                if str(e) == "database is locked":
                    time.sleep(0.1) # Retry later
                else:
                    raise e

    def add_filter(self, filter):
        """
        Add a callback that will be showed or hidden - will be called
        for each new row.
        Must return True if the row should be printed,
        False if it should be hidden
        """
        self.filters.append(filter)

    def process(self, SQL, params=[]):
        """
        Go through all rows and run any filters.  Calls "process_row(row)"
        on all matches
        """

        last_id = 0
        rows = 0
        cursor = self._execute(SQL,params)

        for row in cursor.fetchall():
            rows += 1
            if row[ID] > last_id:
                last_id = row[ID]

            do_process = self.default_show

            # Any matches to hide it?
            for filter in self.filters:
                try:
                    if list(filter(row)):
                        do_process = True
                    else:
                        do_process = False
                    if do_process != self.default_show:
                        break # Found a match, stop now

                except Exception as e:
                    print("Exception executing filter "+str(filter)+":",e)

            if do_process:
                self.process_row(row)
            
class DBtoSVN(BasicDb):
    
    def __init__(self, db_file, svn_file, SQL, print_mode):
        """
        print mode: "by_channel", "by_
        """
        BasicDb.__init__(self, db_file)
        self.print_mode = print_mode
        self.target = open(svn_file, "w")

        self.keys = {}
        for item in [CHANNEL, NAME, VALUE]:
            self.keys[item] = []
        self.data = []
    
        self._start_time = time.time()

        self.process(SQL)

    def process_row(self, row):
        """
        print this to the svn file
        """
        if row[TIMESTAMP] < self._start_time:
            self._start_time = row[TIMESTAMP]
        for item in [CHANNEL, NAME, VALUE]:
            if not row[item] in self.keys[item]:
                if item == CHANNEL:
                    print("Found new channel",row[item])
                self.keys[item].append(row[item])
        self.data.append(row)
            
    def finalize(self):
        """
        Write the actual file
        """
        if self.print_mode == "by_channel":
            # The channels will have separate columns
            self.target.write(",".join(["Time"] + self.keys[CHANNEL]) + "\n")
            channel = {}
            for i in range(0,len(self.keys[CHANNEL])):
                channel[self.keys[CHANNEL][i]] = i
                print("Channel",self.keys[CHANNEL][i],"=",i)

            for row in self.data:
                # Sort on the channel
                line = [str(row[TIMESTAMP]-self._start_time)]
                col_num = channel[row[CHANNEL]]
                line += [""]*col_num
                line += [str(row[VALUE])]
                
                self.target.write(",".join(line) + "\n")
        else:
            raise Exception("Not supported mode %s"%self.print_mode)
        
if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("""Usage:
    %s <database file> <svn file> <print_mode> [expression] [exceptions?] ...
    Exceptions are not yet implemented. ;)
"""%sys.argv[0])
        raise SystemExit("Need at least a database file, an svn file and one parameter")
    
    db_file = sys.argv[1]
    svn_file = sys.argv[2]
    print_mode = sys.argv[3]
    if len(sys.argv) > 4:
        SQL ="SELECT * FROM status WHERE %s"%sys.argv[4]
    else:
        SQL ="SELECT * FROM status"
        
    print(SQL)
    #exceptions = sys.argv[2:]
    #print "Exceptions:",exceptions
    
    db = DBtoSVN(db_file, svn_file, SQL, print_mode)
    db.finalize()
    
