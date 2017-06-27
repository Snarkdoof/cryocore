import time
import sys
import os
import os.path

if sys.version_info[0] == 3:
    import queue
else:
    import Queue as queue

from CryoCore import API, SampleDB

if len(sys.argv) == 1:
    raise SystemExit("Missing file or directory to add")

path = sys.argv[1]
if not os.path.exists(path):
    raise SystemExit("Missing source '%s'"%path)

try:
    _sample_queue = queue.Queue()
    _sample_db = SampleDB.SampleDB(_sample_queue, 
                                   API.api_stop_event)
    _sample_db.start()

    if os.path.isdir(path):
        for elem in os.listdir(path):
            if elem.count("_") != 2:
                continue

            f, ext = os.path.splitext(elem)
            instrument, stime, etime = f.split("_")
            if not instrument:
                instrument = 0
            # Estimate time offset
            sample = {"timestamp": stime,
                      "path": path + elem,
                      "instrumentid": instrument}
            _sample_queue.put(sample)

    else:
        ts = os.path.getctime(path)
        _sample_queue.put({"timestamp": ts,
                           "path": filename,
                           "instrumentid": 125})

    while not _sample_queue.empty():
        time.sleep(1.0)
finally:
    API.shutdown()

                       
                  
