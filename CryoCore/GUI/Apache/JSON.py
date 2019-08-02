import os
os.chdir(os.path.dirname(__file__))
import db
import json
from CryoCore import API
API.__is_direct = True

import time
cfg = API.get_config()
db = db.DBWrapper()


def shutdown(sure):
    if cfg["System.WebServer.allow_shutdown"] and sure:
        import subprocess
        try:
            subprocess.call(["sudo", "halt", "-p"])
            ret = {"result": "Shutting down"}
        except Exception as e:
            ret = {"error": e}
    else:
        ret = {"error": "Remote shutdown not allowed by config"}
    return json.dumps(ret)


def cfg_isupdated(req, since, ts=None):
    since = float(since)
    last_updated = cfg.last_updated()
    res = {"updated": last_updated > since + 0.00001, "last_updated": float(last_updated)}
    return json.dumps(res)


def cfg_get(req, param):
    res = cfg[param]
    return json.dumps({param: res})


def cfg_set(req, param, value, ts=None):
    error = ""
    try:
        if value.isdigit():
            val = int(value)
        elif value.replace(".", "").isdigit():
            val = float(value)
        else:
            val = value
    except Exception as e:
        val = value
        error = e

    err = None
    for i in range(3):
        try:
            cfg[param] = val
            err = None
            break
        except Exception as e:
            err = str(e)
            # time.sleep(0.250)

    if err:
        raise Exception(err)

    if error:
        return json.dumps({param: cfg[param], "error": error})
    return json.dumps({param: cfg[param]})


def cfg_versions(req):
    cfg = API.get_config()
    versions = cfg.list_versions()
    return json.dumps(versions)


def cfg_serialize(req, root=None, version=None, ts=None):
    if not version:
        version = "default"

    cfg = API.get_config(root, version=version)
    serialized = cfg.serialize()
    return serialized


def list_channels_and_params_full(req):
    channelsAndParams = {}
    for channel in db.get_db().get_channels():
        params = db.get_db().get_params_with_ids(channel)
        channelsAndParams[channel] = params
    return json.dumps({"channels": channelsAndParams})


def getmax(req, params, start, end, since=None, since2d=None, aggregate=None):
    params = json.loads(params)
    max_id, dataset = db.get_db().get_max_data(params, float(start), float(end), int(since), aggregate)
    return json.dumps({"max_id": max_id, "data": dataset})


def get(req, params, start, end, since=0, since2d=0, aggregate=None):
    params = json.loads(params)
    if int(since) == 0:
        get_last_values = True
    else:
        get_last_values = False
    max_id, max_id2d, dataset = db.get_db().get_data(params, float(start), float(end), int(since), int(since2d), aggregate, get_last_values)
    return json.dumps({"max_id": max_id, "max_id2d": max_id2d, "data": dataset, "glv": get_last_values})


# ## Also allow setting some values if we want to
def set(req, args):  # Values should be on the format [{channel: ..., "parameter": ..., values: [{"ts": timestamp, "value": ...}]}
    args = json.loads(args)

    for arg in args:
        if "channel" not in arg or "parameter" not in arg or "values" not in arg:
            raise Exception("Bad format, items need to have 'channel', 'parameter' and 'values' keys")
        status = API.get_status(arg["channel"])
        a = status[arg["parameter"]]
        for value in arg["values"]:
            if "value" not in value or "ts" not in value:
                raise Exception("Bad format, values must be a list of items with 'value' and 'ts' keys")

            a.set_value(value["value"], timestamp=value["ts"])

    return json.dumps({"result": "ok"})

# ## Logs ###

def log_search(req, keywords, lines=500, last_id=None, minlevel="DEBUG"):
    keywords = keywords.split(",")
    max_id, logs = db.get_db().log_search(keywords, lines, last_id, minlevel)
    return json.dumps({"max_id": max_id, "data": logs})


def log_getlist(req):
    modules, loggers = db.get_db().log_getlist()
    return json.dumps({"modules": modules, "loggers": loggers})


def log_getlines(req, last_id=None, start=None, end=None, modules="", loggers="", lines=500, minlevel="DEBUG"):
    modules = []
    loggers = []
    if modules:
        modules = modules.split(",")
    if loggers:
        loggers = loggers.split(",")
    max_id, logs = db.get_db().log_getlines(last_id, float(start), float(end), modules, loggers, lines, minlevel)
    return json.dumps({"max_id": max(int(last_id), max_id), "data": logs})


def log_getlevels(req):
    levels = db.get_db().get_log_levels()
    return json.dumps({"levels": levels})


def get_last_value(req, paramid, num_values=1):
    return json.dumps(db.get_db().get_last_value(paramid, num_values))
