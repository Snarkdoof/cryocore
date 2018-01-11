
import db
import json
from CryoCore import API

import os
os.chdir(os.path.dirname(__file__))

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


def cfg_isupdated(req, since):
    since = float(since)
    last_updated = cfg.last_updated()
    res = {"updated": last_updated > since + 0.00001, "last_updated": float(last_updated)}
    return json.dumps(res)


def cfg_get(req, param):
    res = cfg[param]
    return json.dumps({param: res})


def cfg_set(req, param, value):
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
    cfg[param] = val

    if error:
        return json.dumps({param: cfg[param], "error": error})
    return json.dumps({param: cfg[param]})


def cfg_versions(req):
    cfg = API.get_config()
    versions = cfg.list_versions()
    return json.dumps(versions)


def cfg_serialize(req, root=None, version=None):
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
