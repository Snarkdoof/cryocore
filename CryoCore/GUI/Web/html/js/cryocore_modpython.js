var CryoCore = function(_CRYOCORE_) {

  var SERVER = "https://cryosql.itek.norut.no/";

  var XHR = (function() {
    var api = {};

    api.get = function(url, params, onResult, onError) {
      var xhr = new XMLHttpRequest();
      url += "?";
      for (var i in params) {
        url += i + "=" + params[i] + "&";
      }
      xhr.open("GET", url, true);
      xhr.onreadystatechange = function() {
        if (xhr.readyState == 4) {
          try {
            onResult(JSON.parse(xhr.response));
          } catch (err) {
            console.log("Bad data:", err, xhr.response);
            if (onError) {
               onError(err);
            }
          }
        }
        // TODO: if error, call onError
      };
      try {
        xhr.send();
      } catch (err) {
        console.log("Error refreshing", err);
        if (onError) {
          onError(err);
        }
      }
    };
    return api;
  })();

  var Config = function(root, onReadyCB, options) {
    root = root || "";
    var last_config_update = 0;
    var params = {};
    var nodes = {};
    var node_colors = {};
    var callbacks = [];
    var ready = false;
    var readycbs = [];
    options = options || {};

    var is_refreshing = false;
    var refresh = function(func) {
      if (is_refreshing) return;
      is_refreshing = true;
      XHR.get(SERVER + "/JSON.py/cfg_isupdated", {
          "since": last_config_update,
          "ts": new Date()/1000
        },
        function(data) {
          is_refreshing = false;
          last_config_update = data.last_updated;
          if (data.updated) {
            reload(func);
          }
        },
        function() {
          // Error
          is_refreshing = false;
        });
    };

    var _do_ready_cb = function() {
      if (!ready) return;
      while (readycbs.length > 0) {
        try {
          readycbs.shift().call(this);
        } catch (err) {
          console.log("Error in callback");
          console.log(err);
        }
      }
    };

    var onReady = function(func) {
      readycbs.push(func);
      if (ready) {
        _do_ready_cb();
      }
    };

    var reload = function(cb) {
      XHR.get(SERVER + "/JSON.py/cfg_serialize", {
          "root": root,
          "ts": new Date()/1000
        },
        function(data) {
          if (root) {
            params = data[root];
          } else {
            params = data.root;
          }
          ready = true;
          if (cb) {
            cb(this);
          }
          if (callbacks) {
            for (var i = 0; i < callbacks.length; i++) {
              try {
                callbacks[i]();
              } catch (err) {
                console.log(err);
              }
            }
          }
        });
    };

    var set = function(param, value, opt) {
      XHR.get(SERVER + "/JSON.py/cfg_set", {
          "param": param,
          "value": value
        },
        function(res) {
          refresh(options.onchanged);
        });
    };

    var get = function(param, def) {
      var frags = param.split(".");
      var p = params;
      for (var idx in frags) {
        if (!frags.hasOwnProperty(idx)) {
          continue;
        }

        if (!frags.hasOwnProperty(idx)) {
          continue;
        }
        if (p[frags[idx]]) {
          p = p[frags[idx]];
        } else {
          return def;
        }
      }
      if (p.value !== undefined) {
        return p.value;
      }
      return p;
    };

    var addCallback = function(callback) {
      callbacks.push(callback);
    };

    var removeCallback = function(callback) {
      for (var i = 0; i < callbacks.length; i++) {
        if (callbacks[i] == callback) {
          return callbacks.slice(i, i + 1);
        }
      }
    };

    // Use timing object if available
    if (options.timingObject) {
      TIMINGSRC.setInrvalCallback(options.timingObject, update, options.refresh/1000);
    } else {
      setInterval(refresh, 5000); // Auto update 5 seconds          
    }
    refresh(_do_ready_cb);

    var self = {};
    self.set = set;
    self.get = get;
    self.isReady = function() {
      return ready;
    };

    self.onReady = onReady;

    self.getParams = function() {
      return params;
    };

    self.addCallback = addCallback;
    self.removeCallback = removeCallback;

    return self;
  };

  var Dataset = function(options) {
    var monitored = {};
    var monitored_keys = [];
    var historical_data = {};
    var update_funcs = {};
    var paramInfo = {};
    var last_update = 0;
    var last_id = 0;
    var last_id2d = 0;
    var outstanding = 0; // Ensure that we don't hammer server
    var _snr = 1;  // used for sequencer things
    options = options || {};
    options.window_size = options.window_size || 1800; // 30 minutes
    options.history_size = options.history_size || 2 * 3600; // 2 hours
    options.refresh = options.refresh || 5000; // 1 second

    if (options.timingObject === undefined) {
      options.timingObject = new TIMINGSRC.TimingObject();
      options.timingObject.update({position: new Date()/1000, velocity: 1});
    }

    if (options.timingObject) {
      options.timingObject.on("change", function() {
        last_id = 0;
        last_id2d = 0;
        //_cleanHistorical(true);
      });
    }

    if (options.minTimingObject === undefined) {
      options.minTimingObject = new TIMINGSRC.SkewConverter(options.timingObject, -options.window_size);
    }

    // We use a sequencer for the data
    // var sequencer = new TIMINGSRC.Sequencer(options.minTimingObject, options.timingObject);

    var init = function() {};

    var loadParameters = function(onComplete) {
      XHR.get(SERVER + "/JSON.py/list_channels_and_params_full/", {},
        function(data) {
          channels = data.channels;

          for (var channel in channels) {
            if (!channels.hasOwnProperty(channel)) {
              continue;
            }

            var children = [];
            for (var i in channels[channel]) {
              if (!channels[channel].hasOwnProperty(i)) {
                continue;
              }

              var param = channels[channel][i];
              if (param.length > 2) {
                paramInfo[param[0]] = {
                  "channel": channel,
                  "param": param[1],
                  "size": param[2]
                };
              } else {
                paramInfo[param[0]] = {
                  "channel": channel,
                  "param": param[1]
                };
              }
            }
          }
          if (onComplete) {
            onComplete();
          }
        });
    };

    var resolveParams = function(what, exact) {
      var retval = [];
      for (var key in paramInfo) {
        if (!paramInfo.hasOwnProperty(key)) {
          continue;
        }

        if (typeof(what) == "string") {
          if (exact) {
            if (paramInfo[key].param.toLowerCase() === what.toLowerCase()) {
              retval.push(key);
            }
          } else if (paramInfo[key].param.toLowerCase().indexOf(what.toLowerCase()) === 0) {
            retval.push(key);
          }
        } else { // We got a map
          var found = true;
          for (var k in what) {
            if (!what.hasOwnProperty(k)) {
              continue;
            }
            if (paramInfo[key][k]) {
              if (exact) {
                if (paramInfo[key][k].toLowerCase() != what[k].toLowerCase()) {
                  found = false;
                }
              } else if (paramInfo[key][k].toLowerCase().indexOf(what[k].toLowerCase()) !== 0) {
                // if (paramInfo[key][k].toLowerCase() != what[k].toLowerCase()) {
                found = false;
              }
            }
          }
          if (found) retval.push(key);
        }
      }
      return retval;
    };

    var monitorParam = function(what, callback, onComplete) {
      var added = [];

      for (var channel in channels) {
        if (!channels.hasOwnProperty(channel)) {
          continue;
        }
        var children = [];
        for (var i in channels[channel]) {
          if (!channels[channel].hasOwnProperty(i)) {
            continue;
          }
          var param = channels[channel][i];
          if (what.indexOf(param[1]) > -1) {
            addMonitor([param[0]], callback);
            added.push(param[0]);
          }
        }
      }
      if (onComplete) {
        onComplete(added);
      }
    };

    /** Create and ID capable string by removing illegal characters */
    function _to_id(s) {
      return s.replace(" ", "").replace(".", "");
    }

    var addMonitor = function(params, callback, onSuccess) {
      if (callback === undefined || callback.call === undefined) {
        console.log("Warning: adding callback function that isn't a function for parameters", params);
      }
      for (var i = 0; i < params.length; i++) {
        if (params[i] === null || !params[i]) {
          continue;
        }
        if (monitored_keys.indexOf(params[i]) == -1) {
          monitored_keys.push(params[i]);
        }
        if (!monitored[params[i]]) {
          monitored[params[i]] = [];
        }
        monitored[params[i]].push(callback);
      }
      if (onSuccess) {
        onSuccess(params);
      }
    };

    var clearMonitors = function(param_id) {
      if (!monitored[param_id]) {
        console.log("Not monitored");
        return;
      }

      var idx = monitored_keys.indexOf(param_id);
      if (idx > -1) {
        monitored_keys.splice(idx, 1);
      }
      delete monitored[param_id];
    };

    var removeMonitor = function(param_id, callback) {
      if (!monitored[param_id]) {
        console.log("Not monitored");
        return;
      }
      var idx = monitored[param_id].indexOf(callback);
      if (idx == -1) {
        console.log("Can't find the given callback");
        console.log(monitored[param_id]);
        console.log(callback);
        return;
      }
      monitored[param_id].splice(idx, 1);

      /* Go through the monitored list and ensure that the keys are OK */
      k = [];
      for (var key in monitored) {
        if (!monitored.hasOwnProperty(key)) {
          continue;
        }
        if (k.indexOf(key) == -1) {
          k.push(key);
        }
      }
      monitored_keys = k;
    };

    var sequenceMonitor = function(params, sequencer) {
      if (! sequencer || !sequencer._toA) {
        throw new Error("Need a sequencer with two timing objects");
      }

      // Automatically clean up too old stuff
      sequencer.on("remove", function(e) {
        sequencer.removeCue(e.key);
      });

      sequencer._toA.on("change", function() {
        // Fill with historic data
        directLoad(params, sequencer._toA.pos, sequencer._toB.pos, null, function(data) {
          for (var key in data) {
            if (!data.hasOwnProperty(key)) continue;
            for (var i=0; i<data[key].length; i++) {
              var item = data[key][i];
              sequencer.addCue(String(_snr++), new TIMINGSRC.Interval(item[0], item[0]), {key: key, data: item});
            }
          }
        });        
      });

      // Monitor this from now on
      addMonitor(params, function(key, data) {
        for (var i=0; i<data.length; i++) {
          var item = data[i];
          sequencer.addCue(String(_snr++), new TIMINGSRC.Interval(item[0], item[0]), {key: key, data:item});
        }
      });
    };

    var update = function() {
      if (monitored_keys.length === 0) {
        return; // No reason to update from server when no parameters are added
      }
      if (outstanding > 0) return;

      var now;
      now = options.timingObject.pos;
      var lower_limit = options.minTimingObject.pos;
      var upper_limit = now;
      if (monitored_keys === null || monitored_keys === undefined) {
        throw Exception("Update without keys");
      }
      var p = {
        params: JSON.stringify(monitored_keys),
        start: lower_limit, //Math.max(last_update, lower_limit),
        end: upper_limit,
        since: last_id,
        since2d: last_id2d
      };
      last_update = now - 30; // We allow 30 seconds for data to propagate 
      outstanding += 1;
      var success = function(res) {
        outstanding = 0;
        var the_data = {};
        last_id = res.max_id;
        last_id2d = res.max_id2d;
        _store(res.data);
        /* Execute callbacks now that we have the data ready */
        for (var key in res.data) {
          if (!res.data.hasOwnProperty(key)) {
            continue;
          }
          for (var idx in monitored[key]) {
            if (!monitored[key].hasOwnProperty(idx)) {
              continue;
            }
            if (monitored[key][idx]) {
              if (key === "51") { console.log("Monitoring 51")}
              try {
                monitored[key][idx](key, res.data[key]);
              } catch (err) {
                console.log("Error in callback for " + key + ":", monitored[key][idx]);
                console.log(err);
              }
            }
          }
        }
      };
      XHR.get(SERVER + "/JSON.py/get", p, success, function(err) {
        outstanding = 0;
        try {
          var rt = err.responseText.replace(/nan/gi, null);
          var data = JSON.parse(rt);
          success(data);
        } catch (err2) {
          console.log("Bad response: " + rt + ": " + err2);
        }
      });
    };

    var _store = function(data) {
      for (var key in data) {
        if (!data.hasOwnProperty(key)) {
          continue;
        }

        /* Add to historical data */
        if (!historical_data[key]) {
          historical_data[key] = data[key];
        } else {
          // Max ts of what I have
          var _max_current = historical_data[key][historical_data[key].length - 1][0];
          // Min ts of what is new
          var _min_new = data[key][0][0];

          // Remove newer data for this instrument (it's most likely a prognosis)
          if (_min_new < _max_current) {
            for (var i = 0; i < historical_data[key].length; i++) {
              if (historical_data[key][i][0] >= _min_new) {
                // Remove from here out and continue to merge the datasets
                historical_data[key].splice(i - 1, historical_data[key].length - i);
                break;
              }
            }
          }

          // Index them
          var index = {};
          for (var j = 0; j < historical_data[key].length; j++) {
            index[historical_data[key][j]] = true;
          }
          for (var k = 0; k < data[key].length; k++) {
            var item = data[key][k];
            if (index[item]) {
              continue; // Duplicate
            }
            historical_data[key].push(item);
          }
          //historical_data[key] = historical_data[key].concat(data[key]);
        }

        historical_data[key].sort(function(a, b) {
          return a[0] - b[0];
        });
      }
      _cleanHistorical();
    };

    var _cleanHistorical = function(cleanFuture) {
      var now;
      now = options.timingObject.pos;
      var cutoff_time = now - options.history_size;
      var i = 0;
      for (var key in historical_data) {
        if (!historical_data.hasOwnProperty(key)) {
          continue;
        }
        // Search each key and delete older data
        var j;
        for (j = 0; j < historical_data[key].length; j++) {
          if (historical_data[key][j] > cutoff_time) {
            /* We cut the array here, then break */
            if (j > 0) {
              historical_data[key] = historical_data[key].splice(j, historical_data[key].length - j);
            }
            break;
          }
        }
        // Delete too new data if specified
        if (cleanFuture) {
          for (i = historical_data[key].length; i > 0; i--) {
            if (historical_data[key][i] < now) {
              console.log("Cleaning future data for", key);
              /* We cut the array here, then break */
              historical_data[key] = historical_data[key].splice(0, i);
              break;
            }
          }
        }
      }
    };

    var getParamInfo = function(paramid) {
      return paramInfo[paramid];
    };

    var directLoad = function(params, startts, endts, aggregate, onComplete, getmax) {
      if (params.length === 0) {
        console.log("Warning: directLoad called with no parameters");
        return;
      }
      var p = {
        "params": JSON.stringify(params),
        "start": startts,
        "end": endts,
        "since": 1
      };
      if (aggregate) {
        p.aggregate = aggregate;
      }
      /* Load directly and call back */
      var success = function(res) {
        if (!getmax) {
          _store(res.data);
        }
        onComplete(res.data);
      };

      var url = "/JSON.py/get";
      if (getmax) {
        url = "/JSON.py/getmax";
      }
      XHR.get(SERVER + url, p, success,  
        function(err) {
          outstanding = 0;
          var rt = err.responseText.replace(/nan/gi, null);
          try {
            console.log("Warning: Bad data, trying to fix");
            var data = JSON.parse(rt);
            console.log(data);
            success(data);
          } catch (err2) {
            console.log("Bad response: " + rt + ": " + err2);
          }
        }
      );
    };

    var getData = function(paramid, startts, endts) {
      if (!historical_data[paramid]) return [];

      var start_idx = 0;
      for (var idx = 0; idx < historical_data[paramid].length; idx++) {
        if (historical_data[paramid][idx][0] < startts) {
          start_idx = idx + 1;
          continue;
        }
        if (endts && historical_data[paramid][idx][0] > endts)
          return historical_data[paramid].slice(start_idx, idx + 1);
      }

      return historical_data[paramid].slice(start_idx);
    };

    var getLastValue = function(paramid, def) {
      if (!historical_data[paramid]) return def;
      return historical_data[paramid][historical_data[paramid].length - 1][1];
    };

    var getLastValueTs = function(paramid, def) {
      if (!historical_data[paramid]) return def;
      return historical_data[paramid][historical_data[paramid].length - 1];
    };

    var findValue = function(paramid, ts, maxdiff) {
      // Get the last entry BEFORE this timestamp - If maxdiff is given, returns null if we're too far away
      if (!historical_data[paramid]) {
        //console.log("Warning: request for value for unknown parameter", paramid);
        return [null, null];
      }
      var val = [0, null];
      var best_idx = -1;
      for (var i = 0; i < historical_data[paramid].length; i++) {
        if (historical_data[paramid][i][0] > ts) {
          break;
        }
        best_idx = i;
      }
      if (best_idx == -1) {
        return [null, null];
      }
      val = historical_data[paramid][best_idx];
      if (val === undefined) {
        return [null, null];
      }
      var next_val = historical_data[paramid][i + 1] || null;

      if (ts - val[0] > maxdiff) {
        return [null, null];
      }
      return [val, next_val];
    };

    var getValue = function(paramid, ts, maxdiff) {
      var val = findValue(paramid, ts, maxdiff)[0];
      if (val !== null) {
        return val[1];
      }
      return null;
    };

    var getValueTs = function(paramid, ts, maxdiff) {
      var val = findValue(paramid, ts, maxdiff)[0];
      if (val !== null) {
        return val;
      }
      return [null, null];
    };

    var loadHistorical = function(cb) {
      /* ignore, this is not needed any more */
      cb();
    };

    var triggerUpdate = function() {
      /* Trigger an update on all monitored elements with their last values.  Very useful after loadHistorical for anything but plots */
      for (var key in monitored) {
        if (!monitored.hasOwnProperty(key)) {
          continue;
        }
        if (historical_data[key]) {
          for (var idx = 0; idx < monitored[key].length; idx++) {
            if (!monitored[key][idx]) {
              continue;
            }
            if (historical_data[key].length > 0)
              monitored[key][idx](key, historical_data[key]);
          }
        }
      }
    };

    var monitorLastValue = function(what, callback) {
      if (what === undefined) {
        throw new Error("Can't monitor undefined");
      }
      var params = [];
      if (isNaN(parseInt(what))) {
        params = resolveParams(what);
        if (params.length > 1) {
          console.log("WARNING: More than one parameter hit for '" + JSON.stringify(what) + "'");
        }
        if (params.length === 0) {
          console.log("Internal, missing element, query was " + JSON.stringify(what));
          return;
        }
      } else {
        params.push(what);
      }

      addMonitor(params, function(key, updates) {
        var item = updates[updates.length - 1];
        var val = item[1];
        var ts = item[0];
        if (item.length > 2) {
          callback({
            paramid: key,
            ts: ts,
            value: val,
            pos: item[2],
            channel: paramInfo[key].channel,
            param: paramInfo[key].param
          });
        } else {
          callback({
            paramid: key,
            ts: ts,
            value: val,
            channel: paramInfo[key].channel,
            param: paramInfo[key].param
          });
        }
      });

      // Set last value
      var ret = getLastValueTs(params[0]);
      if (!ret) return;
      callback({
        paramid: key,
        ts: ts,
        value: val,
        channel: paramInfo[key].channel,
        param: paramInfo[key].param
      });
    };

    /* Update every second */
    loadParameters(options.onReady);
    setInterval(update, options.refresh);

    var self = {};
    self.init = init;
    self.resolveParams = resolveParams;
    self.addMonitor = addMonitor;
    self.removeMonitor = removeMonitor;
    self.clearMonitors = clearMonitors;
    self.getParamInfo = getParamInfo;
    self.getData = getData;
    self.getLastValue = getLastValue;
    self.getLastValueTs = getLastValueTs;
    self.getValue = getValue;
    self.getValueTs = getValueTs;
    self.findValue = findValue;
    self.directLoad = directLoad;
    self.monitorLastValue = monitorLastValue;
    self.sequenceMonitor = sequenceMonitor;
    self.getTimingObject = function() {
      return options.timingObject;
    };

    /* Testing only */
    self.getMonitored = function() {
      return monitored_keys;
    };
    self.getChannels = function() {
      return channels;
    };
    self.getHistorical = function() {
      return historical_data;
    };

    /* not needed */
    self.loadHistorical = loadHistorical;
    self.triggerUpdate = triggerUpdate;

    return self;

  };

  var Logs = function(startto, endto, options) {
    options = options || {};
    var self = {};
    var last_id = 0;
    var last_refresh_time = -10000;
    self.minLevel = "DEBUG";

    self.sequencer = new TIMINGSRC.Sequencer(startto, endto);

    self.logLevels = {};
    var update_levels = function() {
      XHR.get(SERVER + "/JSON.py/log_getlevels", {}, function(data) {
        // Do reverse mapping
        self.logLevelsReverse = data.levels;
        for (var lvl in data.levels) {
          if (data.levels.hasOwnProperty(lvl)) {
            self.logLevels[data.levels[lvl]] = lvl;
          }
        }
      });
    };


    var get_lines = function() {
      
    }

    if (options.timingObject) {
      options.timingObject.on("change", function() {
        last_updated = 0;
        _cleanHistorical(true);
      });
    }


    self.search = function(keywords, options) {
      options = options || {};
    }

    // Refresh 
    var is_refreshing = false;
    var refresh = function() {
      if (!startto.isReady()) {
        setTimeout(refresh, 100);
        return;
      }
      if (performance.now() - last_refresh_time < 1000) return;  // Not refreshing more than each second MAX

      if (is_refreshing) return;
      is_refreshing = true;

      last_refresh_time = performance.now();
      XHR.get(SERVER + "/JSON.py/log_getlines", 
        {last_id: last_id, start: startto.pos, end: endto.pos, minlevel:self.minLevel},
        function(reply) {
          is_refreshing = false;
          if (reply.data.length > 0)
            console.log(reply);
          last_id = reply.max_id;
          for (var i=0; i<reply.data.length; i++) {
            var line = reply.data[i];
            var data = {
              id: line[0],
              text: line[1],
              level: self.logLevels[line[2]],
              time: line[3] + (line[4]/1000),
              line: line[5],
              function: line[6],
              module: line[7],
              logger: line[8]
            };
            self.sequencer.addCue(String(data.id), new TIMINGSRC.Interval(data.time, data.time), data);
          }
        },
        function() {
          is_refreshing = false;
        }
      );
    }

    // Bind refreshes to changes
    startto.on("change", function() { last_updated = 0; refresh()});
    endto.on("change", function() { last_updated = 0; refresh()});

    TIMINGSRC.setIntervalCallback(endto, refresh, 5);

    update_levels();
    return self;
  };

  _CRYOCORE_.shutdown = function() {
    if (!confirm("Shut down?")) {
      return;
    }
    XHR.get(SERVER + "/JSON.py/shutdown?sure=True");
  };


  _CRYOCORE_.setServer = function(server) { SERVER = server; };
  _CRYOCORE_.getLogs = Logs;
  _CRYOCORE_.Config = Config;
  _CRYOCORE_.Dataset = Dataset;

  return _CRYOCORE_;

}(CryoCore || {});
