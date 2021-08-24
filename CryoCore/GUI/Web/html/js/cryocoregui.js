var CryoCoreGUI = function(_CRYOCOREGUI_) {

  function prettyTime(val) {
    var s = "";
    if (val < 0)
      s = "-";
    val = Math.abs(val); // Might get times in the future!
    /* Convert time in seconds to X days, hours, minutes, seconds */
    var days = Math.floor(val / (60 * 60 * 24));
    var hours = Math.floor((val - days * (60 * 60 * 24)) / (60 * 60));
    var mins = Math.floor((val - days * (60 * 60 * 24) - hours * (60 * 60)) / (60));
    var secs = Math.floor((val - days * (60 * 60 * 24) - hours * (60 * 60)) - mins * 60);
    if (days > 0) s += days + "d ";
    if (hours > 0) s += hours + "h ";
    if (days == 0) {
      if (mins > 0) s += mins + "m ";
    }
    if (days === 0 || hours === 0) {
      s += secs + "s"
    }
    return s;
  }

  function hslToRgb(h, s, l) {
    var r, g, b;
    if (s == 0) {
      r = g = b = l;
    } else {

      function hue2rgb(p, q, t) {
        if (t < 0) t += 1;
        if (t > 1) t -= 1;
        if (t < 1 / 6) return p + (q - p) * 6 * t;
        if (t < 1 / 2) return q;
        if (t < 2 / 3) return p + (q - p) * (2 / 3 - t) * 6;
        return p;
      }

      var q = l < 0.5 ? l * (1 + s) : l + s - l * s;
      var p = 2 * l - q;
      r = hue2rgb(p, q, h + 1 / 3);
      g = hue2rgb(p, q, h);
      b = hue2rgb(p, q, h - 1 / 3);
    }

    return [Math.floor(r * 255), Math.floor(g * 255), Math.floor(b * 255)];
  }

  var numberToColorHsl = function(i) {
    /* as the function expects a value between 0 and 1, and red = 0° and green = 120° we convert the input to the appropriate hue value */
    var hue = i * 1.2 / 360;
    /* we convert hsl to rgb (saturation 100%, lightness 50%) */
    var rgb = hslToRgb(hue, 1, .5);
    /* we format to css value and return */
    return 'rgb(' + rgb[0] + ',' + rgb[1] + ',' + rgb[2] + ')';
  }

  var clone = function(obj) {
    if (null == obj || "object" != typeof obj) return obj;
    var copy = obj.constructor();
    for (var attr in obj) {
      if (!obj.hasOwnProperty(attr)) {
        continue;
      }    
      if (obj.hasOwnProperty(attr)) copy[attr] = clone(obj[attr]);
    }
    return copy;
  }

  var hhmm = function(ts) {
    var pad = function(x) {
      if (x<10) return "0"+x;
      return ""+x;
    }
    var d = new Date();
    if (ts) {
      d = new Date(ts*1000);
    }
    return pad(d.getHours()) + ":" + pad(d.getMinutes());
  }

  var hhdd = function(ts, newday) {
    var day = {
      0: "Son",
      1: "Man",
      2: "Tir",
      3: "Ons",
      4: "Tor",
      5: "Fre",
      6: "Lor"
    };

    var pad = function(x) {
      if (x < 10) return "0" + x;
      return "" + x;
    }
    var d = new Date();
    if (ts) {
      d = new Date(ts * 1000);
    }

    var h = d.getHours();
    if (d.getMinutes() > 29) h++;

    if (newday == undefined) {
      newday = 1;
    }
    if (h < newday) {
      return "<b>" + day[d.getDay()] + "</b>";148
    }
    return pad(h);
  };

  var progressBar = function(element, dataset, paramid, options) {
    var id = "p" + (Math.random() * 10000000).toFixed(0);
    var last_event;

    if (options === undefined) options = {};
    options.min = options.min || 0;
    if (options.max === undefined) {
      options.max = 100;
    }
    options.fill_color = options.fill_color || "blue";
    options.text = options.text || "";

    element.innerHTML = "<div class='ccprogressbar' id='" + id + "' style='width:100%;height:100%;position:relative'><div class='ccbar_full' style='width:0%;height:100%'></div><div class='ccbar_text' style='width:100%;text-align:center;position:absolute;top:50%;transform:translateY(-50%)'></div></div>";
    var bar_full = document.querySelector("#" + id + " .ccbar_full");
    var bar_text = document.querySelector("#" + id + " .ccbar_text");
    bar_text.innerHTML = options.text;

    if (options.fill_color) {
      bar_full.style.background = options.fill_color;
    }
    var onUpdate = function(event) {
      if (event) {
        last_event = event;        
      } else {
        event = last_event;
      }
      if (!event) return;
      var fraction = 100 * (event.value + options.min) / (options.max - options.min);
      var elemWidth = element.offsetWidth;
      if (typeof options.fill_color === "function") {
        bar_full.style.background = options.fill_color(fraction);   
      }
      bar_full.style.width = fraction + "%";
      if (options.showText) {
        bar_text.innerHTML = options.text + " " + fraction.toFixed(0) + "%";      
      }
    }
    dataset.monitorLastValue(paramid, onUpdate);
  };

  var progressMap = function(element, dataset, paramid, options) {
    if (options === undefined) options = {};
    options.fillColor = options.fillColor || function(v) {
      return numberToColorHsl(v);
    }
    var id = "t" + (Math.random() * 10000000).toFixed(0);
    var last_event;
    var info = dataset.getParamInfo(paramid);
    var size = info.size;
    if (info.size === undefined) {
      throw new Error("Missing SIZE for map '" + paramid + "'");
    }
    var width = element.clientWidth;
    var height = element.clientHeight;
    var h = 100/size[0] + "%";
    var w = 100 / size[1] + "%";
    //var h = (height/size[0]) + "px";
    //var w = (width/size[1]) + "px";
    console.log(height, width, "->", h, w, size);
    html = "<table id='" + id + "' style='border-spacing:0px;height:100%;width:100%'>";
    for (var x=0; x<size[0]; x++) {
      html += "<tr>"
      for (var y=0; y<size[1]; y++) {
        html += "<td style='padding:0px;height:" + h + ";width:" + w + "' id='p" + x + "_" + y + "'></td>"; 
      }
      html += "</td>"
    }
    html += "</table>";
    element.innerHTML = html;
    dataset.addMonitor([paramid], function(key, values) {
      // Update all the values
      for (var i=0; i<values.length; i++) {
        var pos = values[i][2] 
        if (!document.querySelector("#" + id + " #p" + pos[0] + "_" + pos[1])) {
          continue;
        }
        //document.querySelector("#" + id + " #p" + pos[0] + "_" + pos[1]).innerHTML = values[i][1]
        document.querySelector("#" + id + " #p" + pos[0] + "_" + pos[1]).style.background = options.fillColor(values[i][1])
      }
    });
  };

  var _Plot = function($target, opt) {
    var _crosshair_timer = null;
    var get_range = function(data) {

      var min_val = 1000000000000;
      var max_val = 0;
      // Data is a plottable thing, so it's a list of elements
      // containing maps, with the data within the data tag...
      // We assume that they are sorted.
      for (var idx in data) {
        if (!data.hasOwnProperty(idx)) {
          continue;
        }

        var the_data = data[idx].data;
        if (the_data.length > 0) {
          min_val = Math.min(min_val, the_data[0][0])
          max_val = Math.max(max_val, the_data[the_data.length - 1][0]);
        }
      }
      return {
        "min": min_val,
        "max": max_val
      }
    }

    var plot = function(what, altopts) {

      if (what.length != 2) {
        throw new Error("Bad parameter, require [data, range] tuple");
      }
      var data = what[0];
      var range = what[1];
      var options = {
        legend: {
          show: true,
          position: "ne"
        },
        series: {
          lines: {
            show: true
          }
        },
        yaxis: {
          ticks: 4,
          show: true,
          // TODO: Add tickFormatter that ensures the correct width for ints
        },
        xaxis: {
          ticks: 4,
          show: true,
          tickFormatter: function(x, something) {
            var now;
            if (opt.timingObject && opt.relativeTime) {
              now = opt.timingObject.pos;
            } else {
              now = new Date().getTime() / 1000;
            }
            return prettyTime(now - x);
          }
        },
        grid: {
          clickable: true,
          hoverable: true
        },
        selection: {
          mode: "xy"
        },
        /*
        crosshair: {
          mode: "x",
          lineWidth: 2
        }
        */
      };

      opt = opt || {};
      opt.rangeX = opt.rangeX || {};
      opt.rangeY = opt.rangeY || {};
      altopts = altopts || {};
      altopts.rangeX = altopts.rangeX || {};
      altopts.rangeY = altopts.rangeY || {};

      options.grid.markings = clone(altopts.markings) || clone(opt.markings) || [];

      options.xaxis.min = altopts.rangeX.min || opt.rangeX.min,
      options.xaxis.max = altopts.rangeX.max || opt.rangeX.max,
      options.yaxis.min = altopts.rangeY.min || opt.rangeY.min,
      options.yaxis.max = altopts.rangeY.max || opt.rangeY.max,
      options.xaxis.ticks = altopts.ticks || opt.ticks || options.xaxis.ticks;

      options.xaxis.tickSize = altopts.tickSize || opt.tickSize || options.xaxis.tickSize;
      options.xaxis.labelWidth = altopts.labelWidth || opt.labelWidth || options.xaxis.labelWidth;

      options.xaxis.tickFormatter = altopts.tick_formatter_x || opt.tick_formatter_x || options.xaxis.tickFormatter;
      options.yaxis.tickFormatter = altopts.tick_formatter_y || opt.tick_formatter_y || options.yaxis.tickFormatter;

      options.yaxis.labelWidth = 35;

      if (opt.hideY)
        options.yaxis.show = false;
      if (opt.hideLegend)
        options.legend.show = false;
      if (opt.legend_pos) {
        options.legend.position = opt.legend_pos;
      }
      if (opt.showPoints)
        options.series.points = {
          show: true
        }
      if (opt.showLines === false)
        options.series.lines = {
          show: false
        }
      if (opt.alarm != undefined) {
        // Create a red line here
        if (!range) { // Use range of data
          range = get_range(data);
        }
        data.push({
          data: [
            [range.min, opt.alarm],
            [range.max, opt.alarm]
          ],
          color: "red"
        });
      }

      options.onhover = altopts.onhover || opt.onhover || options.onhover;
      options.onclick = altopts.onclick || opt.onclick || options.onclick;

      if (range) {
        options.xaxis.min = range[0];
        options.xaxis.max = range[1];
      }

      if (opt.foreach) {
        $.each(data, function(key, dataset) {
          var todelete = [];
          if (dataset.data) {
            $.each(dataset.data, function(idx, elem) {
              if (!elem) return;
              if (opt.foreach(idx, elem) == false) {
                todelete.push(idx);
                //dataset.data.splice(idx, 1);
              }
            });
          }
          // Delete in reverse to ensure we don't mess up the indexes on the way
          while (todelete.length > 0) {
            dataset.data.splice(todelete.pop(), 1);
          }
        });
      }

      if (opt.transform) {
        var _dataset = clone(data);
        $.each(data, function(idx, dataset) { 
          var _data = [];
          if (dataset.data) {
            $.each(dataset.data, function(idx, elem) {
              _data.push(opt.transform(elem));
            });
            _dataset[idx].data = _data;
          } 
        })
        data = _dataset;
      }

      // If we have any null values, remove the time from it - this will split the lines
      $.each(data, function(idx, dataset) {
        if (dataset.data) {
          $.each(dataset.data, function(idx, elem) {
            if (elem && elem[1] == null) {
              dataset.data[idx] = null;
            }
          });
        }
      });

      var now;
      if (opt.timingObject) {
        now = opt.timingObject.pos;
      } else {
        now = new Date()/1000;        
      }
      // Remove any "now" cursor that's already here
      options.grid.markings.push({ color: "#00cc00", lineWidth: 1, xaxis: { from: now, to: now} });
      var p = $.plot($target, data, options);
      // Do we annotate?
     for (var i=0; i<options.grid.markings.length; i++) {
        var marking = options.grid.markings[i];
        var annotation = marking.annotate;
        var color = marking.color;
        if (annotation) {
          annotation.x = annotation.x || 100;
          var o = p.pointOffset({x:0, y:marking.yaxis.from});
          if (isNaN(o.top)) {
            continue;
          }
          var yoffset = 0;
          if (marking.yaxis.from < 0) {
            yoffset = -15;
          }
          if (annotation.yoffset) {
            yoffset = annotation.yoffset;
          }
          $target.append("<div style='position:absolute;left:" + (annotation.x) + "px;top:" + (o.top + yoffset) + "px;font-size:smaller;color:" + color + "'>"+ annotation.text +"</div>");
        }
      }


      if (options.tooltip != false) {
        if ($("#plot_tooltip").length == 0) {
          $("<div id='plot_tooltip'></div>").css({
            position: "absolute",
            display: "none",
            border: "1px solid #fdd",
            padding: "2px",
            "z-index": 100,
            "background-color": "orange", //"#dcc",
            opacity: 0.70
          }).appendTo("body");
        }
        function tooltip(event, pos, item) {
          if (item) {
            var x = item.datapoint[0].toFixed(2),
              y = item.datapoint[1].toFixed(2);

            $("#plot_tooltip").html(hhmm(x) + ":<br/>" + item.series.label + ": " + y)
              .css({
                top: item.pageY + 5,
                left: item.pageX + 5
              })
              .fadeIn(200);
          } else {
            $("#plot_tooltip").hide();
          }          
        }

        $target.bind("plothover", tooltip);
        $target.bind("plotclick", tooltip);
      }
      /*
      if (!_crosshair_timer) {
        function cross() {p.lockCrosshair({"x":new Date()/1000, "y":0})};
        _crosshair_timer = setInterval(cross, 60000);
        cross();
      }*/
      if (options.onhover) {
        $target.bind("plothover", function(event, pos, item) {
          options.onhover(event, pos, item);
        });
      }
      if (options.onclick) {
        $target.bind("plotclick", options.onclick);
      }

      return p;
    }
    return {
      "plot": plot
    };
  }

  /* Plot some data.  Options:
   *   map - map of {parameter: {label:channel+param, color:None, fillBetween:false}}
   *   hideY: Hide y axis (default false)
   *   ticks: Number of ticks on the X axis, default 3
   *   hideLegend: Hide the legend 
   *   liveTime: how long in seconds do we show (Default 300)
   *   endOffset: How long after "now" do we show in the graph
   *   timingObject: Use timing object as opposed to current time.  If not given, the timingobject of the dataset is used, otherwise the current time is always used
   *   relativeTime: Show any time difference to the timing object as opposed to current time
   *   rangeX: Specify range for X axis
   *   rangeY: Specify range for Y axis
   *   annotate: allow annotations - i.e. grid markings
   *   showPoints: Show points on graphs (default False)
   *   showLines: Show lines on graphs (default True)
   *
   *   markings: Grid markings - look at jquery.flot documentation
   */
  var LivePlot = function($target, dataset, params, popts) {
    if (params.length == 0) {
       throw "ERROR: Live plot for no parameters requested";
    }
    if (!popts) popts = {}
    if (!popts.map) popts.map = {};
    if (popts.hideY == undefined) popts.hideY = false;
    if (popts.ticks == undefined) popts.ticks = 3
    if (popts.hideLegend == undefined) popts.hideLegend = false;
    var endOffset =  popts.endOffset || 0.0 ;
    if (!popts.timingObject) {
      popts.timingObject = dataset.getTimingObject();
    }
    var _plot = _Plot($target, popts);
    var time_offset_live = 300; // 5 minutes is reserved for live
    if (popts.liveTime) {
      time_offset_live = popts.liveTime;
    }

    var getData = function() {
      var the_data = [];
      var now;
      if (popts.timingObject) {
        now = popts.timingObject.pos;
      } else {
        now = new Date().getTime() / 1000;
      }
      start_time = now - time_offset_live;
      end_time = now + endOffset;
      /* Get the data for this plot based on it being historic */
      for (var idx in params) {
        if (!params.hasOwnProperty(idx)) {
          continue;
        }
        var paramid = params[idx];
        var data = dataset.getData(paramid, start_time, end_time);
        var info = dataset.getParamInfo(paramid);
        if (!info) {
          continue;
        }
        var color;
        var label = info.channel + "." + info.param;
        
        var fillBetween = undefined;
        var fill = false;
        if (popts.map[paramid]) {
          label = popts.map[paramid].label;
          color = popts.map[paramid].color;
          fillBetween = popts.map[paramid].fillBetween;
          if (fillBetween && fillBetween != "") {
            fill = popts.map[paramid][3] || 0.2;
          }
        }
        var entry = {
          "id":label,
          "label": label,
          "color": color,
          "data": data,
        }
        if (fillBetween != undefined) {  
          entry.label = undefined;
          if (fillBetween != "") {
            entry.lines={show:true, lineWidth:0, fill:fill};
            entry.fillBetween = fillBetween;            
          } else {
            entry.lines={show:true, lineWidth:0, fill:false}
          }
        }

        the_data.push(entry);
      }
      return [the_data, [start_time, end_time]];
    }

    var plot = function() {
      try {
        return _plot.plot(getData());
      } catch (err) {
        console.log("Error plotting", $target.selector, ", stopping monitoring", err);
        // Plot is gone, unregister monitor
        dataset.removeMonitor(params, plot);
      }
    }

    // Register callbacks on updates
    dataset.addMonitor(params, plot)
    var self = {};
    self.plot = plot;
    self.setTimespan = function(t) {
      time_span = t;
    };

    // Preload
    if (popts.preload != false) {
      var now;
      if (popts.timingObject) {
        now = popts.timingObject.pos;
      } else {
        now = new Date().getTime() / 1000
      }
      start_time = now - time_offset_live;
      end_time = now - endOffset;
      dataset.directLoad(params, start_time, end_time, null, function(data) {
        plot();
      });
    }

    return self;
  }
  
  _CRYOCOREGUI_.prettyTime = prettyTime;
  _CRYOCOREGUI_.fractionToColor = function(frac) {
    return numberToColorHsl(frac * 100)
  };

  _CRYOCOREGUI_.progressBar = progressBar;
  _CRYOCOREGUI_.progressMap = progressMap;
  _CRYOCOREGUI_.plot = LivePlot;

  return _CRYOCOREGUI_;

}(CryoCoreGUI || {});
