var ToggleUtil = (function($, ViewUtil, StateUtil) {
  var step_map = {};
  var toggle = {};
  var GIGABYTES = parseInt(1024 * 1024 * 1024);

  toggle.redData     = [];
  toggle.blueData  = [];
  toggle.tipData     = [];
  toggle.maxValues   = [];
  toggle.titles      = [];

  toggle.renderCharts = function(sm) {
    step_map = sm
    buildDatasets();
    renderAndRegisterEvent();
  }

  function buildDatasets() {
    assignData(
      [numTasksMapFunc, gsReadsMapFunc, hdfsReadsMapFunc, clusterReadsMapFunc, localityMapFunc, ioSecsMapFunc, vcoreSecsMapFunc, cpuSecsFunc],
      "redData"
    );

    assignData(
      [numTasksReduceFunc, gsWritesReduceFunc, hdfsWritesReduceFunc, clusterWritesReduceFunc, localityReduceFunc, ioSecsReduceFunc, vcoreSecsReduceFunc, gcSecsFunc],
      "blueData"
    );

    assignData(
      [numTasksTipFunc, hdfsTipFunc, gsTipFunc, clusterTipFunc, localityTipFunc, ioSecsTipFunc, vcoreSecsTipFunc, cpuGcTipFunc],
      "tipData"
    );

    assignData(
      [numTasksMaxValueFunc, hdfsMaxValueFunc, gsMaxValueFunc, clusterMaxValueFunc, localityMaxValueFunc, ioSecsMaxValueFunc, vcoreSecsMaxValueFunc, cpuGcMaxValueFunc],
      "maxValues"
    );

    assignTitles([
      "Map and Reduce tasks per Step",
      "GB Read/Written (HDFS) per Step",
      "GB Read/Written (GCS) per Step",
      "GB Read/Written (Cluster Disk) per Step",
      "% of Node and Rack Locality per Step",
      "Cascading Total R/W Durations (all Tasks) per Step",
      "Vcore-Seconds per Step",
      "Total CPU and GC Seconds per Step"
    ]);
  }

  function renderAndRegisterEvent() {
    var ndx = StateUtil.getChartState();
    var actitle = $("#actitle");
    actitle.text(toggle.titles[ndx]);
    actitle.append('<button class="glyphicon glyphicon-arrow-right" style="float:right" id="actoggle_right"></button>');
    actitle.append('<button class="glyphicon glyphicon-arrow-left" style="float:right" id="actoggle_left"></button>');

    $("#actoggle_right").on("click", function(evt) {
      StateUtil.incrementChartState(toggle.redData.length);
      renderAndRegisterEvent();
    });

    $("#actoggle_left").on("click", function(evt) {
      StateUtil.decrementChartState(toggle.redData.length);
      renderAndRegisterEvent();
    });

    AreaChart.renderAreaChart(
      toggle.redData[ndx],
      toggle.blueData[ndx],
      toggle.tipData[ndx],
      toggle.maxValues[ndx]
    );
  }

  function assignData(funcs, attrib) {
    for (ndx in funcs) {
      var data = funcs[ndx](step_map);
      if (data !== undefined) {
        toggle[attrib].push(data);
      }
    }
  }

  function assignTitles(titles) {
    for (ndx in titles) {
      toggle.titles.push( titles[ndx] );
    }
  }


//////// numTasks functions ////////
  function numTasksMapFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push({
        x: key,
        y: step.map_tasks
      });
    }
    return arr;
  }

  function numTasksReduceFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push({
        x: key,
        y: step.reduce_tasks
      });
    }
    return arr;
  }

  function numTasksTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push('<span style="color:Pink">' + step.map_tasks + '</span> Map Tasks<br>' +
        '<span style="color:LightBlue">' + step.reduce_tasks + '</span> Reduce Tasks<p>' +
        ' used in Job Step ' + key
      );
    }
    return arr;
  }

  function numTasksMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
      var step = step_map[key];
      if (step.map_tasks > max) { max = step.map_tasks; }
      if (step.reduce_tasks > max) { max = step.reduce_tasks; }
    }
    return max;
  }

  //////// hdfs* functions  ////////
  function hdfsReadsMapFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push({
        x: key,
        y: Math.round(step.hdfs_bytes_read / GIGABYTES)
      });
    }
    return arr;
  }

  function hdfsWritesReduceFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push({
        x: key,
        y: Math.round(step.hdfs_bytes_written / GIGABYTES)
      });
    }
    return arr;
  }

    function gsReadsMapFunc(step_map) {
        var arr = [];
        for (key in step_map) {
            var step = step_map[key];
            arr.push({
                x: key,
                y: Math.round(step.gs_bytes_read / GIGABYTES)
            });
        }
        return arr;
    }

    function gsWritesReduceFunc(step_map) {
        var arr = [];
        for (key in step_map) {
            var step = step_map[key];
            arr.push({
                x: key,
                y: Math.round(step.gs_bytes_written / GIGABYTES)
            });
        }
        return arr;
    }

  function hdfsTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push('<span style="color:Pink">' + ViewUtil.prettyPrintBytes(step.hdfs_bytes_read) + '</span> Read<br>' +
        '<span style="color:LightBlue">' + ViewUtil.prettyPrintBytes(step.hdfs_bytes_written) + '</span> Written<p>' +
        ' in Job Step ' + key
      );
    }
    return arr;
  }

  function hdfsTipFunc(step_map) {
      var arr = [];
      for (key in step_map) {
          var step = step_map[key];
          arr.push('<span style="color:Pink">' + ViewUtil.prettyPrintBytes(step.gs_bytes_read) + '</span> Read<br>' +
              '<span style="color:LightBlue">' + ViewUtil.prettyPrintBytes(step.gs_bytes_written) + '</span> Written<p>' +
              ' in Job Step ' + key
          );
      }
      return arr;
  }

    function hdfsMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
      var step = step_map[key];
      if (step.hdfs_bytes_read > max) { max = step.hdfs_bytes_read; }
      if (step.hdfs_bytes_written > max) { max = step.hdfs_bytes_written; }
    }
    return parseInt(max / GIGABYTES);
  }

  function hdfsMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
      var step = step_map[key];
      if (step.gs_bytes_read > max) { max = step.gs_bytes_read; }
      if (step.gs_bytes_written > max) { max = step.gs_bytes_written; }
    }
    return parseInt(max / GIGABYTES);
  }

  //////// cluster* functions  ////////
  function clusterReadsMapFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push({
        x: key,
        y: Math.round(step.file_bytes_read / GIGABYTES)
      });
    }
    return arr;
  }

  function clusterWritesReduceFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push({
        x: key,
        y: Math.round(step.file_bytes_written / GIGABYTES)
      });
    }
    return arr;
  }

  function clusterTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push('<span style="color:Pink">' + ViewUtil.prettyPrintBytes(step.file_bytes_read) + '</span> Read<br>' +
        '<span style="color:LightBlue">' + ViewUtil.prettyPrintBytes(step.file_bytes_written) + '</span> Written<p>' +
        ' in Job Step ' + key
      );
    }
    return arr;
  }

  function clusterMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
      var step = step_map[key];
      if (step.file_bytes_read > max) { max = step.file_bytes_read; }
      if (step.file_bytes_written > max) { max = step.file_bytes_written; }
    }
    return parseInt(max / GIGABYTES);
  }

  //////// locality* functions  ////////
  function localityMapFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.map_tasks > 0 ? Math.round(100 * (step.data_local_map_tasks / step.map_tasks), 4) : 0;
      arr.push({x: key, y: value});
    }
    return arr;
  }

  function localityReduceFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.map_tasks > 0 ? Math.round(100 * (step.rack_local_map_tasks / step.map_tasks), 4) : 0;
      arr.push({x: key, y: value});
    }
    return arr;
  }

  function localityTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var local = step.map_tasks > 0 ? Math.round(100 * (step.data_local_map_tasks / step.map_tasks), 4) : "?";
      var rack = step.map_tasks > 0 ? Math.round(100 * (step.rack_local_map_tasks / step.map_tasks), 4) : "?";
      arr.push('<span style="color:Pink">' + local + '%</span> Node Local Mappers<br>' +
        '<span style="color:LightBlue">' + rack + '%</span> Rack Local Mappers<p>' +
        ' in Job Step ' + key
      );
    }
    return arr;
  }

  function localityMaxValueFunc(step_map) {
    return 100;
  }

    //////// vcoreSecs* functions  ////////
    function vcoreSecsMapFunc(step_map) {
      var arr = [];
      for (key in step_map) {
          var step = step_map[key];
          var value = step.map_vcore_secs;
          arr.push({x: key, y: value});
      }
      return arr;
    }

    function vcoreSecsReduceFunc(step_map) {
      var arr = [];
      for (key in step_map) {
        var step = step_map[key];
        var value = step.reduce_vcore_secs;
        arr.push({x: key, y: value});
      }
      return arr;
    }

    function vcoreSecsTipFunc(step_map) {
      var arr = [];
      for (key in step_map) {
        var step  = step_map[key];
        var map = step.map_vcore_secs;
        var reduce = step.reduce_vcore_secs;
        arr.push('<span style="color:Pink">' + map + ' Vcore-Seconds</span> Map Tasks<br>' +
           '<span style="color:LightBlue">' + reduce + ' Vcore-Seconds</span> Reduce Tasks<p>' +
           ' in Job Step ' + key);
      }
      return arr;
    }

  function vcoreSecsMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
        var step = step_map[key];
        var map = step.map_vcore_secs;
        var reduce = step.reduce_vcore_secs;
        if (map > max) { max = map; }
        if (reduce > max) { max = reduce; }
    }
    return max;
  }


  //////// ioSecs* functions  ////////
  function ioSecsMapFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.io_read_millis > 0 ? Math.round(step.io_read_millis / 1000) : 0;
      arr.push({x: key, y: value});
    }
    return arr;
  }

  function ioSecsReduceFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.io_write_millis > 0 ? Math.round(step.io_write_millis / 1000) : 0;
      arr.push({x: key, y: value});
    }
    return arr;
  }

  function ioSecsTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step  = step_map[key];
      var read  = step.io_read_millis > 0 ? Math.round(step.io_read_millis / 1000) : 0.0;
      var write = step.io_write_millis > 0 ? Math.round(step.io_write_millis / 1000) : 0.0;
      arr.push('<span style="color:Pink">' + read + ' secs</span> IO Read Duration<br>' +
        '<span style="color:LightBlue">' + write + ' secs</span> IO Write Duration<p>' +
        ' in Job Step ' + key
      );
    }
    return arr;
  }

  function ioSecsMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
      var step = step_map[key];
      if (step.io_read_millis > max) { max = step.io_read_millis; }
      if (step.io_write_millis > max) { max = step.io_write_millis; }
    }
    return Math.round(max / 1000);
  }

  //// CPU & GC funcs ////
  function cpuSecsFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.cpu_millis > 0 ? Math.round(step.cpu_millis / 1000) : 0;
      arr.push({x: key, y: value});
    }
    return arr;
  }

  function gcSecsFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.gc_millis > 0 ? Math.round(step.gc_millis / 1000) : 0;
      arr.push({x: key, y: value});
    }
    return arr;
  }

  function cpuGcTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key]; 
      var cpu_secs = step.cpu_millis > 0 ? Math.round(step.cpu_millis / 1000.0, 2) : 0.0;
      var gc_secs = step.gc_millis > 0 ? Math.round(step.gc_millis / 1000.0, 2) : 0.0;
      arr.push('<span style="color:Pink">' + cpu_secs + ' secs</span> CPU time<br>' +
        '<span style="color:LightBlue">' + gc_secs + ' secs</span> GC time<p>' +
        ' in Job Step ' + key);
    }
    return arr;
  }

  function cpuGcMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
      var step = step_map[key];
      if (step.cpu_millis > max) { max = step.cpu_millis; }
      if (step.gc_millis > max) { max = step.gc_millis; }
    }
    return Math.round(max / 1000);
  }

  return toggle;

})(jQuery, ViewUtil, StateUtil);
