var ToggleUtil = (function($, ViewUtil, StateUtil) {
  var step_map = {};
  var toggle = {};
  var GIGABYTES = parseInt(1024 * 1024 * 1024);

  toggle.mapData     = [];
  toggle.reduceData  = [];
  toggle.tipData     = [];
  toggle.maxValues   = [];
  toggle.titles      = [];

  toggle.renderCharts = function(step_map) {
    buildDatasets(step_map);
    renderAndRegisterEvent();
  }

  function buildDatasets(sm) {
    step_map = sm;

    assignData(
      [numTasksMapFunc, hdfsReadsMapFunc, clusterReadsMapFunc, localityMapFunc, ioSecsMapFunc, vcoreSecsMapFunc],
      "mapData"
    );

    assignData(
      [numTasksReduceFunc, hdfsWritesReduceFunc, clusterWritesReduceFunc, localityReduceFunc, ioSecsReduceFunc, vcoreSecsReduceFunc],
      "reduceData"
    );

    assignData(
      [numTasksTipFunc, hdfsTipFunc, clusterTipFunc, localityTipFunc, ioSecsTipFunc, vcoreSecsTipFunc],
      "tipData"
    );

    assignData(
      [numTasksMaxValueFunc, hdfsMaxValueFunc, clusterMaxValueFunc, localityMaxValueFunc, ioSecsMaxValueFunc, vcoreSecsMaxValueFunc],
      "maxValues"
    );

    assignTitles([
      "Map and Reduce tasks per Step",
      "GB Read/Written (HDFS) per Step",
      "GB Read/Written (Cluster Disk) per Step",
      "% of Node and Rack Locality per Step",
      "Cascading Total R/W Durations (all Tasks) per Step",
      "Vcore-Seconds per Step"	
    ]);
  }

  function renderAndRegisterEvent() {
      var ndx = parseInt(StateUtil.getChartState() % toggle.mapData.length);
    var actitle = $("#actitle");
      actitle.text(toggle.titles[ndx]);
      actitle.append('<button class="glyphicon glyphicon-arrow-right" style="float:right" id="actoggle_right"></button>');
      actitle.append('<button class="glyphicon glyphicon-arrow-left" style="float:right" id="actoggle_left"></button>');

    $("#actoggle_right").on("click", function(evt) {
      StateUtil.incrementChartState(toggle.mapData.length);
      renderAndRegisterEvent();
    });

    $("#actoggle_left").on("click", function(evt) {
      StateUtil.decrementChartState(toggle.mapData.length);
      renderAndRegisterEvent();
    });

    AreaChart.renderAreaChart(
      toggle.mapData[ndx],
      toggle.reduceData[ndx],
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
        y: step.maptasks
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
        y: step.reducetasks
      });
    }
    return arr;
  }

  function numTasksTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push('<span style="color:Pink">' + step.maptasks + '</span> Map Tasks<br>' +
        '<span style="color:LightBlue">' + step.reducetasks + '</span> Reduce Tasks<p>' +
        ' used in Job Step ' + key
      );
    }
    return arr;
  }

  function numTasksMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
      var step = step_map[key];
      if (parseInt(step.maptasks) > parseInt(max)) { max = step.maptasks; }
      if (parseInt(step.reducetasks) > parseInt(max)) { max = step.reducetasks; }
    }
    return parseInt(max);
  }

  //////// hdfs* functions  ////////
  function hdfsReadsMapFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push({
        x: key,
        y: Math.round(step.hdfsbytesread / GIGABYTES)
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
        y: Math.round(step.hdfsbyteswritten / GIGABYTES)
      });
    }
    return arr;
  }

  function hdfsTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push('<span style="color:Pink">' + ViewUtil.prettyPrintBytes(step.hdfsbytesread) + '</span> Read<br>' +
        '<span style="color:LightBlue">' + ViewUtil.prettyPrintBytes(step.hdfsbyteswritten) + '</span> Written<p>' +
        ' in Job Step ' + key
      );
    }
    return arr;
  }

  function hdfsMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
      var step = step_map[key];
      if (parseInt(step.hdfsbytesread) > parseInt(max)) { max = step.hdfsbytesread; }
      if (parseInt(step.hdfsbyteswritten) > parseInt(max)) { max = step.hdfsbyteswritten; }
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
        y: Math.round(step.filebytesread / GIGABYTES)
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
        y: Math.round(step.filebyteswritten / GIGABYTES)
      });
    }
    return arr;
  }

  function clusterTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      arr.push('<span style="color:Pink">' + ViewUtil.prettyPrintBytes(step.filebytesread) + '</span> Read<br>' +
        '<span style="color:LightBlue">' + ViewUtil.prettyPrintBytes(step.filebyteswritten) + '</span> Written<p>' +
        ' in Job Step ' + key
      );
    }
    return arr;
  }

  function clusterMaxValueFunc(step_map) {
    var max = 0;
    for (key in step_map) {
      var step = step_map[key];
      if (parseInt(step.filebytesread) > parseInt(max)) { max = step.filebytesread; }
      if (parseInt(step.filebyteswritten) > parseInt(max)) { max = step.filebyteswritten; }
    }
    return parseInt(max / GIGABYTES);
  }

  //////// locality* functions  ////////
  function localityMapFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.maptasks > 0 ? Math.round(100 * (step.datalocalmaptasks / step.maptasks), 4) : 0;
      arr.push({
        x: key,
        y: value
      });
    }
    return arr;
  }

  function localityReduceFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.maptasks > 0 ? Math.round(100 * (step.racklocalmaptasks / step.maptasks), 4) : 0;
      arr.push({
        x: key,
        y: value
      });
    }
    return arr;
  }

  function localityTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var local = step.maptasks > 0 ? Math.round(100 * (step.datalocalmaptasks / step.maptasks), 4) : "?";
      var rack = step.maptasks > 0 ? Math.round(100 * (step.racklocalmaptasks / step.maptasks), 4) : "?";
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
	    arr.push({
		x: key,
		y: value
	    });
	}
	return arr;
    }

    function vcoreSecsReduceFunc(step_map) {
	var arr = [];
	for (key in step_map) {
	    var step = step_map[key];
	    var value = step.reduce_vcore_secs;
	    arr.push({
		x: key,
		y: value
	    });
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
		     ' in Job Step ' + key
		    );
	}
	return arr;
    }

    function vcoreSecsMaxValueFunc(step_map) {
	var max = 0;
	for (key in step_map) {
	    var step = step_map[key];
	    var map = step.map_vcore_secs;
	    var reduce = step.reduce_vcore_secs;
	    if (parseInt(map) > parseInt(max)) { max = map; }
	    if (parseInt(reduce) > parseInt(max)) { max = reduce; }
	}
	return max;
    }


  //////// ioSecs* functions  ////////
  function ioSecsMapFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.ioreadmillis > 0 ? Math.round(step.ioreadmillis / 1000) : 0;
      arr.push({
        x: key,
        y: value
      });
    }
    return arr;
  }

  function ioSecsReduceFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step = step_map[key];
      var value = step.iowritemillis > 0 ? Math.round(step.iowritemillis / 1000) : 0;
      arr.push({
        x: key,
        y: value
      });
    }
    return arr;
  }

  function ioSecsTipFunc(step_map) {
    var arr = [];
    for (key in step_map) {
      var step  = step_map[key];
      var read  = step.ioreadmillis > 0 ? Math.round(step.ioreadmillis / 1000) : 0.0;
      var write = step.iowritemillis > 0 ? Math.round(step.iowritemillis / 1000) : 0.0;
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
      if (parseInt(step.ioreadmillis) > parseInt(max)) { max = step.ioreadmillis; }
      if (parseInt(step.iowritemillis) > parseInt(max)) { max = step.iowritemillis; }
    }
    return Math.round(max / 1000);
  }


  return toggle;

})(jQuery, ViewUtil, StateUtil);
