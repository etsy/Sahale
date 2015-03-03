var ToggleUtil = (function($, ViewUtil, StateUtil) {
  var stepMap = {};
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
    stepMap = sm;

    assignData(
      [numTasksMapFunc, hdfsReadsMapFunc, clusterReadsMapFunc, localityMapFunc],
      "mapData"
    );

    assignData(
      [numTasksReduceFunc, hdfsWritesReduceFunc, clusterWritesReduceFunc, localityReduceFunc],
      "reduceData"
    );

    assignData(
      [numTasksTipFunc, hdfsTipFunc, clusterTipFunc, localityTipFunc],
      "tipData"
    );

    assignData(
      [numTasksMaxValueFunc, hdfsMaxValueFunc, clusterMaxValueFunc, localityMaxValueFunc],
      "maxValues"
    );

    assignTitles([
      "Map and Reduce tasks per Step",
      "GB Read/Written (HDFS) per Step",
      "GB Read/Written (Cluster Disk) per Step",
      "% of Node and Rack Locality per Step"
    ]);
  }

  function renderAndRegisterEvent() {
    var ndx = parseInt(StateUtil.getChartState() % toggle.mapData.length);
    var actitle = $("#actitle");
    actitle.text(toggle.titles[ndx]);
    actitle.append('<button class="glyphicon glyphicon-stats" style="float:right" id="actoggle"></button>');

    $("#actoggle").on("click", function(evt) {
      StateUtil.incrementChartState();
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
      var data = funcs[ndx](stepMap);
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
  function numTasksMapFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      arr.push({
        x: key,
        y: step.maptasks
      });
    }
    return arr;
  }

  function numTasksReduceFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      arr.push({
        x: key,
        y: step.reducetasks
      });
    }
    return arr;
  }

  function numTasksTipFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      arr.push('<span style="color:Pink">' + step.maptasks + '</span> Map Tasks<br>' +
        '<span style="color:LightBlue">' + step.reducetasks + '</span> Reduce Tasks<p>' +
        ' used in Job Step ' + key
      );
    }
    return arr;
  }

  function numTasksMaxValueFunc(stepMap) {
    var max = 0;
    for (key in stepMap) {
      var step = stepMap[key];
      if (parseInt(step.maptasks) > parseInt(max)) { max = step.maptasks; }
      if (parseInt(step.reducetasks) > parseInt(max)) { max = step.reducetasks; }
    }
    return parseInt(max);
  }

  //////// hdfs* functions  ////////
  function hdfsReadsMapFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      arr.push({
        x: key,
        y: Math.round(step.hdfsbytesread / GIGABYTES)
      });
    }
    return arr;
  }

  function hdfsWritesReduceFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      arr.push({
        x: key,
        y: Math.round(step.hdfsbyteswritten / GIGABYTES)
      });
    }
    return arr;
  }

  function hdfsTipFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      arr.push('<span style="color:Pink">' + ViewUtil.prettyPrintBytes(step.hdfsbytesread) + '</span> Read<br>' +
        '<span style="color:LightBlue">' + ViewUtil.prettyPrintBytes(step.hdfsbyteswritten) + '</span> Written<p>' +
        ' in Job Step ' + key
      );
    }
    return arr;
  }

  function hdfsMaxValueFunc(stepMap) {
    var max = 0.0;
    for (key in stepMap) {
      var step = stepMap[key];
      if (parseInt(step.hdfsbytesread) > parseInt(max)) { max = step.hdfsbytesread; }
      if (parseInt(step.hdfsbyteswritten) > parseInt(max)) { max = step.hdfsbyteswritten; }
    }
    return parseInt(max / GIGABYTES);
  }

  //////// cluster* functions  ////////
  function clusterReadsMapFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      arr.push({
        x: key,
        y: Math.round(step.filebytesread / GIGABYTES)
      });
    }
    return arr;
  }

  function clusterWritesReduceFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      arr.push({
        x: key,
        y: Math.round(step.filebyteswritten / GIGABYTES)
      });
    }
    return arr;
  }

  function clusterTipFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      arr.push('<span style="color:Pink">' + ViewUtil.prettyPrintBytes(step.filebytesread) + '</span> Read<br>' +
        '<span style="color:LightBlue">' + ViewUtil.prettyPrintBytes(step.filebyteswritten) + '</span> Written<p>' +
        ' in Job Step ' + key
      );
    }
    return arr;
  }

  function clusterMaxValueFunc(stepMap) {
    var max = 0.0;
    for (key in stepMap) {
      var step = stepMap[key];
      if (parseInt(step.filebytesread) > parseInt(max)) { max = step.filebytesread; }
      if (parseInt(step.filebyteswritten) > parseInt(max)) { max = step.filebyteswritten; }
    }
    return parseInt(max / GIGABYTES);
  }

  //////// locality* functions  ////////
  function localityMapFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      var value = step.maptasks > 0 ? Math.round(100 * (step.datalocalmaptasks / step.maptasks), 4) : 0;
      arr.push({
        x: key,
        y: value
      });
    }
    return arr;
  }

  function localityReduceFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      var value = step.maptasks > 0 ? Math.round(100 * (step.racklocalmaptasks / step.maptasks), 4) : 0;
      arr.push({
        x: key,
        y: value
      });
    }
    return arr;
  }

  function localityTipFunc(stepMap) {
    var arr = [];
    for (key in stepMap) {
      var step = stepMap[key];
      var local = step.maptasks > 0 ? Math.round(100 * (step.datalocalmaptasks / step.maptasks), 4) : "?";
      var rack = step.maptasks > 0 ? Math.round(100 * (step.racklocalmaptasks / step.maptasks), 4) : "?";
      arr.push('<span style="color:Pink">' + local + '%</span> Node Local Mappers<br>' +
        '<span style="color:LightBlue">' + rack + '%</span> Rack Local Mappers<p>' +
        ' in Job Step ' + key
      );
    }
    return arr;
  }

  function localityMaxValueFunc(stepMap) {
    return 100;
  }


  return toggle;

})(jQuery, ViewUtil, StateUtil);
