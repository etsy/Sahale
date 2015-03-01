/////////////////////////////// For DagreD3 Graph View ////////////////////////////////////
var GraphUtil = (function($, d3, dagreD3, ViewUtil, ChartUtil) {
  // module 'constant'
  var FLOWGRAPH_INIT_STATE = { tx: 40, ty: 40, scale: 1, curid: 0, curcolor: "NONE" };

  // Global module state
  var graph = {},
    translateRegex = /translate\(\s*([0-9.-]+)\s*,\s*([0-9.-]+)\)/,
    scaleRegex =     /scale\(\s*([0-9.-]+)\s*\)/,
    flowId =         "",
    height =         0,
    state =          FLOWGRAPH_INIT_STATE,
    theG =           null
  ;


  //////////////////////// GraphUtil public functions //////////////////////
  graph.captureFlowId = function(fl) {
    flowId = fl.flow_id;
  }

  graph.setNavigationLinks = function(theFlow) {
    $('#jobname').text(theFlow.flow_name.replace('com.etsy.scalding.jobs.',''));
    $('li.flowname a').attr('href', '/history/' + theFlow.flow_name);
    $('li.clearstate a').click(function() {
      sessionStorage.removeItem('flowid-' + theFlow.flow_id);
      $(window).off('unload');
      location.reload();
    });
  }

  graph.setEventHandlers = function(stepMap) {
    for (var key in stepMap) {
      registerHover(stepMap[key]);
    }
    updateUnloadHandler();
    updateViewState(stepMap);
  }

  graph.addAllEdges = function(graphData, stepMap, rows) {
    rows.forEach(function(item, ndx, arr) {
      var step = stepMap[item.src_stage];
      graphData.addEdge(
        null,
        item.src_stage.toString(),
        item.dest_stage.toString(),
        {
          label: '<span style="font-size:14px;color:' + ChartUtil.getColorForNumBytes(step.hdfsbyteswritten) + '">' +
            ViewUtil.prettyPrintBytes(step.hdfsbyteswritten) + '</span>'
        }
      );
    });
  }

  graph.addAllVertices = function(graphData, stepMap) {
    for (var key in stepMap) {
      var step = stepMap[key];
      graphData.addNode(key, { label: addElephant(step) });
    }
  }

  graph.renderFlowGraph = function(graphData) {
    theG = d3.select("#flowgraph > g");
    renderViewLayout(graphData);
    d3.select("#flowgraph").call(d3.behavior.zoom().on("zoom", zoomFunction));
  }

  /////////////////// private utility methods //////////////////////
  function addElephant(item) {
    return '<img width="40px" height="40px" ' +
      'title="MapReduce Job (Step ' + item.stepnumber + ' of Flow)" ' +
      'style="width:40px;height:40px;background-color:' +
      ViewUtil.getColorByStepStatus(item) + '" ' +
      'src="' + getImageByStepStatus(item) + '" ' +
      'class="img-rounded" ' +
      'id="step-image-' + item.stepnumber + '" />';
  }

  function getImageByStepStatus(item) {
    switch(item.stepstatus) {
      case "RUNNING": case "SUMBITTED": case "SUCCESSFUL":
        return "/images/happy_elephant.png";
      case "FAILED":
        return "/images/sad_elephant.png";
      default:
        return "/images/skeptical_elephant.png";
    }
  }

  function renderViewLayout(graphData) {
    // draw the flow graph
    var renderer = new dagreD3.Renderer();
    var layout = renderer.layout().rankDir("LR");
    layout = renderer.layout(layout).run(graphData, theG);
    // capture some state to maintain view during transformations/refreshes etc.
    height = layout.graph().height;
  }

  // load graph view state or set init values, call zoom event to exec
  function updateViewState(stepMap) {
    state = getViewState();
    checkPreviouslySelectedVertex(stepMap);
    theG.attr("transform", stringifyTransform(state));
  }

  function registerHover(step) {
    $("#step-image-" + step.stepnumber).hover(
      function(e) {
        if (state.curid !== 0) {
          $("#step-image-" + state.curid).removeClass("hi-lite");
          $("#step-" + state.curid).focus().hide();
        }
        $("#no-step").hide();

        state.curid = step.stepnumber;
        var vertex = $(this);
        state.curcolor = vertex.addClass("hi-lite");
        vertex.focus();

        var mrTitle = $("#mrdetail-title");
        mrTitle.html(ViewUtil.renderStepStatus(step)).show().focus();

        $("#step-" + state.curid).fadeIn(200);
      },
      function(e) {
        e.stopPropagation();
      }
    );
    $("#step-" + step.stepnumber).hide();
  }

  function zoomFunction() {
      var result = "translate(" + d3.event.translate + ") scale(" + d3.event.scale + ")";
      if (state === null) { state = getViewState(); }
      var adjscale = Math.max(.0001, parseFloat(state.scale) * parseFloat(d3.event.scale));
      var match = /([0-9.-]+)\s*,\s*([0-9.-]+)/.exec(d3.event.translate);
      var dx = parseFloat(match[1]);
      var dy = parseFloat(match[2]);
      var adjx = parseFloat(state.tx) + dx;
      var adjy = parseFloat(state.ty) + dy;
      result = "translate(" + adjx + "," + adjy + ") scale(" + adjscale + ")";
      theG.attr("transform", result);
  }

  // set the handler that will save graph view state on page reload
  function updateUnloadHandler() {
    $(window).on("unload", function() {
      if (flowId && flowId !== "")  {
        setViewState();
      }
    });
  }

  // takes JSON string and converts to js Object literal
  // or supplies default data struct if this is first visit of session
  function getViewState() {
    var newStateStr = sessionStorage.getItem("flowid-" + flowId);
    return newStateStr === null ?
      updateWithGraphRenderInfo(FLOWGRAPH_INIT_STATE) : JSON.parse(newStateStr);
  }

  // converts string 'transform' attribute -> JSON string using
  // sessionStorage (with flowId as key) to retain after page refresh
  function setViewState() {
    var raw = d3.select("#flowgraph > g").attr("transform");
    var tMatch = translateRegex.exec(raw);
    var sMatch = scaleRegex.exec(raw);
    var transx = tMatch[1];
    var transy = tMatch[2];
    var scalexy = sMatch[1];
    // 'curcolor' should be set by getter code as it can change during a page refresh
    var result =  JSON.stringify(
     { tx: transx, ty: transy, scale: scalexy, curid: state.curid, curcolor: "ERROR_BAD_PARAM" }
    );
    sessionStorage.setItem("flowid-" + flowId, result);
  }

  function updateWithGraphRenderInfo(template) {
    template.ty = 183.0 - (parseInt(height) / 2.0);
    return template;
  }

  // if there was a previous selected state, restore it - but store the
  // color of the current selected job stage from using fresh page data
  // as it could have been updated during page refresh.
  function checkPreviouslySelectedVertex(stepMap) {
    if (state.curid !== 0) {
      $("#no-step").remove();
      var curImg = $("#step-image-" + state.curid);
      state.curcolor = curImg.removeClass("hi-lite");
      $("#mrdetails-title").html(ViewUtil.renderStepStatus(stepMap[state.curid])).show();
      $("#step-" + state.curid).show();
      curImg.trigger('mouseover');
    }
  }

  function stringifyTransform(data) {
    return "translate(" + data.tx + "," + data.ty + ") scale(" + data.scale + ")";
  }

  return graph;
}(jQuery, d3, dagreD3, ViewUtil, ChartUtil));
