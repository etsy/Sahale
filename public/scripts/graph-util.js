/////////////////////////////// For DagreD3 Graph View ////////////////////////////////////
var GraphUtil = (function($, d3, dagreD3, ViewUtil, ChartUtil, StateUtil) {

  // Global module state
  var graph = {},
    translateRegex = /translate\(\s*([0-9.-]+)\s*,\s*([0-9.-]+)\)/,
    scaleRegex =     /scale\(\s*([0-9.-]+)\s*\)/,
    flow_id =         "",
    height =         0,
    theG =           null
  ;


  //////////////////////// GraphUtil public functions //////////////////////
  graph.captureFlowId = function(fl) {
    flow_id = fl.flow_id;
  }

  graph.setNavigationLinks = function(flow) {
    $('#jobname').text(flow.flow_name.replace('com.etsy.scalding.jobs.',''));
    $('li.flowname a').attr('href', '/history/' + flow.flow_name);
    $('li.clearstate a').click(function() {
      StateUtil.clearFlowState(flow.flow_id);
      $(window).off('unload');
      location.reload();
    });
  }

  graph.setEventHandlers = function(step_map) {
    for (var key in step_map) {
      registerHover(step_map[key]);
    }
    updateUnloadHandler();
    updateViewState(step_map);
  }

  graph.renderFlowGraph = function(graph_data, step_map, edges) {
    addAllVertices(graph_data, step_map);
    addAllEdges(graph_data, step_map, edges);
    renderD3Graph(graph_data);
  }


  /////////////////// private utility methods //////////////////////
  function addAllEdges(graph_data, step_map, rows) {
    rows.forEach(function(item, ndx, arr) {
      var step = step_map[item.src_stage];
      graph_data.addEdge(
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

  function addAllVertices(graph_data, step_map) {
    for (var key in step_map) {
      var step = step_map[key];
      graph_data.addNode(key, { label: addElephant(step) });
    }
  }

  function renderD3Graph(graph_data) {
    theG = d3.select("#flowgraph > g");
    renderViewLayout(graph_data);
    d3.select("#flowgraph").call(d3.behavior.zoom().on("zoom", zoomFunction));
  }

  function getAdjustedYPanelHeight() {
    return 183.0 - (parseInt(height) / 2.0);
  }

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

  function renderViewLayout(graph_data) {
    // draw the flow graph
    var renderer = new dagreD3.Renderer();
    var layout = renderer.layout().rankDir("LR");
    layout = renderer.layout(layout).run(graph_data, theG);
    // capture some state to maintain view during transformations/refreshes etc.
    height = layout.graph().height;
    StateUtil.captureGraphViewYOffset(getAdjustedYPanelHeight());
  }

  // load graph view state or set init values, call zoom event to exec
  function updateViewState(step_map) {
    state = StateUtil.getFlowState(flow_id);
    checkPreviouslySelectedVertex(step_map);
    theG.attr("transform", stringifyTransform(state));
  }

  function registerHover(step) {
    $("#step-image-" + step.stepnumber).hover(
      function(e) {
        var state = StateUtil.getFlowState(flow_id);
        if (state.curid !== 0) {
          $("#step-image-" + state.curid).removeClass("hi-lite");
          $("#step-" + state.curid).focus().hide();
        }
        $("#no-step").hide();

        StateUtil.updateCurrentId(step.stepnumber);
        $(this).addClass("hi-lite").focus();
        var mrTitle = $("#mrdetail-title");
        mrTitle.html(ViewUtil.renderStepStatus(step)).show().focus();

        $("#step-" + StateUtil.getFlowState(flow_id).curid).fadeIn(200);
      },
      function(e) {
        e.stopPropagation();
      }
    );
    $("#step-" + step.stepnumber).hide();
  }

  function zoomFunction() {
      var result = "translate(" + d3.event.translate + ") scale(" + d3.event.scale + ")";
      var state = StateUtil.getFlowState(flow_id);
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
      if (flow_id && flow_id !== "")  {
        setViewState(flow_id);
      }
    });
  }

  // converts string 'transform' attribute -> JSON string using
  // sessionStorage (with flow_id as key) to retain after page refresh
  function setViewState() {
    var raw = d3.select("#flowgraph > g").attr("transform");
    var tMatch = translateRegex.exec(raw);
    var sMatch = scaleRegex.exec(raw);
    var transx = tMatch[1];
    var transy = tMatch[2];
    var scalexy = sMatch[1];
    StateUtil.updateViewState(transx, transy, scalexy);
    StateUtil.setFlowState(flow_id);
  }

  // if there was a previous selected state, restore it - but store the
  // color of the current selected job stage from using fresh page data
  // as it could have been updated during page refresh.
  function checkPreviouslySelectedVertex(step_map) {
    var state = StateUtil.getFlowState(flow_id);
    if (state.curid !== 0) {
      $("#no-step").remove();
      var curImg = $("#step-image-" + state.curid);
      curImg.removeClass("hi-lite");
      $("#mrdetails-title").html(ViewUtil.renderStepStatus(step_map[state.curid])).show();
      $("#step-" + state.curid).show();
      curImg.trigger('mouseover');
    }
  }

  function stringifyTransform(data) {
    return "translate(" + data.tx + "," + data.ty + ") scale(" + data.scale + ")";
  }

  return graph;

}(jQuery, d3, dagreD3, ViewUtil, ChartUtil, StateUtil));
