/////////////////////////////// For DagreD3 Graph View ////////////////////////////////////
var GraphUtil = (function($, d3, dagreD3, ViewUtil, ChartUtil, StateUtil) {

    // Global module state
    var graph  = {},
	flow_id  = "",
	theG     = null
    ;


    //////////////////////// GraphUtil public functions //////////////////////
    graph.setNavigationLinks = function(flow) {
	flow_id = flow.flow_id;
	$('#jobname').text(flow.truncated_name);
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
	registerTabShown();
	updateUnloadHandler();
	updateViewState(step_map);
    }

    graph.renderFlowGraph = function(graph_data, step_map, edges) {
	addAllVertices(graph_data, step_map);
	addAllEdges(graph_data, step_map, edges);
	renderD3Graph(graph_data);

	// ugly hack to handle chrome wierdness with SVG rendering
	if (navigator.userAgent.toLowerCase().indexOf('chrome') > -1) {
	    $("#flowgraph > g .node.enter > rect").css("opacity", "0");
	}
    }


    /////////////////// private utility methods //////////////////////
    function addAllEdges(graph_data, step_map, rows) {
	//console.log(JSON.stringify(step_map)); // DEBUG
	rows.forEach(function(item, ndx, arr) {
	    var step = step_map[item.src_stage];
	    graph_data.addEdge(
		null,
		item.src_stage.toString(),
		item.dest_stage.toString(),
		{
		    label: '<span style="font-size:14px;color:' + ChartUtil.getColorForNumBytes(step.hdfs_bytes_written) + '">' +
			ViewUtil.prettyPrintBytes(step.hdfs_bytes_written) + '</span>'
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

    function addElephant(item) {
	return '<img width="40px" height="40px" ' +
	    'title="MapReduce Job (Step ' + item.step_number + ' of Flow)\n' + getStepDescription(item) +  '" ' +
	    'style="width:40px;height:40px;background-color:' +
	    ViewUtil.getColorByStepStatus(item) + '" ' +
	    'src="' + getImageByStepStatus(item) + '" ' +
	    'class="img-rounded" ' +
	    'id="step-image-' + item.step_number + '" />';
    }

    function getImageByStepStatus(item) {
	switch(item.step_status) {
	case "RUNNING": case "SUMBITTED": case "SUCCESSFUL":
            return "/images/happy_elephant.png";
	case "FAILED":
            return "/images/sad_elephant.png";
	default:
            return "/images/skeptical_elephant.png";
	}
    }

    function getStepDescription(step) {
	return step.config_props["scalding.step.descriptions"] || ""
    }

    function renderViewLayout(graph_data) {
	// draw the flow graph
	var renderer = new dagreD3.Renderer();
	var layout = renderer.layout().rankDir("LR");
	layout = renderer.layout(layout).run(graph_data, theG);
	// capture some state to maintain view during transformations/refreshes etc.
	StateUtil.captureGraphViewYOffset(layout.graph().height);
    }

    // load graph view state or set init values, call zoom event to exec
    function updateViewState(step_map) {
	var state = StateUtil.getFlowState(flow_id);
	setPreviouslySelectedVertex(step_map);
	setPreviouslyActiveTabs();
	theG.attr("transform", stringifyTransform(state));
    }

    function setPreviouslyActiveTabs() {
	// tabs is like { "12" : "sometab-12", "3" : "othertab-3", ... }
	var tabs = StateUtil.getTabState();
	for (key in tabs) {
	    $(".nav-tabs a[href$=" + tabs[key] + "]").tab("show");
	}
    }

    function registerTabShown() {
	$(document).on('shown.bs.tab', 'a[data-toggle="tab"]', function (e) {
	    //console.debug(e.target.href);
	    var result = /[a-z_]+-([0-9]+)/.exec(e.target.href);
	    StateUtil.updateTabState(result[1], result[0]);
	});
    }

    function registerHover(step) {
	$("#step-image-" + step.step_number).hover(
	    function(e) {
		var state = StateUtil.getFlowState(flow_id);
		if (state.curid !== 0) {
		    $("#step-image-" + state.curid).removeClass("hi-lite");
		    $("#step-" + state.curid).focus().hide();
		}
		$("#no-step").hide();

		StateUtil.updateCurrentId(step.step_number);
		$(this).addClass("hi-lite").focus();
		$("#mrdetail-title").html(ViewUtil.renderStepStatus(step)).show().focus();
		$("#step-" + StateUtil.getFlowState(flow_id).curid).fadeIn(200);
	    },
	    function(e) {
		e.stopPropagation();
	    }
	);
	$("#step-" + step.step_number).hide();
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
		var raw = d3.select("#flowgraph > g").attr("transform");
		var tMatch = /translate\(\s*([0-9.-]+)\s*,\s*([0-9.-]+)\)/.exec(raw);
		var sMatch = /scale\(\s*([0-9.-]+)\s*\)/.exec(raw);
		var transx = tMatch[1];
		var transy = tMatch[2];
		var scalexy = sMatch[1];
		StateUtil.updateViewState(transx, transy, scalexy);
		StateUtil.setFlowState(flow_id);
	    }
	});
    }

    // if there was a previous selected state, restore it - but store the
    // color of the current selected job stage from using fresh page data
    // as it could have been updated during page refresh.
    function setPreviouslySelectedVertex(step_map) {
	var state = StateUtil.getFlowState(flow_id);
	var curId = state.curid === 0 ? 1 : state.curid
	if (curId !== 0) {
	    $("#no-step").remove();
	    var curImg = $("#step-image-" + curId);
	    curImg.removeClass("hi-lite");
	    $("#mrdetails-title").html(ViewUtil.renderStepStatus(step_map[curId])).show();
	    $("#step-" + curId).show();
	    curImg.trigger('mouseover');
	}
    }

    function stringifyTransform(data) {
	return "translate(" + data.tx + "," + data.ty + ") scale(" + data.scale + ")";
    }

    return graph;

}(jQuery, d3, dagreD3, ViewUtil, ChartUtil, StateUtil));
