$(document).ready(function() {

    jumpToLinks = function(stepNum) {
	$('.nav-tabs a[href="#links-' + stepNum + '"]').tab('show');
    }

  // extract the Flow ID (for db query) from this page's URL path
  var flow_id = document.URL.substring(document.URL.lastIndexOf('/') + 1);
  var graph_data = new dagreD3.Digraph();
  var step_map = null;

  // API call to get the Cascading Flow metrics for this Flow ID
  $.get('/flow/' + flow_id, function(flow_data) {

    // should only be one row returned
    var flow = DataUtil.unpackFlow(flow_data[0]);

    // set up nav bar button links, job name, etc.
    GraphUtil.setNavigationLinks(flow);

    // render the table of aggregated Flow metrics at top of page
    ViewUtil.renderRunningJobs([flow]);

    // API call to get individual MapReduce jobs associated with one Flow ID
    $.get('/steps/' + flow.flow_id, function(step_data) {

      // build the state needed and add vertices to the graph visualization
      step_map = DataUtil.buildStepNumberToStepMap(step_data);

      // API call to get edge mapping between vertices in the graph, by Flow ID
      $.get('/edges/' + flow.flow_id, function(edge_data) {

        GraphUtil.renderFlowGraph(graph_data, step_map, edge_data);

        // extract browser state if there is any
        StateUtil.getFlowState(flow.flow_id);

        // render metrics views
        ViewUtil.renderMapReducePanels(step_map, flow);

        // render toggle-able area charts
        ToggleUtil.renderCharts(step_map);

        // render the running times chart
        StackedBarUtil.render(step_map, flow);

        // get event handling wired up
        GraphUtil.setEventHandlers(step_map);

        // set refresh interval only if the Flow isn't finished running
        if (["FAILED", "STOPPED", "SUCCESSFUL"].indexOf(flow.flow_status) < 0) {
          setTimeout( function() { location.reload(); }, 30 * 1000 );
        }
      });
    });
  });
});

