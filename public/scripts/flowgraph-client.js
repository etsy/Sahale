$(document).ready(function() {

  // extract the Flow ID (for db query) from this page's URL path
  var flowId = document.URL.substring(document.URL.lastIndexOf('/') + 1);
  var graphData = new dagreD3.Digraph();
  var stepMap = null;

  // API call to get the Cascading Flow metrics for this Flow ID
  $.get('/flow/' + flowId, function(flowData) {

    // should only be one row returned
    var theFlow = DataUtil.unpackFlow(flowData[0]);
    GraphUtil.captureFlowId(theFlow);

    // set up nav bar button links, job name, etc.
    GraphUtil.setNavigationLinks(theFlow);

    // render the table of aggregated Flow metrics at top of page
    ViewUtil.renderRunningJobs([theFlow]);

    // API call to get individual MapReduce jobs associated with one Flow ID
    $.get('/steps/' + theFlow.flow_id, function(stepData) {

      // build the state needed and add vertices to the graph visualization
      stepMap = GraphUtil.buildStepNumberToStepMap(stepData);

      // API call to get edge mapping between vertices in the graph, by Flow ID
      $.get('/edges/' + theFlow.flow_id, function(edgeData) {

        // populate Dagre D3 graph data structure
        GraphUtil.addAllVertices(graphData, stepMap);
        GraphUtil.addAllEdges(graphData, stepMap, edgeData);

        // render this thing
        GraphUtil.renderFlowGraph(graphData);

        // render metrics views
        ViewUtil.renderMapReducePanels(stepMap, theFlow);

        // render toggle-able area charts
        ToggleUtil.buildDatasets(stepMap);
        ToggleUtil.renderAndRegisterEvent();

        // render the running times chart
        StackedBarUtil.renderRunningTimes(stepMap, theFlow);

        // get event handling wired up
        GraphUtil.setEventHandlers(stepMap);

        // set refresh interval
        setTimeout( function() { location.reload(); }, 35 * 1000 );
      });
    });
  });
});

