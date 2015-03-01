// Setup and callback chains for the Flow details page
$(document).ready(function() {
  // extract the Flow ID (for db query) from this page's URL path
  var flowName = document.URL.substring(document.URL.lastIndexOf('/') + 1);

  $.get('/flow_history/' + flowName, function(theFlows) {
    var unpacked = DataUtil.unpackFlows(theFlows);
    var flowAgg = {};
    unpacked.forEach(function(item, ndx, arr) {
      flowAgg[item.flow_id] = {
        maptasks: 0,
        reducetasks: 0,
        diskwrites: 0,
        hdfswrites: 0
      };
    });
    var data = "flows=" + Object.keys(flowAgg).join("~");

    // render the chart-style flow list for this job at the bottom of the page
    ViewUtil.renderCompletedJobs(unpacked);

    // get the (flow => steps) rollup data we need to aggregate for these charts
    $.post('/step_group', data, function(theSteps, code, xhr) {
      ChartUtil.aggregateStepData(flowAgg, DataUtil.unpackSteps(theSteps));
      ChartUtil.renderBarChart("#runningtimes", ChartUtil.getRunningTimeData(unpacked), "Minutes");
      ChartUtil.renderBarChart("#slotcounts", ChartUtil.getMapReduceSlotData(unpacked, flowAgg), "Slots");
      ChartUtil.renderBarChart("#hdfswrites", ChartUtil.getHdfsWriteData(unpacked, flowAgg), "Gigabytes");
      ChartUtil.renderBarChart("#diskwrites", ChartUtil.getDiskWriteData(unpacked, flowAgg), "Gigabytes");

      // set refresh interval
      setTimeout( function() { location.reload(); }, 60 * 1000 );
    });
  });
});

