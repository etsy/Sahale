// Setup and callback chains for the Flow details page
$(document).ready(function() {
  // extract the Flow ID (for db query) from this page's URL path
  var flow_name = document.URL.substring(document.URL.lastIndexOf('/') + 1);

  $.get('/flow_history/' + flow_name, function(flows) {
    var unpacked = DataUtil.unpackFlows(flows);
    var aggr_flow_data = {};
    unpacked.forEach(function(item, ndx, arr) {
      aggr_flow_data[item.flow_id] = {
        maptasks: 0,
        reducetasks: 0,
        diskwrites: 0,
        hdfswrites: 0
      };
    });
    var data = "flows=" + Object.keys(aggr_flow_data).join("~");

    // render the chart-style flow list for this job at the bottom of the page
    ViewUtil.renderCompletedJobs(unpacked);

    // get the (flow => steps) rollup data we need to aggregate for these charts
    $.post('/step_group', data, function(steps, code, xhr) {
      ChartUtil.aggregateStepData(aggr_flow_data, DataUtil.unpackSteps(steps));
      ChartUtil.renderBarChart("#runningtimes", ChartUtil.getRunningTimeData(unpacked), "Minutes");
      ChartUtil.renderBarChart("#slotcounts", ChartUtil.getMapReduceSlotData(unpacked, aggr_flow_data), "Slots");
      ChartUtil.renderBarChart("#hdfswrites", ChartUtil.getHdfsWriteData(unpacked, aggr_flow_data), "Gigabytes");
      ChartUtil.renderBarChart("#diskwrites", ChartUtil.getDiskWriteData(unpacked, aggr_flow_data), "Gigabytes");

      // set refresh interval
      setTimeout( function() { location.reload(); }, 60 * 1000 );
    });
  });
});

