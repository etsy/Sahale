$(document).ready(function() {
  var params = URI(window.location.href).search(true);

  $.get('/flow_history/' + flow_name, function(raw) {
      var flows = DataUtil.unpackFlows(raw);
      if (params.cluster !== undefined) {
        flows = flows.filter(function(item) {
          return item.cluster_name === params.cluster;
        });
      }
      var aggr_flow_data = {};
      flows.forEach(function(item, ndx, arr) {
        aggr_flow_data[item.flow_id] = {
          maptasks: 0,
          reducetasks: 0,
          diskwrites: 0,
          hdfswrites: 0
        };
      });
    var data = 'flows=' + JSON.stringify(Object.keys(aggr_flow_data));

    // render the chart-style flow list for this job at the bottom of the page
    ViewUtil.renderCompletedJobs(flows, params.cluster);

    // get the (flow => steps) rollup data we need to aggregate for these charts
    $.post('/step_group', data, function(steps, code, xhr) {
      ChartUtil.aggregateStepData(aggr_flow_data, DataUtil.unpackSteps(steps));
      $('.waitforrender').remove();

      ChartUtil.renderBarChart('#runningtimes', ChartUtil.getRunningTimeData(flows), 'Minutes');
      ChartUtil.renderBarChart('#slotcounts', ChartUtil.getMapReduceSlotData(flows, aggr_flow_data), 'Slots');
      ChartUtil.renderBarChart('#hdfswrites', ChartUtil.getHdfsWriteData(flows, aggr_flow_data), 'Gigabytes');
      ChartUtil.renderBarChart('#diskwrites', ChartUtil.getDiskWriteData(flows, aggr_flow_data), 'Gigabytes');

      // set refresh interval
      setTimeout( function() { location.reload(); }, 60 * 1000 );
    });
  });
});

