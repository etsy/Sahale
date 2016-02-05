$(document).ready(function() {
  var params = URI(window.location.href).search(true);

  $.get('/flow_history/' + flow_name, function(raw) {
      var flows = DataUtil.unpackFlows(raw);
      // temporary shim for transition to v0.8 FT
      flows.filter(function(item) {
        return item.aggregated !== undefined;
      });
      if (params.cluster !== undefined) {
        flows = flows.filter(function(item) {
          return item.cluster_name === params.cluster;
        });
      }

      // render the chart-style flow list for this job at the bottom of the page
      ViewUtil.renderCompletedJobs(flows, params.cluster);

      ChartUtil.renderBarChart('#runningtimes', ChartUtil.getRunningTimeData(flows), 'Minutes');
      ChartUtil.renderBarChart('#slotcounts', ChartUtil.getMapReduceSlotData(flows), 'Tasks');
      ChartUtil.renderBarChart('#hdfswrites', ChartUtil.getHdfsWriteData(flows), 'Gigabytes');
      ChartUtil.renderBarChart('#diskwrites', ChartUtil.getDiskWriteData(flows), 'Gigabytes');

      ChartUtil.renderBarChart('#hdfsread', ChartUtil.getHdfsReadData(flows), 'Gigabytes');
      ChartUtil.renderBarChart('#diskread', ChartUtil.getDiskReadData(flows), 'Gigabytes');
      ChartUtil.renderBarChart('#vcoresecs', ChartUtil.getVcoreSecs(flows), 'Seconds');

      ChartUtil.renderBarChart('#cpusecs', ChartUtil.getCpuSecs(flows), 'Seconds');

      // set refresh interval
      setTimeout( function() { location.reload(); }, 60 * 1000 );
  });
});

