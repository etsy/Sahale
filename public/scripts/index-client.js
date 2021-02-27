$(document).ready(function() {

  var params = URI(window.location.href).search(true);

  LoadUtil.populateDropDown();

  // populate and render tables
  $.get('/flows/running', function(data) {
      ViewUtil.renderRunningJobs(DataUtil.unpackFlows(data), params.cluster, false);
      $("#running").tablesorter();
  });

  $.get('/flows/completed', function(data) {
      ViewUtil.renderCompletedJobs(DataUtil.unpackFlows(data), params.cluster);
      $("#completed").tablesorter();
  });

  // set refresh interval
  setTimeout( function() { location.reload(); }, 25 * 1000 );
});
