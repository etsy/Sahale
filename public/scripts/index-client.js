$(document).ready(function() {

  // populate and render tables
  $.get('/flows/running', function(data) {
    ViewUtil.renderRunningJobs(DataUtil.unpackFlows(data));
  });

  $.get('/flows/completed', function(data) {
    ViewUtil.renderCompletedJobs(DataUtil.unpackFlows(data));
  });

  // set refresh interval
  setTimeout( function() { location.reload(); }, 25 * 1000 );
});
