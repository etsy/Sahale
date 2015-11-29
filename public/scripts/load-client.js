$(document).ready(function() {

  LoadUtil.populateDropDown();

  // populate and render tables
  $.get('/flows/running', function(flows_data) {
    // unpack and filter for particular cluster if needed
    var flows = DataUtil.unpackFlows(flows_data).filter(function(f) {
      return LoadUtil.filterForCluster(cluster_name, f.cluster_name);
    });

    LoadUtil.makeJobs(flows);
  });
});

$(document).ajaxStop(function() {
  LoadUtil.makeLinks();
  $('.waitforrender').remove();
  LoadUtil.render();

  // TODO: set refresh interval or leave it to the user?
  setTimeout( function() { location.reload(); }, 90 * 1000 );
});

