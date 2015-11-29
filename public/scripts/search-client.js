$(document).ready(function() {

  // populate and render results table
  $.get('/flows/search/' + search_term, function(data) {
    ViewUtil.renderMatchedJobs(DataUtil.unpackFlows(data));
  });

});
