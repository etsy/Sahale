$(document).ready(function() {
  // TODO: this feels wrong but makes Express and Bootstrap play nice?
  var searchTerm = window.location.href.substr(
    window.location.href.lastIndexOf("?searchterm=") + 12,
    window.location.href.length
  );

  // populate and render results table
  $.get('/flows/search/' + searchTerm, function(data) {
    ViewUtil.renderMatchedJobs(DataUtil.unpackFlows(data));
  });

});
