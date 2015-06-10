var sqlutil = require('../public/scripts/sql-util.js');


//////////// VIEW ROUTES ///////////////////////////////
exports.index = function(req, res) {
  res.render('index');
};

exports.help = function(req, res) {
  res.render('help');
};

exports.history = function(req, res) {
  res.render('history', { flow_name: req.param('flow_name') });
};

exports.flowgraph = function(req, res) {
  res.render('flowgraph', { flow_id: req.param('flow_id') });
};

exports.search = function(req, res) {
  var st = decodeURIComponent(req.param('searchterm'));
  res.render('search', { search_term: st });
}

/////////// API ENDPOINTS RETURNING JSON DATA /////////
exports.cluster_name_mapping = function(req, res) {
    sqlutil.getClusterNameMapping(
	function(data) { res.json(data); }
    )
}

exports.flows_running = function(req, res) {
  sqlutil.getRunningFlows(
    function(data) { res.json(data); }
  );
};

exports.flows_completed = function(req, res) {
  sqlutil.getCompletedFlows(
    function(data) { res.json(data); }
  );
};

exports.flow_search = function(req, res){
  sqlutil.getMatchingFlows(
    req.param('searchterm'),
    function(data) { res.json(data); }
  );
};

exports.steps = function(req, res){
  sqlutil.getStepsByFlowId(
    req.param('flow_id'),
    function(data) { res.json(data); }
  );
};

exports.flow = function(req, res){
  sqlutil.getFlowByFlowId(
    req.param('flow_id'),
    function(data) { res.json(data); }
  );
};

exports.edges = function(req, res) {
  sqlutil.getEdgesByFlowId(
    req.param('flow_id'),
    function(data) { res.json(data); }
  );
};

exports.flow_history = function(req, res) {
  sqlutil.getFlowsByJobName(
    req.param('flow_name'),
    function(data) { res.json(data); }
  );
}

exports.step_group = function(req, res) {
  sqlutil.getStepsByManyFlowIds(
    req.body.flows,
    function(data) { res.json(data); }
  );
}

exports.insert_or_update_flow = function(req, res) {
  sqlutil.upsertFlow(req.param('flow_id'), req.body);
  res.end();
}

exports.insert_or_update_steps = function(req, res) {
  sqlutil.upsertSteps(req.param('flow_id'), req.body);
  res.end();
}

exports.insert_or_update_edge = function(req, res) {
  sqlutil.upsertEdges(req.param('flow_id'), req.body);
  res.end();
}
