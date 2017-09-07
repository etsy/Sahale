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

exports.load = function(req, res) {
  res.render('load', { cluster_name: req.param('cluster') });
}

exports.search = function(req, res) {
  var st = decodeURIComponent(req.param('searchterm'));
  res.render('search', { search_term: st });
}

/////////// API ENDPOINTS RETURNING JSON DATA /////////

json_callback = function(req, res) {
  return function(error, data) {
    if (error == null) {
      res.json(data);
    } else {
      console.log(error.stack)
      res.status(500).send("Internal error:" + error.message)
    }
  }
}

exports.sahale_config_data = function(req, res) {
    sqlutil.getSahaleConfigData(json_callback(req, res));
}

exports.flows_running = function(req, res) {
  sqlutil.getRunningFlows(json_callback(req, res));
};

/**
 * DEPRECATED
 * Use flows_completed_all_ids and the individual flow API.
 */
exports.flows_completed = function(req, res) {
  sqlutil.getCompletedFlows(json_callback(req, res));
};

exports.flows_completed_all_ids = function(req, res) {
  sqlutil.getAllCompletedFlowIds(json_callback(req, res));
};

exports.flows_completed_all = function(req, res) {
  sqlutil.getAllCompletedFlows(json_callback(req, res));
};

exports.flow_search = function(req, res){
  sqlutil.getMatchingFlows(
    req.param('searchterm'),
    json_callback(req, res)
  );
};

exports.steps = function(req, res){
  sqlutil.getStepsByFlowId(
    req.param('flow_id'),
    json_callback(req, res)
  );
};

exports.flow = function(req, res){
  sqlutil.getFlowByFlowId(
    req.param('flow_id'),
    json_callback(req, res)
  );
};

exports.edges = function(req, res) {
  sqlutil.getEdgesByFlowId(
    req.param('flow_id'),
    json_callback(req, res)
  );
};

exports.flow_history = function(req, res) {
  sqlutil.getFlowsByJobName(
    req.param('flow_name'),
    json_callback(req, res)
  );
}

exports.step_group = function(req, res) {
  sqlutil.getStepsByManyFlowIds(
    req.body.flows,
    json_callback(req, res)
  );
}

exports.agg_by_flow = function(req, res) {
  sqlutil.getAggByFlowId(
      req.param('flow_id'),
      json_callback(req, res)
  );
}

exports.agg_by_epoch_start_end = function(req, res) {
  sqlutil.getAggByEpochMsStartEnd(
      req.param('flow_id'),
      json_callback(req, res)
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

exports.insert_flow_agg = function(req, res) {
   sqlutil.insertAggregation(req.body);
   res.end();
}

