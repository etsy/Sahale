// required libs
var fs = require('fs');
var mysql = require('mysql');

// users must customize the 'db-config.json' file and
// create the required tables before running the app
var dsn_json = '{}';
var dsn = null;
var flowTable = '';
var stepTable = '';
var edgeTable = '';
var aggTable  = '';

fs.readFile('db-config.json', 'UTF-8', function(err, data) {
  if (err) {
    return console.log(err);
  }
  dsn_json = data;
  dsn = JSON.parse(data);
  flowTable = dsn['flow_table_alias'] || 'cascading_job_flows';
  stepTable = dsn['step_table_alias'] || 'cascading_job_steps';
  edgeTable = dsn['edge_table_alias'] || 'cascading_job_edges';
  aggTable  = dsn['agg_table_alias']  || 'cascading_job_aggregated';
});
var removeFromDsn = [ 'user', 'password', 'host', 'port', 'database',
                      'flow_table_alias', 'step_table_alias',
                      'edge_table_alias', 'agg_table_alias' ];

// The time windows (secs back from 'now') for
// running and recently completed Flows to query
var small_time_offset = 60 * 10; // 10 minutes
var big_time_offset = 21600;
var one_week_offset = 86400 * 7; // for DEBUG purposes only


////////////////// Exported public functions ////////////////////
exports.getSahaleConfigData = function(call_back) {
  var sahaleConfigData = JSON.parse(dsn_json); // fresh copy can be mutated
  for (i in removeFromDsn) {
    sahaleConfigData[removeFromDsn[i]] = '';
  }
  call_back(sahaleConfigData);
}

exports.getRunningFlows = function(call_back) {
  var now       = getNowEpoch();
  var recent    = now - small_time_offset;
  executeQuery(
    "SELECT * FROM " + flowTable + " WHERE update_date > ? AND flow_status IN ('RUNNING', 'SUBMITTED', 'STARTED') ORDER BY create_date DESC ",
    [recent],
    call_back
  );
}

exports.getCompletedFlows = function(call_back) {
  var now       = getNowEpoch();
  var completed = now - big_time_offset;
  executeQuery(
    'SELECT * FROM ' + flowTable + ' ' +
    "WHERE update_date > ? AND flow_status IN ('STOPPED', 'FAILED', 'SUCCESSFUL', 'SKIPPED') " +
    'ORDER BY create_date DESC ',
    [completed],
    call_back
  );
}

exports.getAllCompletedFlows = function(call_back) {
  var now       = getNowEpoch();
  var completed = 0;
  executeQuery(
    'SELECT * FROM ' + flowTable + ' ' +
    "WHERE update_date > ? AND flow_status IN ('STOPPED', 'FAILED', 'SUCCESSFUL', 'SKIPPED') " +
    'ORDER BY create_date DESC ',
    [completed],
    call_back
  );
}

exports.getMatchingFlows = function(search_term, call_back) {
  var like = '%' + decodeURIComponent(search_term).replace('+', ' ').trim().replace(/\s+/g, "%") + '%';
  executeQuery(
    'SELECT * FROM ' + flowTable + ' ' +
    "WHERE flow_name LIKE ?" +
    'ORDER BY create_date DESC LIMIT 40',
    [like],
    call_back
  );
}

exports.getFlowsByJobName = function(jobName, call_back) {
  executeQuery(
    'SELECT * FROM ' + flowTable + ' ' +
    "WHERE flow_name = ? AND flow_status IN ('STOPPED', 'FAILED', 'SUCCESSFUL')" +
    'ORDER BY create_date DESC LIMIT 40',
    [jobName],
    call_back
  );
}

exports.getStepsByManyFlowIds = function(str, call_back) {
  var flow_ids = JSON.parse(str);
  var args = '';
  flow_ids.forEach(function(item, ndx, arr) {
    args += "'" + item + "'";
    if (ndx < (flow_ids.length - 1)) { args += ", " }
  });
  executeQuery(
    'SELECT * from ' + stepTable + ' WHERE flow_id IN (' + args + ') ',
    flow_ids,
    call_back
  );
}

exports.getFlowByFlowId = function(flow_id, call_back) {
  executeQuery('SELECT * from ' + flowTable + ' WHERE flow_id = ?', [flow_id], call_back);
}

exports.getStepsByFlowId = function(flow_id, call_back) {
  executeQuery('SELECT * FROM ' + stepTable + ' WHERE flow_id = ?', [flow_id], call_back);
}

exports.getEdgesByFlowId = function(flow_id, call_back) {
  executeQuery('SELECT * from ' + edgeTable + ' WHERE flow_id = ?', [flow_id], call_back);
}

exports.getAggByFlowId = function(flow_id, call_back) {
  executeQuery('SELECT * from ' + aggTable + ' WHERE flow_id = ?', [flow_id], call_back);
}

exports.getAggByEpochMsStartEnd = function(start, end, call_back) {
  executeQuery(
      'SELECT * from ' + aggTable +
      ' WHERE epoch_ms >= ? AND epoch_ms =< ?',
      [start, end], call_back);
}

// DEBUG
function inspect(str, obj) {
  console.log(str);
  console.log(obj);
}

///////////// REST API to update db via POST requests from running Cascading jobs //////////////
var emptyCallback = function(x) { };

exports.upsertFlow = function(flow_id, data) {
  var epoch_now = getNowEpoch();
  //inspect("[FLOW UPSERT]", data); // DEBUG
  var flow_name = data['flow_name'];
  var flow_status = data['flow_status'];
  // don't record old-data-model updates (shim for transition to FT 0.8.x)
  if (data['aggregated'] !== undefined) {
    var json = JSON.stringify(data); // re-encode for storage
    var argz = [
      flow_id, flow_name, flow_status, json, epoch_now, epoch_now, // for insert
      flow_id, flow_name, flow_status, json, epoch_now             // for update
    ];
    executeQuery('INSERT INTO ' + flowTable + ' ' +
      '(flow_id, flow_name, flow_status, flow_json, update_date, create_date) ' +
      'VALUES(?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ' +
      'flow_id=?, flow_name=?, flow_status=?, flow_json=?, update_date=?, create_date=create_date',
      argz,
      emptyCallback
    );
  }
}

exports.upsertSteps = function(flow_id, data) {
  var epoch_now = getNowEpoch();
  //inspect("[STEPS UPSERT]", data); // DEBUG
  for (ndx in data) {
    var json = JSON.stringify(data[ndx]); // re-encode for storage
    var step_id = data[ndx].step_id
    var argz = [
      step_id, flow_id, json, epoch_now, epoch_now, // for insert
      step_id, flow_id, json, epoch_now             // for update
    ];
    executeQuery('INSERT INTO ' + stepTable + ' ' +
      '(step_id, flow_id, step_json, update_date, create_date) ' +
      'VALUES(?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ' +
      'step_id=?, flow_id=?, step_json=?, update_date=?, create_date=create_date',
      argz,
      emptyCallback
    );
  }
}

exports.upsertEdges = function(flow_id, data) {
  var epoch_now = getNowEpoch();
  //inspect("[EDGES UPSERT]", data); // DEBUG
  for (k in data) {
    for (v_ndx in data[k]) { // produces array indexes into v_list :(
      executeQuery('INSERT INTO ' + edgeTable + ' ' +
        '(flow_id, src_stage, dest_stage, update_date, create_date) ' +
        'VALUES(?, ?, ?, ?, ?)',
        [ flow_id, k, data[k][v_ndx], epoch_now, epoch_now ],
        emptyCallback
      );
    }
  }
}

exports.insertAggregation = function(data) {
  //inspect("[AGG UPSERT]", data); // DEBUG
  var json = JSON.stringify(data); // re-encode for storage
  var flow_id = data.flow_id;
  var epoch_ms = data.epoch_ms;
  var argz = [
    flow_id, epoch_ms, json, // insert
    flow_id, epoch_ms, json  // update
  ];
  executeQuery('INSERT INTO ' + aggTable + ' ' +
    '(flow_id, epoch_ms, agg_json) ' +
    'VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE ' +
    'flow_id=?, epoch_ms=?, agg_json=?',
    argz,
    emptyCallback
  );
}


/////////////////// Utility functions ///////////////////////////
function executeQuery(query, param_array, call_back) {
  try {
    var conn = mysql.createConnection(dsn);
    conn.query(query, param_array, function(err, rows) {
      if (err) {
        console.log("Error during query in sql-utils. Stack trace: " + err.stack);
        // Ignore from here, we don't want the server to crash, just let this get messy and move on!
      }
      //console.log(rows); // DEBUG
      call_back(rows);
    });
  } catch(err) {
    console.log("Connection error in sql-utils. Stack trace: " + err.stack);
    // Ignore from here, we don't want the server to crash, just let this get messy and move on!
  } finally {
    if (conn) { conn.end(); }
  }
}

function getNowEpoch() {
  return Math.round((new Date).getTime() / 1000.0);
}
