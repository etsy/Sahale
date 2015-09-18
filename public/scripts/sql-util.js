// required libs
var fs = require('fs');
var mysql = require('mysql');

// users must customize the 'db-config.json' file and
// create the required tables before running the app
var dsn = null;
fs.readFile('db-config.json', 'UTF-8', function(err, data) {
  if (err) {
    return console.log(err);
  }
  dsn = JSON.parse(data);
});

// The time windows (secs back from 'now') for
// running and recently completed Flows to query
var small_time_offset = 60 * 10; // 10 minutes
var big_time_offset = 21600;
var one_week_offset = 86400 * 7; // for DEBUG purposes only


////////////////// Exported public functions ////////////////////
exports.getClusterNameMapping = function(call_back) {
    call_back(dsn['cluster_name_mapping']);
}

exports.getRunningFlows = function(call_back) {
  var now       = getNowEpoch();
  var recent    = now - small_time_offset;
  executeQuery(
    "SELECT * FROM cascading_job_flows WHERE update_date > ? AND flow_status IN ('RUNNING', 'SUBMITTED', 'STARTED') ORDER BY create_date DESC ",
    [recent],
    call_back
  );
}

exports.getCompletedFlows = function(call_back) {
  var now       = getNowEpoch();
  var completed = now - big_time_offset;
  executeQuery(
    'SELECT * FROM cascading_job_flows ' +
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
    'SELECT * FROM cascading_job_flows ' +
    "WHERE update_date > ? AND flow_status IN ('STOPPED', 'FAILED', 'SUCCESSFUL', 'SKIPPED') " +
    'ORDER BY create_date DESC ',
    [completed],
    call_back
  );
}

exports.getMatchingFlows = function(search_term, call_back) {
  var like = '%' + decodeURIComponent(search_term).replace('+', ' ').trim().replace(/\s+/g, "%") + '%';
  executeQuery(
    'SELECT * FROM cascading_job_flows ' +
    "WHERE flow_name LIKE ?" +
    'ORDER BY create_date DESC LIMIT 40',
    [like],
    call_back
  );
}

exports.getFlowsByJobName = function(jobName, call_back) {
  executeQuery(
    'SELECT * FROM cascading_job_flows ' +
    "WHERE flow_name = ? AND flow_status IN ('STOPPED', 'FAILED', 'SUCCESSFUL')" +
    'ORDER BY create_date DESC LIMIT 40',
    [jobName],
    call_back
  );
}

exports.getStepsByManyFlowIds = function(str, call_back) {
  var flow_ids = str.split("~");
  var args = '';
  flow_ids.forEach(function(item, ndx, arr) {
    args += "'" + item + "'";
    if (ndx < (flow_ids.length - 1)) { args += ", " }
  });

  executeQuery(
    'SELECT * from cascading_job_steps WHERE flow_id IN (' + args + ') ',
    flow_ids,
    call_back
  );
}

exports.getFlowByFlowId = function(flow_id, call_back) {
  executeQuery('SELECT * from cascading_job_flows WHERE flow_id = ?', [flow_id], call_back);
}

exports.getStepsByFlowId = function(flow_id, call_back) {
  executeQuery('SELECT * FROM cascading_job_steps WHERE flow_id = ?', [flow_id], call_back);
}

exports.getEdgesByFlowId = function(flow_id, call_back) {
  executeQuery('SELECT * from cascading_job_edges WHERE flow_id = ?', [flow_id], call_back);
}


///////////// REST API to update db via POST requests from running Cascading jobs //////////////
var emptyCallback = function(x) { };

exports.upsertFlow = function(flow_id, data) {
  var epoch_now = getNowEpoch();
  var flow_name = data['flowname'];
  var flow_status = data['flowstatus'];
  var json = decodeURIComponent(data['json']);
  var argz = [
    flow_id, flow_name, flow_status, json, epoch_now, epoch_now, // for insert
    flow_id, flow_name, flow_status, json, epoch_now             // for update
  ];
  executeQuery('INSERT INTO cascading_job_flows ' +
    '(flow_id, flow_name, flow_status, flow_json, update_date, create_date) ' +
    'VALUES(?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ' +
    'flow_id=?, flow_name=?, flow_status=?, flow_json=?, update_date=?, create_date=create_date',
    argz,
    emptyCallback
  );
}

exports.upsertSteps = function(flow_id, data) {
  var epoch_now = getNowEpoch();
  for (step_id in data) {
    var json = decodeURIComponent(data[step_id]);
    var argz = [
      step_id, flow_id, json, epoch_now, epoch_now, // for insert
      step_id, flow_id, json, epoch_now             // for update
    ];
    executeQuery('INSERT INTO cascading_job_steps ' +
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
  for (k in data) {
    var v_list = data[k].split('|'); // produces array of values per key
    for (v_ndx in v_list) { // produces array indexes into v_list :(
      executeQuery('INSERT INTO cascading_job_edges ' +
        '(flow_id, src_stage, dest_stage, update_date, create_date) ' +
        'VALUES(?, ?, ?, ?, ?)',
        [ flow_id, k, v_list[v_ndx], epoch_now, epoch_now ],
        emptyCallback
      );
    }
  }
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
