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
var smallTimeOffset = 30;
var bigTimeOffset = 21600;
var oneWeekOffset = 86400 * 7; // for DEBUG purposes only


////////////////// Exported public functions ////////////////////
exports.getRunningFlows = function(callBack) {
  var now       = getNowEpoch();
  var recent    = now - smallTimeOffset;
  executeQuery(
    'SELECT * FROM cascading_job_flows WHERE update_date > ? ORDER BY create_date DESC ',
    [recent],
    callBack
  );
}

exports.getCompletedFlows = function(callBack) {
  var now       = getNowEpoch();
  var completed = now - bigTimeOffset;
  executeQuery(
    'SELECT * FROM cascading_job_flows ' +
    "WHERE update_date > ? AND flow_status IN ('STOPPED', 'FAILED', 'SUCCESSFUL', 'SKIPPED') " +
    'ORDER BY create_date DESC ',
    [completed],
    callBack
  );
}

exports.getMatchingFlows = function(searchTerm, callBack) {
  var like = '%' + decodeURIComponent(searchTerm).replace('+', ' ').trim().replace(/\s+/g, "%") + '%';
  executeQuery(
    'SELECT * FROM cascading_job_flows ' +
    "WHERE flow_name LIKE ?" +
    'ORDER BY create_date DESC LIMIT 40',
    [like],
    callBack
  );
}

exports.getFlowsByJobName = function(jobName, callBack) {
  executeQuery(
    'SELECT * FROM cascading_job_flows ' +
    "WHERE flow_name = ? AND flow_status IN ('STOPPED', 'FAILED', 'SUCCESSFUL')" +
    'ORDER BY create_date DESC LIMIT 40',
    [jobName],
    callBack
  );
}

exports.getStepsByManyFlowIds = function(str, callBack) {
  var flowIds = str.split("~");
  var args = '';
  flowIds.forEach(function(item, ndx, arr) {
    args += "'" + item + "'";
    if (ndx < (flowIds.length - 1)) { args += ", " }
  });

  executeQuery(
    'SELECT * from cascading_job_steps WHERE flow_id IN (' + args + ') ',
    flowIds,
    callBack
  );
}

exports.getFlowByFlowId = function(flowId, callBack) {
  executeQuery('SELECT * from cascading_job_flows WHERE flow_id = ?', [flowId], callBack);
}

exports.getStepsByFlowId = function(flowId, callBack) {
  executeQuery('SELECT * FROM cascading_job_steps WHERE flow_id = ?', [flowId], callBack);
}

exports.getEdgesByFlowId = function(flowId, callBack) {
  executeQuery('SELECT * from cascading_job_edges WHERE flow_id = ?', [flowId], callBack);
}


///////////// REST API to update db via POST requests from running Cascading jobs //////////////
var emptyCallback = function(x) { };

exports.upsertFlow = function(flowId, data) {
  var epoch_now = getNowEpoch();
  var flowName = data['flowname'];
  var flowStatus = data['flowstatus'];
  var json = decodeURIComponent(data['json']);
  var argz = [
    flowId, flowName, flowStatus, json, epoch_now, epoch_now, // for insert
    flowId, flowName, flowStatus, json, epoch_now             // for update
  ];
  executeQuery('INSERT INTO cascading_job_flows ' +
    '(flow_id, flow_name, flow_status, flow_json, update_date, create_date) ' +
    'VALUES(?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ' +
    'flow_id=?, flow_name=?, flow_status=?, flow_json=?, update_date=?, create_date=create_date',
    argz,
    emptyCallback
  );
}

exports.upsertSteps = function(flowId, data) {
  var epoch_now = getNowEpoch();
  for (stepId in data) {
    var json = decodeURIComponent(data[stepId]);
    var argz = [
      stepId, flowId, json, epoch_now, epoch_now, // for insert
      stepId, flowId, json, epoch_now             // for update
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

exports.upsertEdges = function(flowId, data) {
  var epoch_now = getNowEpoch();
  for (k in data) {
    var v_list = data[k].split('|'); // produces array of values per key
    for (v_ndx in v_list) { // produces array indexes into v_list :(
      executeQuery('INSERT INTO cascading_job_edges ' +
        '(flow_id, src_stage, dest_stage, update_date, create_date) ' +
        'VALUES(?, ?, ?, ?, ?)',
        [ flowId, k, v_list[v_ndx], epoch_now, epoch_now ],
        emptyCallback
      );
    }
  }
}


/////////////////// Utility functions ///////////////////////////
function executeQuery(query, paramArray, callBack) {
  try {
    var conn = mysql.createConnection(dsn);
    conn.query(query, paramArray, function(err, rows) {
      if (err) {
        console.log("Error during query in sql-utils. Stack trace: " + err.stack);
        // Ignore from here, we don't want the server to crash, just let this get messy and move on!
      }
      //console.log(rows); // DEBUG
      callBack(rows);
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
