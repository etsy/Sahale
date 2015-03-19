/////////////////////////////// For Table Views ////////////////////////////////////
var ViewUtil = (function($) {
  var view = {};

  ////////// public ViewUtil functions ////////////////
  view.renderRunningJobs = function(flows) {
    $("#running").hide().html(
        renderJobsTable(flows, "progress-bar-warning progress-bar-striped active")
      ).fadeIn(500);
  }

  view.renderCompletedJobs = function(flows) {
    $("#completed").hide().html(
        renderJobsTable(flows, "progress-bar-info")
      ).fadeIn(500);
  }

  view.renderMatchedJobs = function(flows) {
    $("#matched").hide().html(
        renderJobsTable(flows, "progress-bar-info")
      ).fadeIn(500);
  }

  view.renderMapReducePanels = function(stepMap, flow) {
    var html = '<div height="550px">' +
      '<div style="text-align:center" class="alert alert-info" id="no-step" role="alert">Select A Node</div>';
    for (key in stepMap) {
      var step = stepMap[key];
      html += '<div id="step-' + step.stepnumber + '">';
      html += renderHadoopLogLinks(step, flow);
      html += renderMapReduceProgress(step);
      html += renderTabHeader(step.stepnumber);
      html += '<div id="the-content-' + step.stepnumber + '" class="tab-content push-it-down">';
      html += '<div id="jobstats-' + step.stepnumber + '" class="tab-pane active">';
      html += renderMapReduceLocalityAndCounts(step);
      html += renderIo(step);
      html += '</div>';
      html += '<div id="sourcessinks-' + step.stepnumber + '" class="tab-pane">';
      html += renderTapsAndFields("danger", "Sources and Fields", step.sources, step.sourcesfields);
      html += renderTapsAndFields("info", "Sink and Fields", step.sink, step.sinkfields);
      html += '</div>';
      html += '<div id="hadoopcounters-' + step.stepnumber + '" class="tab-pane">';
      html += renderHadoopCounters(step);
      html += '</div>';
      html += '</div>';
      html += '</div>';
    }
    html += '</div>';
    $("#flowdetails").html(html);
  }

  view.getColorByStepStatus = function(step) {
    var color;
    switch(step.stepstatus) {
      case "RUNNING": case "SUBMITTED":
        color = "orange";
        break;
      case "SUCCESSFUL":
        color = "lightgreen";
        break;
      case "FAILED":
        color = "red";
        break;
      default: // PENDING, SKIPPED, STARTED, STOPPED, NOT_LAUNCHED
        color = "lightgray";
        break;
    }
    return color;
  }

  view.renderStepStatus = function(step) {
    var color = view.getColorByStepStatus(step);
    return 'Step ' + step.stepnumber + ':<span style="padding-left:8px;color:' + color + '">' +
      step.stepstatus + '</span>';
  }

  view.prettyPrintBytes = function(bytes) {
    var temp = bytes;
    var scale = ["B", "KB", "MB", "GB", "TB", "PB"];
    var index = 0;
    while(index < scale.length && temp > 1024) {
      index += 1;
      temp /= 1024.0;
    }
    return Math.round(temp, 2) + scale[index];
  }

  ///////////////// private utility functions ////////////////
  function renderJobsTable(flows, barStylez) {
    var rows = '<tr>' +
      '<th>Job Name</th>' +
      '<th>User</th>' +
      '<th>Status</th>' +
      '<th># of Steps</th>' +
      '<th>Running Time</th>' +
      '<th>Progress</th>' +
      '</tr>';

    for (var i = 0; i < flows.length; ++i) {
      var f = flows[i]
      var fp = function(f) { if (f.flow_status === "SUCCESSFUL") return "100.00"; else return f.flow_progress; }(f);
      rows += '<tr>' +
        '<td>' + prettyLinkedJobName(f.flow_name, f.flow_id) + '</td>' +
        '<td>' + f.user_name + '</td>' + // removed link to Staff page for OSS version
        '<td>' + prettyFlowStatus(f.flow_status) + '</td>' +
        '<td>' + f.total_stages + '</td>' +
        '<td>' + view.prettyFlowTimeFromMillis(f.flow_duration) + '</td>' +
        '<td>' + prettyProgress(fp, barStylez) + '</td>' +
        '</tr>';
    }
    return rows;
  }

  function renderTabHeader(idnum) {
    var html = '<ul id="tabs-step-' + idnum + '" class="nav nav-tabs" data-tabs="tabs">';
    html += '<li class="active"><a href="#jobstats-' + idnum + '" data-toggle="tab">Stats</a></li>';
    html += '<li><a href="#sourcessinks-' + idnum + '" data-toggle="tab">Taps</a></li>';
    html += '<li><a href="#hadoopcounters-' + idnum + '" data-toggle="tab">Counters</a></li>';
    html += '</ul>';
    return html;
  }

  function prettyLinkedJobName(name, flowId) {
    return '<a href="/flowgraph/' + flowId + '">' +
      name.replace('com.etsy.scalding.jobs.', '') +
      '</a>';
  }

  function prettyFlowStatus(flowStatus) {
    return '<span style="color:' + view.getFlowStatusColor(flowStatus) + '">' + flowStatus + '</span>';
  }

  view.getFlowStatusColor = function(flowStatus) {
    var color;
    switch(flowStatus) {
      case "RUNNING": case "SUBMITTED": case "SUCCESSFUL":
        color = "green";
        break;
      case "FAILED":
        color = "red";
        break;
      default: // PENDING, SKIPPED, STOPPED, STARTED
        color = "black";
        break;
    }
    return color;
  }

  function prettyProgress(prog, barStylez) {
    return '<div class="progress">' +
      '<div class="progress-bar ' + barStylez + '" role="progressbar" aria-valuenow="' +
      Math.floor(prog) + '" style="width:' + prog + '%;">' +
      prog + '%' +
      '</div>' +
      '</div>';
  }

  view.prettyFlowTimeFromMillis = function(msecs) {
    var sec_num = parseInt(msecs, 10) / 1000;
    var hours   = Math.floor(sec_num / 3600);
    var minutes = Math.floor((sec_num / 60) % 60);
    var seconds = Math.floor(sec_num % 60);
    if (minutes < 10) { minutes = "0" + minutes; }
    if (seconds < 10) { seconds = "0" + seconds; }
    return hours + ':' + minutes + ':' + seconds;
  }

  function renderMapReduceLocalityAndCounts(step) {
    var local = step.maptasks > 0 ? Math.round(100 * (step.datalocalmaptasks / step.maptasks), 4) : "Unknown";
    var rack = step.maptasks > 0 ? Math.round(100 * (step.racklocalmaptasks / step.maptasks), 4) : "Unknown";
    return '<table style="font-size:10px" class="table table-centered">' +
      '<tr>' +
      '<th>Data Local Mappers:</th><td>' + local + '%</td>' +
      '<th>Rack Local Mappers:</th><td>' + rack + '%</td>' +
      '</tr>' +
      '<tr>' +
      '<th>Map Tasks:</th><td class="mr-count-red">' + step.maptasks + '</td>' +
      '<th>Reduce Tasks:</th><td class="mr-count-blue">' + step.reducetasks + '</td>' +
      '</tr>' +
      '</table>';
  }

  function renderMapReduceProgress(step) {
    var m = Math.round(step.mapprogress / 2, 2);
    var r = Math.round(step.reduceprogress / 2, 2);
    return '<div class="progress">' +
      '<div class="progress-bar progress-bar-danger progress-bar-striped active" ' +
      'role="progressbar" aria-valuemin="0" aria-valuemax="50" aria-valuenow="' +
      Math.floor(m) + '" style="width:' + Math.floor(m) + '%">' + step.mapprogress + '% Map</div>' +
      '<div class="progress-bar progress-bar-info progress-bar-striped active" ' +
      'role="progressbar" aria-valuemin="0" aria-valuemax="50" aria-valuenow="' +
      Math.floor(r) + '" style="width:' + Math.floor(r) + '%">' + step.reduceprogress + '% Reduce</div>' +
      '</div>';
  }

  function renderHadoopLogLinks(step, flow) {
    var link = '#';
    if (step.jobid !== null && step.jobid !== 'NO_JOB_ID') {
      // Hack to point log links at JT for MRv1, YARN History Server for MRv2
      if (flow.yarn_job_history !== "false") {
        link = makeYarnUrl(flow, step);
      } else {
        link = 'http://' + flow.jt_url + ':50030/jobdetails.jsp?jobid=' + step.jobid + '&refresh=0';
      }
    }
    var text = '<div class="logbox"><a href="' + link + '"><b>View Hadoop Logs</b></a>';
    if (step.stepstatus == 'FAILED' && flow.yarn_job_history !== "false") {
      // We know this is a History Server link at this point, so this replacement should be valid
      var failedMapTasks = link.replace("/job/", "/attempts/") + "/m/FAILED"
      var failedReduceTasks = link.replace("/job/", "/attempts/") + "/r/FAILED"
      if (step.failedmaptasks > 0) {
          text += '<br/><a href="' + failedMapTasks + '"><b>Failed Map Tasks</b></a>'
      }
      if (step.failedreducetasks > 0) {
          text += '<br/><a href="' + failedReduceTasks + '"><b>Failed Reduce Tasks</b></a>'
      }
    }
    text += '</div>';

    return text;
  }

  function makeYarnUrl(flow, step) {
    var yarn_id = step.jobid.slice(4, step.jobid.length);
    switch(step.stepstatus) {
    case "SUCCESSFUL":
    case "FAILED":
    case "SKIPPED":
    case "STOPPED":
      var yarn_history = makeHistoryUrl(flow, '19888');
      var link = 'http://' + yarn_history + '/jobhistory/job/job_' + yarn_id;
      break;
    default:
      var yarn_history = makeHistoryUrl(flow, '8088');
      var link = 'http://' + yarn_history + '/proxy/application_' + yarn_id;
      break;
    }
    return link;
  }

  function makeHistoryUrl(flow, port) {
    var clip_port = flow.yarn_job_history.indexOf(':');
    var host = flow.yarn_job_history;
    if (clip_port > -1) {
      host = host.slice(0, clip_port);
    }
    return host + ':' + port;
  }

  function renderIo(step) {
    return '<div style="font-size:10px">' +
      '<table class="table table-centered table-striped">' +
      '<tr> <th>I/O Type</th> <th>Read</th> <th>Written</th> </tr>' +
      '<tr> <td>HDFS</td> ' +
      '<td>' + view.prettyPrintBytes(step.hdfsbytesread) + '</td>' +
      '<td>' + view.prettyPrintBytes(step.hdfsbyteswritten) + '</td>' +
      '</tr>' +
      '<tr> <td>Tuples</td> ' +
      '<td>' + step.tuplesread + '</td>' +
      '<td>' + step.tupleswritten + '</td>' +
      '</tr>' +
      '<tr> <td>Cluster Disk</td> ' +
      '<td>' + view.prettyPrintBytes(step.filebytesread) + '</td>' +
      '<td>' + view.prettyPrintBytes(step.filebyteswritten) + '</td>' +
      '</tr>' +
      '</table>' +
      '</div>';
  }

  function renderTapsAndFields(style, title, tapsEntry, fieldsEntry) {
    var taps = tapsEntry.split(',');
    var fields = fieldsEntry.split(';');
    var html = '<div class="panel panel-' + style + '" style="font-size:10px">' +
      '<div class="panel-heading" style="text-align:center">' +
      title + '</div><div class="panel-body">';
    var addSeparator = taps.length - 1;
    for (var i = 0; i < taps.length; ++i) {
      if (fields[i] === undefined) { fields[i] = "UNKNOWN"; }
      html += '<div style="overflow-x:auto">';
      html += '<strong style="margin-left:4px;margin-top:8px">' + taps[i] + '</strong>';
      html += '<div style="margin-left:4px">' + fields[i].replace(/,/g, ', ') + '</div>';
      html += '</div>';
      if (i < addSeparator) {
        html += '<div class="tapline" />';
      }
    }
    html += '</div></div>';
    return html;
  }

  function renderHadoopCounters(step) {
    var html = '';
    var groups = step['counters'];
    for (var groupKey in groups) {
      html += '<div class="panel panel-info" style="font-size:10px">';
      html +=  '<div class="panel-heading" style="text-align:center">' + groupKey + '</div>';
      html +=  '<div class="panel-body">';
      var group = groups[groupKey];
      for (var counterKey in group) {
        var value = group[counterKey];
        html += '<div style="overflow-x:auto;margin-left:4px">' + counterKey + ": " + value + '</div>';
        html += '<div class="tapline" />';
      }
      html += '</div></div>';
    }
    return html;
  }

  return view;

}(jQuery));
