// Table-style views are generated with these helpers
var ViewUtil = (function($) {
    var view = {};

    ////////// public ViewUtil functions ////////////////
    view.renderRunningJobs = function(flows, clusterFilter, isCompletedJobs) {
	$("#running").hide().html(
            renderJobsTable(flows, "progress-bar-warning progress-bar-striped active", clusterFilter, isCompletedJobs)
	).fadeIn(500);
    }

    view.renderCompletedJobs = function(flows, clusterFilter) {
	$("#completed").hide().html(
            renderJobsTable(flows, "progress-bar-info", clusterFilter, true)
	).fadeIn(500);
    }

    view.renderMatchedJobs = function(flows, clusterFilter) {
	$("#matched").hide().html(
            renderJobsTable(flows, "progress-bar-info", clusterFilter, true)
	).fadeIn(500);
    }

    view.renderMapReducePanels = function(stepMap, flow) {
	var html = '<div height="550px">' +
	    '<div style="text-align:center" class="alert alert-info" id="no-step" role="alert">Select A Node</div>';
	for (key in stepMap) {
	    var step = stepMap[key];

	    html += '<div id="step-' + step.step_number + '">';
	    html += renderMapReduceProgress(step);
	    html += renderTabHeader(step.step_number);
	    html += '<div id="the-content-' + step.step_number + '" class="tab-content push-it-down">';
	    html += '<div id="jobstats-' + step.step_number + '" class="tab-pane active">';
	    html += renderMapReduceLocalityAndCounts(step);
	    html += renderIo(step);
	    html += '</div>';
	    html += '<div id="sourcessinks-' + step.step_number + '" class="tab-pane">';
	    html += renderTapsAndFields("danger", "Sources and Fields", step.sources);
	    html += renderTapsAndFields("info", "Sink and Fields", step.sink);
	    html += '</div>';
	    html += '<div id="hadoopcounters-' + step.step_number + '" class="tab-pane">';
	    html += renderHadoopCounters(step);
	    html += '</div>';
	    html += '<div id="links-' + step.step_number + '" class="tab-pane">';
	    html += renderLinks(step, flow);
	    html += '</div>';
	    html += '</div>';
	    html += '</div>';
	}
	html += '</div>';
	$("#flowdetails").html(html);
    }

    view.renderInputsAndOutputs = function(stepMap, flow) {	
	var input_html = '<div class="logbox">';
	input_html += '<div style="font-size:10px">';
	var output_html = '<div class="logbox">';
	output_html += '<div style="font-size:10px">';

	for (key in stepMap) {
	    var step = stepMap[key]

	    for (tap in step.sources) {
		if (!tap.startsWith("/tmp") && input_html.indexOf(tap) == -1) {
		    input_html += '<div class="steplink"><p class=wordwrap>' + tap + '</p></div>';
		}
	    }

	    for (tap in step.sink) {
		if (!tap.startsWith("/tmp") && output_html.indexOf(tap) == -1) {
		    output_html += '<div class=steplink wordwrap><p class=wordwrap>' + tap + '</p></div>';
		}
	    }
	}

	input_html += '</div>';
	input_html += "</div>";

	output_html += '</div>';
	output_html += "</div>";

	$("#all_inputs").html(input_html);
	$("#all_outputs").html(output_html);
    }

    view.getColorByStepStatus = function(step) {
	var color;
	switch(step.step_status) {
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

    view.unhideFailLinks = function(flow) {
	switch(flow.flow_status) {
	case "RUNNING": case "SUBMITTED": case "SUCCESSFUL": case "PENDING": case "STARTED": case "SKIPPED":
            break;
	default: 
	    $("#fail_links_parent").removeClass('hidden');
            break;
	}
    }

    view.renderStepStatus = function(step) {
	var color = view.getColorByStepStatus(step);
	return 'Step ' + step.step_number + ':<span style="padding-left:8px;color:' + color + '">' +
	    step.step_status + '</span>';
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
    function renderJobsTable(flows, barStylez, clusterFilter, isCompletedJobs) {
	var rows = '<tr>' +
	    '<th>Job Name</th>' +
	    '<th>User</th>' +
	    '<th>Cluster</th>' +
	    '<th>Status</th>' +
	    '<th># of Steps</th>' +
	    '<th>Job Date</th>' +
	    '<th>Running Time</th>' +
	    '<th>Start Time</th>' +
	    (isCompletedJobs ? '<th>End Time</th>' : '<th>Progress</th>') +
	    '</tr>';
	for (var i = 0; i < flows.length; ++i) {
	    var f = flows[i];
	    if (clusterFilter === undefined || f.cluster_name === clusterFilter) {
		var fp = function(f) {
		    return f.flow_status === "SUCCESSFUL" ? 100.0 : f['aggregated']['flow_progress'];
		}(f);
		rows += '<tr>' +
		    '<td>' + prettyLinkedJobName(f) + '</td>' +
		    '<td>' + renderUsernameLink(f) + '</td>' +
		    '<td>' + renderClusterFilterLink(f.cluster_name) + '</td>' +
		    '<td>' + prettyFlowStatus(f.flow_status) + '</td>' +
		    '<td>' + f.total_stages + '</td>' +
		    '<td>' + renderJobDate(f) + '</td>' +
		    '<td>' + view.prettyFlowTimeFromMillis(f.flow_duration) + '</td>' +
		    '<td>' + renderDate(f.flow_start_epoch_ms) + '</td>' +
		    (isCompletedJobs ? '<td>' + renderDate(f.flow_end_epoch_ms) + '</td>' : '<td>' + prettyProgress(fp, barStylez) + '</td>') +
		    '</tr>';
	    }
	}
	return rows;
    }

    function renderUsernameLink(flow) {
	if (flow.username_link) {
	    return '<a href=' + flow.username_link + ' target=_blank>' + flow.user_name + '</a>';
	} else {
	    return flow.user_name;
	}
    }

    function renderJobDate(flow) {
	var args = flow['config_props']['scalding.job.args'];
	if (args !== undefined) {
	    var dateIndex = args.indexOf('--date');
	    if (dateIndex === -1) {
		return 'None';
	    }
	    var firstDate = args[dateIndex + 1].replace(/_/g, '-');
	    if (dateIndex + 2 <= args.length - 1 && args[dateIndex + 2].indexOf('--') === -1) {
		var secondDate = args[dateIndex + 2].replace(/_/g, '-');
	    }
	    
	    if (args.indexOf('--daily') != -1) {
		return firstDate;
	    } else {
		if (secondDate === undefined) {
		    return firstDate
		} else {
		    return firstDate + ' - ' + secondDate;
		}
	    }
	} else {
	    return 'Unknown';
	}
    }

    function renderClusterFilterLink(clusterName) {
	return '<a href=?cluster=' + clusterName + '>' + clusterName + '</a>';
    }

    function renderTabHeader(idnum) {
	var html = '<ul id="tabs-step-' + idnum + '" class="nav nav-tabs" data-tabs="tabs">';
	html += '<li class="active"><a href="#jobstats-' + idnum + '" data-toggle="tab">Stats</a></li>';
	html += '<li><a href="#sourcessinks-' + idnum + '" data-toggle="tab">Taps</a></li>';
	html += '<li><a href="#hadoopcounters-' + idnum + '" data-toggle="tab">Counters</a></li>';
	html += '<li><a href="#links-' + idnum + '" data-toggle="tab">Links</a></li>';
	html += '</ul>';
	return html;
    }

    function prettyLinkedJobName(f) {
	return '<a href="/flowgraph/' + f.flow_id + '">' + f.truncated_name + '</a>';
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
	var local = step.map_tasks > 0 ? Math.round(100 * (step.data_local_map_tasks / step.map_tasks), 4) : "Unknown";
	var rack = step.map_tasks > 0 ? Math.round(100 * (step.rack_local_map_tasks / step.map_tasks), 4) : "Unknown";
	return '<table style="font-size:10px" class="table table-centered">' +
	    '<tr>' +
	    '<th>Data Local Mappers:</th><td>' + local + '%</td>' +
	    '<th>Rack Local Mappers:</th><td>' + rack + '%</td>' +
	    '</tr>' +
	    '<tr>' +
	    '<th>Map Tasks:</th><td class="mr-count-red">' + step.map_tasks + '</td>' +
	    '<th>Reduce Tasks:</th><td class="mr-count-blue">' + step.reduce_tasks + '</td>' +
	    '</tr>' +
	    '<tr>' +
	    '<th>Map Vcore-Seconds:</th><td class="mr-count-red">' + step.map_vcore_secs + '</td>' +
	    '<th>Reduce Vcore-Seconds:</th><td class="mr-count-blue">' + step.reduce_vcore_secs + '</td>' +
	    '</tr>' +
	    '</table>';
    }

    function renderMapReduceProgress(step) {
	var m = Math.round(step.map_progress / 2, 2);
	var r = Math.round(step.reduce_progress / 2, 2);
	return '<div class="progress">' +
	    '<div class="progress-bar progress-bar-danger progress-bar-striped active" ' +
	    'role="progressbar" aria-valuemin="0" aria-valuemax="50" aria-valuenow="' +
	    Math.floor(m) + '" style="width:' + Math.floor(m) + '%">' + step.map_progress + '% Map</div>' +
	    '<div class="progress-bar progress-bar-info progress-bar-striped active" ' +
	    'role="progressbar" aria-valuemin="0" aria-valuemax="50" aria-valuenow="' +
	    Math.floor(r) + '" style="width:' + Math.floor(r) + '%">' + step.reduce_progress + '% Reduce</div>' +
	    '</div>';
    }

    function makeYarnUrl(flow, step) {
	var yarn_id = step.job_id.slice(4, step.job_id.length);
	switch(step.step_status) {
	case "SUCCESSFUL":
	case "FAILED":
	case "SKIPPED":
	case "STOPPED":
	    var yarn_history = makeHistoryUrl(flow, '19888');
	    var link = yarn_history + '/jobhistory/job/job_' + yarn_id;
	    break;
	default:
	    var yarn_history = makeHistoryUrl(flow, '8088');
	    var link = yarn_history + '/proxy/application_' + yarn_id;
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
	    '<td>' + view.prettyPrintBytes(step.hdfs_bytes_read) + '</td>' +
	    '<td>' + view.prettyPrintBytes(step.hdfs_bytes_written) + '</td>' +
	    '</tr>' +
	    '<tr> <td>Tuples</td> ' +
	    '<td>' + step.tuples_read + '</td>' +
	    '<td>' + step.tuples_written + '</td>' +
	    '</tr>' +
	    '<tr> <td>Cluster Disk</td> ' +
	    '<td>' + view.prettyPrintBytes(step.file_bytes_read) + '</td>' +
	    '<td>' + view.prettyPrintBytes(step.file_bytes_written) + '</td>' +
	    '</tr>' +
	    '</table>' +
	    '</div>';
    }

    function renderTapsAndFields(style, title, data) {
	var html = '<div class="panel panel-' + style + '" style="font-size:10px">' +
            '<div class="panel-heading" style="text-align:center">' +
            title + '</div><div class="panel-body">';
	var addSeparator = Object.keys(data).length - 1;
	for (var tap in data) {
	    var fields = data[tap];
	    if (fields === undefined) { fields = "UNKNOWN"; }
	    html += '<div style="overflow-x:auto">';
	    html += '<strong style="margin-left:4px;margin-top:8px">' + tap + '</strong>';
	    html += '<div style="margin-left:4px">' + fields.join(', ') + '</div>';
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

    function renderDate(epochms) {
	return epochms !== 0 ?
	    moment(new Date(epochms)).utc().format('MM-DD-YYYY HH:mm:ss z') : "";
    }

    function renderLinks(step, flow) {
	var html = '<div style="font-size:10px">';
	var interpolationData = {
	    user: flow.user_name,
	    job_name: flow.truncated_name.replace(/\./g, '-'),
	    flow_id: flow.flow_id,
	    stage: step.step_number
	};
	var additionalLinks = step.config_props['sahale.additional.links'];
	var logLinks = [];
	if (step.job_id !== undefined && step.job_id !== null && step.job_id !== 'NO_JOB_ID') {
	    if (flow.yarn_job_history !== "false") {
		var jobLink = makeYarnUrl(flow, step);
		logLinks.push({name: 'View Hadoop Logs', url: jobLink});
		if (step.step_status == 'FAILED') {
		    // We know this is a History Server link at this point, so this replacement should be valid
		    var failedMapTasks = jobLink.replace("/job/", "/attempts/") + "/m/FAILED"
		    var failedReduceTasks = jobLink.replace("/job/", "/attempts/") + "/r/FAILED"
		    if (step.failed_map_tasks > 0) {
			logLinks.push({name: 'Failed Map Tasks', url: failedMapTasks});
			var globalFailLinkHtml = $('#fail_links').html();
			globalFailLinkHtml += '<div class=steplink><a href=//' + failedMapTasks + ' target=_blank>' + 'Failed Map Tasks for Step ' + step.step_number  + '</a></div>';
			$('#fail_links').html(globalFailLinkHtml);
		    }
		    if (step.failed_reduce_tasks > 0) {
			logLinks.push({name: 'Failed Reduce Tasks', url: failedReduceTasks});
			var globalFailLinkHtml = $('#fail_links').html();
			globalFailLinkHtml += '<div class=steplink><a href=//' + failedReduceTasks + ' target=_blank>' + 'Failed Reduce Tasks for Step ' + step.step_number  + '</a></div>';
			$('#fail_links').html(globalFailLinkHtml);
		    }
		}
		logLinks.push({name: 'ApplicationMaster', url: flow.jt_url + ':8088/cluster/app/' + step.job_id.replace('job_', 'application_')});
	    } else {
		logLinks.push({name: 'View Hadoop Logs', url: 'http://' + flow.jt_url + ':50030/jobdetails.jsp?jobid=' + step.job_id + '&refresh=0'});
	    }
	}
	for(i = 0; i < logLinks.length; ++i) {
	    var link = logLinks[i];
	    html += '<div class="steplink"><a href="//' + link.url + '" target=_blank><b>'+ link.name +'</b></a></div>';
	}

	if (additionalLinks !== undefined) {
	    var links = additionalLinks.split(';');
	    for (i = 0; i < links.length; ++i) {
		var link = links[i];
		var tokens = link.split('|');
		if (tokens.length == 2) {
		    var name = tokens[0].replace(/\+/g, ' ');
		    var url = Kiwi.compose(tokens[1], interpolationData);
		    html += '<div class=steplink><a href=//' + url + ' target=_blank>' + name + '</a></div>';
		}
	    }
	}

	html += '</div>';
	return html;
    }

    return view;

}(jQuery));
