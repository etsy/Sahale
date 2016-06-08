var DataUtil = (function() {
    var data = {};

    var SAHALE_SERVER_CONFIG = "sahale_server_config";

    data.unpackFlow = function(flow) {
	// unpack JSON fields
	var out_flow = JSON.parse(flow['flow_json']);

	// no need to unpack fields used in queries, they get their own columns
	out_flow.flow_id = flow.flow_id;
	out_flow.flow_name = flow.flow_name;
	out_flow.create_date = flow.create_date;
	out_flow.update_date = flow.update_date;
	out_flow.cluster_name = getClusterNameMapping(out_flow);
	out_flow.truncated_name = getTruncatedName(out_flow);

	var sja = out_flow['config_props']['scalding.job.args']
	if (sja) {
	    out_flow['config_props']['scalding.job.args'] = sja.split(/\s+/);
	}

	return out_flow;
    }

    data.getConfigState = function() {
	var sc = sessionStorage.getItem(SAHALE_SERVER_CONFIG);
	if (sc) {
	    sc = JSON.parse(sc);
	} else {
	    $.ajax({
		async: false,
		type: 'GET',
		url: '/sahale_config_data',
		success: function(data) {
		    sc = data;
		    sessionStorage.setItem(SAHALE_SERVER_CONFIG, JSON.stringify(sc));
		}
	    });
	}
	return sc;
    }

    var server_config = data.getConfigState();

    data.unpackFlows = function(flowz) {
	var out = [];
	for (index in flowz) {
	    out.push(data.unpackFlow(flowz[index]));
	}
	return out;
    }

    // build a map of step number to hydrated step object
    data.buildStepNumberToStepMap = function(rows) {
	var map = {};
	rows.forEach(function(item, ndx, arr) {
	    var step = data.unpackStep(item);
	    map[step.step_number] = step;
	});
	return map;
    }

    // unpack a collection of steps
    data.unpackSteps = function(steps) {
	var out = [];
	steps.forEach(function(item, ndx, arr) {
	    out.push(data.unpackStep(item));
	});
	return out;
    }

    data.unpackStep = function(item) {
	var step = JSON.parse(item.step_json);

	// top-level table fields we might want at some point
	step.create_date          = item.create_date;
	step.update_date          = item.update_date;
	step.flow_id              = item.flow_id;
	//step.step_id              = item.step_id; // provided in JSON

	// set "shortcut" fields from the counters map
	step.map_tasks            = checkedStepUnpack(step, 'mapreduce.JobCounter', 'TOTAL_LAUNCHED_MAPS', 0);
	step.reduce_tasks         = checkedStepUnpack(step, 'mapreduce.JobCounter', 'TOTAL_LAUNCHED_REDUCES', 0);
	step.failed_map_tasks     = checkedStepUnpack(step, 'mapreduce.JobCounter', 'NUM_FAILED_MAPS', 0);
	step.failed_reduce_tasks  = checkedStepUnpack(step, 'mapreduce.JobCounter', 'NUM_FAILED_REDUCES', 0);
	step.data_local_map_tasks = checkedStepUnpack(step, 'mapreduce.JobCounter', 'DATA_LOCAL_MAPS', 0);
	step.rack_local_map_tasks = checkedStepUnpack(step, 'mapreduce.JobCounter', 'RACK_LOCAL_MAPS', 0);
	step.hdfs_bytes_read      = checkedStepUnpack(step, 'mapreduce.FileSystemCounter', 'HDFS_BYTES_READ', 0);
	step.hdfs_bytes_written   = checkedStepUnpack(step, 'mapreduce.FileSystemCounter', 'HDFS_BYTES_WRITTEN', 0);
	step.file_bytes_read      = checkedStepUnpack(step, 'mapreduce.FileSystemCounter', 'FILE_BYTES_READ', 0);
	step.file_bytes_written   = checkedStepUnpack(step, 'mapreduce.FileSystemCounter', 'FILE_BYTES_WRITTEN', 0);
	step.tuples_read          = checkedStepUnpack(step, 'cascading.flow.StepCounters', 'Tuples_Read', 0);
	step.tuples_written       = checkedStepUnpack(step, 'cascading.flow.StepCounters', 'Tuples_Written', 0);
	step.io_read_millis       = checkedStepUnpack(step, 'cascading.flow.SliceCounters', 'Read_Duration', 0);
	step.io_write_millis      = checkedStepUnpack(step, 'cascading.flow.SliceCounters', 'Write_Duration', 0);
	step.map_vcore_millis     = checkedStepUnpack(step, 'mapreduce.JobCounter', 'VCORES_MILLIS_MAPS', 0);
	step.reduce_vcore_millis  = checkedStepUnpack(step, 'mapreduce.JobCounter', 'VCORES_MILLIS_REDUCES', 0);
	step.cpu_millis           = checkedStepUnpack(step, 'mapreduce.TaskCounter', 'CPU_MILLISECONDS', 0);
	step.gc_millis            = checkedStepUnpack(step, 'mapreduce.TaskCounter', 'GC_TIME_MILLIS', 0);
	step.map_vcore_secs       = Math.round(step.map_vcore_millis / 1000);
	step.reduce_vcore_secs    = Math.round(step.reduce_vcore_millis / 1000)
	step.config_props         = extractConfigurationProperties(step);

	return step;
    }

    function checkedStepUnpack(step, group, counter, defaultValue) {
	if (!step || !group || !counter) {
	    console.log("Invalid value found in step['counters'][group][counter]");
	    console.log("StepStatus object: ");
	    console.debug(step);
	    console.log("Group:" + group + ", counter: " + counter);
	    return defaultValue;
	}
	if (step['counters'] === undefined) {
	    console.log("step['counters'] is undefined");
	    return defaultValue;
	}
	if (step['counters'][group] === undefined) {
	    //console.log("step['counters'][group] is undefined where group is " + group);
	    return defaultValue;
	}
	return step['counters'][group][counter] || defaultValue;
    }

    function extractConfigurationProperties(step) {
	if (step['config_props'] === undefined) {
	    return {};
	}
	return step['config_props'];
    }

    function getTruncatedName(out_flow) {
	var jnp = server_config['job_name_prefix'] || '';
	if (jnp !== '') {
	    return out_flow.flow_name.replace(jnp, '');
	}
	return out_flow.flow_name;
    }

    function getClusterNameMapping(out_flow) {
	var cnm = server_config['cluster_name_mapping'];
	var name = cnm[out_flow.jt_url] || 'Unknown';
	if (name === 'Unknown') {
	    var regexes = server_config['cluster_name_regexes'];
	    Object.keys(regexes).forEach(function (regex) {
	        var pattern = new RegExp(regex);
		if (pattern.test(out_flow.jt_url)) {
		    name = regexes[regex];
		}
	    });
	}

	return name;
    }

    return data;
}());
