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
	out_flow.username_link = getUsernameLink(out_flow);
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

    data.doRegexReplacement = function (jt_url, regexes, default_value) {
        // A utility function for rewriting cluster host names according to the
        // config options cluster_name_regexes and cluster_link_regexes.
        // The `regexes` arg is a mapping from regex => replacement string;
        // if you put capture groups in the regex, then you can reference them
        // in the capture group using javascript standard notation, e.g.
        // the regex-to-replacement mapping
        //    "cluster-([0-9]+)-master": "cluster-number-$1"
        // will remap the cluster "cluster-123-master" to "cluster-number-123"
        var name = default_value;

        if(!regexes) {
            return name;
        }

        Object.keys(regexes).forEach(function(regex) {
            var pattern = new RegExp(regex);
            var matches = pattern.exec(jt_url);
            if (matches) {
                // We should probably really return upon first hit, but we will
                // leave this as it originally was to avoid altering existing
                // behavior
                name = insertCaptureGroups(regexes[regex], matches);
            }
        });

        return name;
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

    function insertCaptureGroups(str, matches) {
        // This method lets us support regexp capture groups in the regex
        // mapping string.  For example, we might define a regex mapping
        //   'gcp-(.*)-[0-9]+': '$1'
        // which would map the jobtracker name 'gcp-my-cluster-12345' to 'my-cluster'
        for(group = 0; group < matches.length; group++) {
            target = (group == 0) ? '$&' : ('$' + group);

            str = str.replace(target, matches[group]);
        }

        return str;
    }

    function getClusterNameMapping(out_flow) {
	var cnm = server_config['cluster_name_mapping'] || {};
	var name = cnm[out_flow.jt_url] ||
        data.doRegexReplacement(out_flow.jt_url, server_config['cluster_name_regexes'], 'Unknown');

    return name;
    }

    function getUsernameLink(out_flow) {
	var usernameLinkFormat = server_config['username_link_format'];
	var username = out_flow.user_name;
	var link = undefined;
	if (usernameLinkFormat !== undefined) {
	    link = usernameLinkFormat.replace(/\${username}/g, username);
	}
	return link;
    }

    return data;
}());
