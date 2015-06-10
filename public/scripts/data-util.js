var DataUtil = (function() {
    var data = {};

    var cluster_name_mapping = null;

    $.ajax({
	async: false,
	type: 'GET',
	url: '/cluster_name_mapping',
	success: function(data) {
	    cluster_name_mapping = data;
	}
    });
    
  data.unpackFlow = function(flow) {
    // unpack JSON fields
    var outFlow = JSON.parse(flow['flow_json']);

    // no need to unpack fields used in queries, they get their own columns
    outFlow.flow_id = flow.flow_id;
    outFlow.flow_name = flow.flow_name;
    outFlow.flow_status = flow.flow_status;
    outFlow.create_date = flow.create_date;
    outFlow.update_date = flow.update_date;
    outFlow.cluster_name = cluster_name_mapping[outFlow.jt_url];


    return outFlow;
  }

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
      map[step.stepnumber] = step;
    });
    return map;
  }

  // unpack a collection of steps
  data.unpackSteps = function(steps) {
    var out = [];
    steps.forEach(function(item, ndx, arr) {
      var step = data.unpackStep(item);
      out.push(step);
    });
    return out;
  }

  data.unpackStep = function(item) {
    var step = JSON.parse(item.step_json);
    step.flow_id = item['flow_id'];
    step.step_id = item['step_id'];

    // if legacy data, we're done here.
    if (step['hdfsbytesread'] !== undefined) return step;

    // if not, we need to set "shortcut" fields from the counters map
    step.maptasks = checkedStepUnpack(step, 'mapreduce.JobCounter', 'TOTAL_LAUNCHED_MAPS', 0);
    step.reducetasks = checkedStepUnpack(step, 'mapreduce.JobCounter', 'TOTAL_LAUNCHED_REDUCES', 0);
    step.failedmaptasks = checkedStepUnpack(step, 'mapreduce.JobCounter', 'NUM_FAILED_MAPS', 0);
    step.failedreducetasks = checkedStepUnpack(step, 'mapreduce.JobCounter', 'NUM_FAILED_REDUCES', 0);
    step.datalocalmaptasks = checkedStepUnpack(step, 'mapreduce.JobCounter', 'DATA_LOCAL_MAPS', 0);
    step.racklocalmaptasks = checkedStepUnpack(step, 'mapreduce.JobCounter', 'RACK_LOCAL_MAPS', 0);
    step.hdfsbytesread = checkedStepUnpack(step, 'mapreduce.FileSystemCounter', 'HDFS_BYTES_READ', 0);
    step.hdfsbyteswritten = checkedStepUnpack(step, 'mapreduce.FileSystemCounter', 'HDFS_BYTES_WRITTEN', 0);
    step.filebytesread = checkedStepUnpack(step, 'mapreduce.FileSystemCounter', 'FILE_BYTES_READ', 0);
    step.filebyteswritten = checkedStepUnpack(step, 'mapreduce.FileSystemCounter', 'FILE_BYTES_WRITTEN', 0);
    step.tuplesread = checkedStepUnpack(step, 'cascading.flow.StepCounters', 'Tuples_Read', 0);
    step.tupleswritten = checkedStepUnpack(step, 'cascading.flow.StepCounters', 'Tuples_Written', 0);
    step.ioreadmillis = checkedStepUnpack(step, 'cascading.flow.SliceCounters', 'Read_Duration', 0);
    step.iowritemillis = checkedStepUnpack(step, 'cascading.flow.SliceCounters', 'Write_Duration', 0);
    step.configuration_properties = extractConfigurationProperties(step);

    return step;

    // TODO: these are not exposed in Sahale yet, add them
    //step.committedheapbytes = checkedStepUnpack(step, 'mapreduce.TaskCounter', 'COMMITTED_HEAP_BYTES', 0);
    //step.gcmillis = checkedStepUnpack(step, 'mapreduce.TaskCounter', 'GC_TIME_MILLIS', 0);
    //step.cpumillis = checkedStepUnpack(step, 'mapreduce.TaskCounter', 'CPU_MILLISECONDS', 0);
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
	if (step['configuration_properties'] === undefined) {
	    return {};
	}

	return step['configuration_properties'];
    }

  return data;
}());
