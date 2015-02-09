var DataUtil = (function() {
  var data = {};

  data.unpackFlow = function(flow) {
    // unpack JSON fields
    var outFlow = JSON.parse(flow['flow_json']);

    // no need to unpack fields used in queries, they get their own columns
    outFlow.flow_id = flow.flow_id;
    outFlow.flow_name = flow.flow_name;
    outFlow.flow_status = flow.flow_status;
    outFlow.create_date = flow.create_date;
    outFlow.update_date = flow.update_date;

    return outFlow;
  }

  data.unpackFlows = function(flowz) {
    var out = [];
    for (index in flowz) {
      out.push(data.unpackFlow(flowz[index]));
    }
    return out;
  }

  return data;
}());
