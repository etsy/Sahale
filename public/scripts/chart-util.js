/* *****************************************************
 * This is for D3 Bar Charts on history view page
 */
var ChartUtil = (function($, d3, ViewUtil) {

  var chartutil = {},
      // quick hack to control the label text length for bar labels
      months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
  ;

  //////////////// D3 chart rendering ///////////////////////////////////
  chartutil.renderBarChart = function(element_id, data, y_axis_label, barcolor) {
    var margin = { top: 10, right: 10, bottom: 0, left: 55 };
    var width = Math.max(300, (80 * data.length)),
      height = 120,
      innerWidth = width - margin.left - margin.right,
      inner_height = height - margin.top - margin.bottom;
    var data_domain = [];
    data.map(function(elem, i, data) { data_domain.push(elem.name); });

    var x = d3.scale.ordinal()
        .domain(data_domain)
        .rangeRoundBands([0, innerWidth], .1);

    var y = d3.scale.linear()
        .range([inner_height, 0]);

    x.domain(data.map(function(d) { return d.name; }));
    y.domain([0, d3.max(data, function(d) { return d.value; })]);

    var chart = d3.select(element_id + " .rrchart")
        .attr("width", width)
        .attr("height", height + 20)
        .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var xAxis = d3.svg.axis()
      .scale(x)
      .orient("bottom");

    var yAxis = d3.svg.axis()
      .scale(y)
      .orient("right");

    chart.append("g")
      .attr("class", "x rraxis")
      .attr("transform", "translate(0," + inner_height + ")")
      .attr("height", height)
      .call(xAxis);

    chart.append("g")
      .attr("class", "y rraxis")
      .attr("transform", "translate(-45,0)")
      .call(yAxis)
      .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", -10)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text(y_axis_label);

    chart.selectAll(".rrbar")
      .data(data)
      .enter().append("rect")
        .attr("class", "rrbar")
        .attr("x", function(d) { return x(d.name); })
        .attr("width", x.rangeBand())
        .attr("y", function(d) { return y(d.value); })
        .attr("height", function(d) { return inner_height - y(d.value); })
        .style("fill", function(d) { return d.barcolor; })
        .on("click", function(d) {
            window.location.href = "/flowgraph/" + d.flowid;
          }
        );

    $(".rrbar")
      .tipsy({
        gravity: 's',
        html: true,
        title: function() { return this.__data__.tip + "<p style=\"color:Yellow;font-size:8px;\">Click Bar For Details</p>"; }
      });

      $(element_id + ' .waitforrender').remove();
  }

  chartutil.getRunningTimeData = function(flows) {
    var list = [];
    for (key in flows) {
      var flow = flows[key];
      var color = getFlowStatusColorForTooltip(flow['flow_status']);
      var create_date = flow['create_date'] * 1000;
      var val = Math.round((flow['flow_duration'] / 1000) / 60);
      if (val < 1) { val = 1; }
      list.push({
        name: formatEpochMs(create_date),
        value: val,
        tip: '<h5 style="text-align:center;color:' + color + '">' +
          flow['flow_status'] + '</h5>Running Time: ' + ViewUtil.prettyFlowTimeFromMillis(flow['flow_duration']),
        barcolor: ViewUtil.getFlowStatusColor(flow['flow_status']),
        flowid: flow['flow_id']
      });
    }
    return list;
  }

  chartutil.getMapReduceSlotData = function(flows) {
    var list = [];
    for (key in flows) {
      var flow = flows[key];
      var m = flow.aggregated.flow_total_map_tasks;
      var r = flow.aggregated.flow_total_reduce_tasks;
      var slots = m + r;
      if (slots < 1) { slots = 1; }
      list.push({
        name: formatEpochMs(flow['create_date'] * 1000),
        value: slots,
        tip: '<div>Map Count: <b style="color:Pink">' + m + '</b></div>' +
          '<div>Reduce Count: <b style="color:LightBlue">' + r + '</b></div>',
        barcolor: "Indigo",
        flowid: flow['flow_id']
      })
    }
    return list;
  }

  chartutil.getHdfsWriteData = function(flows) {
    var list = [];
    for (key in flows) {
      var flow = flows[key];
      var raw = flow.aggregated.flow_hdfs_bytes_written;
      var val = Math.round(raw / (1024 * 1024 * 1024), 3);
      if (val < 1) { val = 1; }
      list.push({
        name: formatEpochMs(flow['create_date'] * 1000),
        value: val,
        tip: "Total HDFS Writes: " + ViewUtil.prettyPrintBytes(raw),
        barcolor: this.getColorForNumBytes(raw),
        flowid: flow['flow_id']
      })
    }
    return list;
  }

  chartutil.getGsWriteData = function(flows) {
      var list = [];
      for (key in flows) {
          var flow = flows[key];
          var raw = flow.aggregated.flow_gs_bytes_written;
          var val = Math.round(raw / (1024 * 1024 * 1024), 3);
          if (val < 1) { val = 1; }
          list.push({
              name: formatEpochMs(flow['create_date'] * 1000),
              value: val,
              tip: "Total GS Writes: " + ViewUtil.prettyPrintBytes(raw),
              barcolor: this.getColorForNumBytes(raw),
              flowid: flow['flow_id']
          })
      }
      return list;
  }

  chartutil.getDiskWriteData = function(flows) {
    var list = [];
    for (key in flows) {
      var flow = flows[key];
      var raw = flow.aggregated.flow_file_bytes_written;
      var val = Math.round(raw / (1024 * 1024 * 1024), 3);
      if (val < 1) { val = 1; }
      list.push({
        name: formatEpochMs(flow['create_date'] * 1000),
        value: val,
        tip: "Cluster Disk Writes: " + ViewUtil.prettyPrintBytes(raw),
        barcolor: this.getColorForNumBytes(raw),
        flowid: flow['flow_id']
      })
    }
    return list;
  }

  chartutil.getHdfsReadData = function(flows) {
    var list = [];
    for (key in flows) {
      var flow = flows[key];
      var raw = flow.aggregated.flow_hdfs_bytes_read;
      var val = Math.round(raw / (1024 * 1024 * 1024), 3);
      if (val < 1) { val = 1; }
      list.push({
        name: formatEpochMs(flow['create_date'] * 1000),
        value: val,
        tip: "Total HDFS Reads: " + ViewUtil.prettyPrintBytes(raw),
        barcolor: this.getColorForNumBytes(raw),
        flowid: flow['flow_id']
      })
    }
    return list;
  }

  chartutil.getGsReadData = function(flows) {
      var list = [];
      for (key in flows) {
          var flow = flows[key];
          var raw = flow.aggregated.flow_gs_bytes_read;
          var val = Math.round(raw / (1024 * 1024 * 1024), 3);
          if (val < 1) { val = 1; }
          list.push({
              name: formatEpochMs(flow['create_date'] * 1000),
              value: val,
              tip: "Total HDFS Reads: " + ViewUtil.prettyPrintBytes(raw),
              barcolor: this.getColorForNumBytes(raw),
              flowid: flow['flow_id']
          })
      }
      return list;
  }

  chartutil.getDiskReadData = function(flows) {
    var list = [];
    for (key in flows) {
      var flow = flows[key];
      var raw = flow.aggregated.flow_file_bytes_read;
      var val = Math.round(raw / (1024 * 1024 * 1024), 3);
      if (val < 1) { val = 1; }
      list.push({
        name: formatEpochMs(flow['create_date'] * 1000),
        value: val,
        tip: "Cluster Disk Reads: " + ViewUtil.prettyPrintBytes(raw),
        barcolor: this.getColorForNumBytes(raw),
        flowid: flow['flow_id']
      })
    }
    return list;
  }

  chartutil.getCpuSecs = function(flows) {
    var list = [];
    for (key in flows) {
      var flow = flows[key];
      var raw = flow.aggregated.flow_cpu_millis;
      var val = Math.round(raw / 1000, 3);
      if (val < 1) { val = 1; }
      list.push({
        name: formatEpochMs(flow['create_date'] * 1000),
        value: val,
        tip: 'Flow CPU time<br>all tasks/cores:<br>' +
          ViewUtil.prettyFlowTimeFromMillis(raw) + '<br>' +
          '(' + val + ' secs)',
        barcolor: ViewUtil.getFlowStatusColor(flow['flow_status']),
        flowid: flow['flow_id']
      })
    }
    return list;
  }

  chartutil.getVcoreSecs = function(flows) {
    var list = [];
    for (key in flows) {
      var flow = flows[key];
      var raw = flow.aggregated.flow_map_vcore_millis +
        flow.aggregated.flow_reduce_vcore_millis;
      var val = Math.round(raw / 1000, 3);
      if (val < 1) { val = 1; }
      list.push({
        name: formatEpochMs(flow['create_date'] * 1000),
        value: val,
        tip: 'Flow VCore time<br>all tasks/cores:<br>' +
          ViewUtil.prettyFlowTimeFromMillis(raw) + '<br>' +
          '(' + val + ' secs)',
        barcolor: ViewUtil.getFlowStatusColor(flow['flow_status']),
        flowid: flow['flow_id']
      })
    }
    return list;
  }

  chartutil.getColorForNumBytes = function(bytes) {
    return bytes >= 10995116277760 ? "red" : bytes >= 536870912000 ? "orange" : "green";
  }


  //////////////// private utility functions for bar charts ///////////////////////
  function getFlowStatusColorForTooltip(flow_status) {
    var color = ViewUtil.getFlowStatusColor(flow_status);
    return color === 'black' ? 'white' : color;
  }

  function formatEpochMs(epoch_ms) {
    var date = new Date(epoch_ms);
    return months[date.getUTCMonth()] + " " +
      date.getUTCDate() + ", " +
      date.getUTCFullYear() + " " +
      pad(date.getUTCHours()) + ":" +
      pad(date.getUTCMinutes()) + ":" +
      pad(date.getUTCSeconds())
  }

  function pad(num) {
    return num < 10 ? "0" + num : num;
  }

  return chartutil;
}(jQuery, d3, ViewUtil));
