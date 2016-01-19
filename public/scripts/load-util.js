var LoadUtil = (function($, d3, DataUtil) {
  var load = {
    links: [],
    nodes: [] 
  };

  load.populateDropDown = function() {
      var seen_already = {};
      var dropdown = $("#load-view-dropdown");
      var cnm = DataUtil.getConfigState()['cluster_name_mapping'];
      if (cnm) {
        for (key in cnm) {
          var cluster = cnm[key];
          if (seen_already.hasOwnProperty(cluster)) {
            continue;
          }
          seen_already[cluster] = true;
          dropdown.append("<li><a href='/load/" + cluster + "'> " + cluster + "</a></li>");
        }
      }
  };

  load.makeJobs = function(flows) {
    for (var i = 0; i < flows.length; ++i) {
      $.get('/steps/' + flows[i].flow_id, getStepsCallback(flows[i]));
    }
  }

  load.makeLinks = function() {
    var first_job_ndx = load.nodes.length;
    for (var i = 0; i < job_nodes.length; ++i) {
      var job = job_nodes[i];
      var job_ndx = first_job_ndx + i;
      for (var j = 0; j < first_job_ndx; ++j) {
        if (load.nodes[j].parent_id === job.flow_id) {
          load.links.push({
            source: j,
            target: job_ndx,
          });
        }
      }
    }
    Array.prototype.push.apply(load.nodes, job_nodes);
  };

  load.render = function() {
    var width = $("div.panel-heading").width();
        height = 600;

    var color = d3.scale.category20().domain(d3.range(0, 20, 1));
 
    var force = d3.layout.force()
        .charge(-75)
        .gravity(0.02)
        .linkDistance(80)
        .size([width, height]);

    var svg = d3.select("#clusterviz").append("svg:svg")
        .attr("width", width)
        .attr("height", height);

    force.links(load.links).nodes(load.nodes);

    svg.append("g").attr("class", "links");
    svg.append("g").attr("class", "nodes");

    var update = function() {
      var link = svg.select(".links").selectAll(".link").data(load.links,
          function (d) {
            return d.source.index + "-" + d.target.index;
          });

      link.enter().append("g").append("line").attr("class", "link");
      link.exit();

      var node = svg.select(".nodes").selectAll(".node")
        .data(load.nodes, function (d) { return d.index; });
      node.enter().append("g").attr("class", "node").call(force.drag);

      node.append("circle")
        .attr("r", function(d) { return d.radius; })
        .on("mousedown", function(d) { is_dragging = false; })
        .on("mousemove", function(d) { is_dragging = true; })
        .on("mouseup", function(d) {
          var was_dragging = is_dragging;
          is_dragging = false;
          if (!was_dragging && d.hasOwnProperty("flow_id")) {
            window.location.href = "/flowgraph/" + d.flow_id;
          }
        })
        .style("fill", function(d) {
          if (d.group === JOBNODE) {
            return "#CDCDCD";
          }
          return color(d.group);
        });
      node.append("text")
        .attr("text-anchor", "middle")
        .attr("class", "nodetext")
        .text(function(d) { return d.txt; });
      node.append("title").text(function(d) { return d.name; });
      node.exit();

      force.on("tick", function() {
        link.select("line")
            .attr("x1", function(d) { return d.source.x; })
            .attr("y1", function(d) { return d.source.y; })
            .attr("x2", function(d) { return d.target.x; })
            .attr("y2", function(d) { return d.target.y; });

        node.select("circle")
            .attr("cx", function(d) { return d.x; })
            .attr("cy", function(d) { return d.y; });

        node.select("text")
            .attr("x", function(d) { return d.x; })
            .attr("y", function(d) { return d.y; });
      });

    }
    force.start();
    update();
  };

  load.filterForCluster = function(target, test) {
    if (target == undefined || test == undefined ||
        target.toLowerCase() === 'all' || target.toLowerCase() === 'unknown') {
        return true; // keep all, no filtering
    }
    return target.toLowerCase() === test.toLowerCase();
  };


  //// PRIVATE UTIL FUNCTIONS ////
  var JOBNODE = 31337;
  var sizes = [ 50, 100, 500, 1000, 5000, 10000, 50000, 100000 ];
  var adjusted = [ 5, 10, 20, 30, 40, 50, 60, 70 ];
  var is_dragging = false;
  var job_nodes = [];

  function getStepsCallback(flow) {
    return function(steps_data) {
      var steps = DataUtil.unpackSteps(steps_data);
      makeNodes(flow, steps);
    };
  }

  function makeNodes(flow, steps) {
    var runningSteps = steps.filter(function(s) { return s.stepstatus == "RUNNING" });

    job_nodes.push({
      name: flow.flow_name + " (" + flow.user_name + ")",
      group: JOBNODE,
      radius: 20, // TODO: scale this by power use, disk I/O, etc!
      flow_id: flow.flow_id,
      txt: flow.truncated_name,
    });

    if (runningSteps.length === 0) {
      return;
    }

    for (var i = 0; i < runningSteps.length; ++i) {
      var step = runningSteps[i];

      load.nodes.push({
        name: "step: " + step.stepnumber + " | " + step.maptasks + " mappers",
        group: mapStepColor(step.stepnumber),
        radius: setNodeSize(step.maptasks),
        parent_id: flow.flow_id,
        txt: step.maptasks + " mappers",
      });

      if (step.reducetasks > 0) {
        load.nodes.push({
          name: "step: " + step.stepnumber + " | " + step.reducetasks + " reducers",
          group: reduceStepColor(step.stepnumber),
          radius: setNodeSize(step.reducetasks),
          parent_id: flow.flow_id,
          txt: step.reducetasks + " reducers"
        });
      }
    }
  };

  function mapStepColor(n) {
    return ((n - 1) * 2) % 20;
  }

  function reduceStepColor(n) {
    return (((n - 1) * 2) + 1) % 20;
  }

  function setNodeSize(tasks) {
    var i = 0;
    while (i < sizes.length && tasks > sizes[i]) { ++i; }
    return adjusted[i];
  };

  return load;

}(jQuery, d3, DataUtil));
