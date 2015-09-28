////////////////////////////// For running time stacked bar ///////////////////
var StackedBarUtil = (function($, StateUtil) {
    var sbutil = {};
    var toggle = {};

    toggle.title = [
	'Running Time Per Step',
	'Job Links'
    ];
    toggle.html = [];

    sbutil.render = function(step_map, flow) {
	toggle.html = [
	    renderRunningTimes(step_map, flow),
	    renderJobLinks(flow)
	];
	renderAndRegisterEvent();
    }

    function renderAndRegisterEvent() {
	var ndx = parseInt(StateUtil.getRightToggleState());
	var title = $('#right_toggle_title')
	title.text(toggle.title[ndx]);
	title.append('<button class="glyphicon glyphicon-arrow-right" style="float:right" id="right_toggle_right"></button>');
	title.append('<button class="glyphicon glyphicon-arrow-left" style="float:right" id="right_toggle_left"></button>');
	console.log(StateUtil.getRightToggleState());
	console.log(JSON.stringify(toggle.html[ndx]));
	console.log(JSON.stringify(toggle.title[ndx]));
	$("#timechart").html(toggle.html[ndx]);

	$("#right_toggle_right").on("click", function(evt) {
	    StateUtil.incrementRightToggleState(toggle.html.length);
	    renderAndRegisterEvent();
	});

	$("#right_toggle_left").on("click", function(evt) {
	    StateUtil.decrementRightToggleState(toggle.html.length);
	    renderAndRegisterEvent();
	});
    }

    function renderJobLinks(flow) {
	var interpolationData = {
	    user: flow.user_name,
	    job_name: flow.flow_name.replace('com.etsy.scalding.jobs.', '').replace(/\./g, '-'),
	    flow_id: flow.flow_id
	};

	var flowLinks = flow.flow_links;
	
	var html = '<div class="logbox">';
	html += '<div style="font-size:10px">';

	if (flowLinks !== undefined) {
	    var links = flowLinks.split(';');
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
	html += "</div>";

	return html;
    }
    
  function renderRunningTimes(step_map, flow) {
    var width_map = generateWidthMap(step_map);

    var html = '<div class="inner-stack-box">';
    html += buildStackChart(width_map, step_map);
    html += "</div>";

      return html;
  };

  function buildStackChart(width_map, step_map) {
    var html = "";

    for (key in step_map) {
      var step = step_map[key];
      var barWidth = width_map[key + "-scaled"];
      var tip = formatTipMessage(step);
      var color = setColorCycle(step);
      html += '<span id="stack-step-' + step.stepnumber + '" class="time-block ' +
        color + '" style="width:' + barWidth + '%;"></span>';
      html += '<script>';
      html += '  $("#stack-step-' + step.stepnumber + '")';
      html += '    .tipsy({ title: function() { return "' + tip + '" } })';
      html += '    .on("mouseover", function(e) { $("#step-image-' +
        step.stepnumber + '").trigger("mouseover"); return true; });';
      html += '</script>';
    }

    return html;
  }

  function setColorCycle(step) {
    var variant = (step.stepnumber % 3) + 1;
    var color = "grey";

    switch(step.stepstatus) {
      case "RUNNING":
        color = "orange";
        break;
      case "FAILED": case "KILLED":
        color = "red";
        break;
      case "SUCCESSFUL":
        color = "green";
        break;
      default:
        break;
    }

    return "color-cycle-" + color + "-" + variant;
  }

  function formatTipMessage(step) {
    if (["PENDING", "SUBMITTED", "STARTED", "NOT_LAUNCHED"].indexOf(step.stepstatus) !== -1) {
      return 'Step ' + step.stepnumber + ': Unknown';
    }

    return 'Step ' + step.stepnumber + ': ' + step.steprunningtime + ' secs';
  }

  function generateWidthMap(step_map) {
    var width_map = {};
    var total = 0;
    for (key in step_map) {
      var step = step_map[key];
      width_map[key] = getBarWidth(step);
      total += parseFloat(width_map[key]);
    }

    width_map['total'] = Math.ceil(total);
    for (key in step_map) {
      if (width_map['total'] === 0) break;
      var result = Math.floor((width_map[key] / width_map['total']) * 100.0);
      if (result === 0) {
         result = 1; // just so we can see all the steps in the diagram
      }
      width_map[key + "-scaled"] = result;
    }

    return width_map;
  }

  function getBarWidth(step) {
    if (step.stepstatus === "RUNNING") {
      return 2;
    } else {
      return step.steprunningtime;
    }
  }

  return sbutil;
}(jQuery, StateUtil));
