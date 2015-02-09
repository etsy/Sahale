////////////////////////////// For running time stacked bar ///////////////////
var StackedBarUtil = (function($) {
  var sbutil = {};

  sbutil.renderRunningTimes = function(stepMap, flow) {
    var widthMap = generateWidthMap(stepMap);

    var html = '<div class="inner-stack-box">';
    html += buildStackChart(widthMap, stepMap);
    html += "</div>";

    $("#timechart").html(html);
  };

  function buildStackChart(widthMap, stepMap) {
    var html = "";

    for (key in stepMap) {
      var step = stepMap[key];
      var barWidth = widthMap[key + "-scaled"];
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
    } else if (step.stepstatus === "RUNNING") {
      return 'Step ' + step.stepnumber + ': In Progress';
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
}(jQuery));
