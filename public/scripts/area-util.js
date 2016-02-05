var AreaChart = (function($, d3) {
  /*
   * Utilities to build the Flow Details Area Chart
   */
  var area = {},
    height = 115,
    width = 0,
    margin = 22,
    xFn = null,
    yFn = null,
    mapData = [],
    reduceData = [],
    tipData = [],
    svg = null,
    maxValue = 0,
    dataPoints = 0,
    spacing = 25
  ;

  area.renderAreaChart = function(md, rd, td, mv) {
    mapData = md;
    reduceData = rd;
    tipData = td;
    maxValue = mv;
    dataPoints = mapData.length;
    width =  spacing * (dataPoints + 2);

    xFn = d3.scale.ordinal()
      .domain(getXRange(dataPoints))
      .range(getXSpacing(dataPoints, margin))

    yFn = d3.scale.linear()
      .domain([0, maxValue])
      .range([height - margin, margin]);

    if (dataPoints < 2) {
      $("#areachart")
        .addClass("alert alert-info")
        .attr("role", "alert")
        .html('<div style="text-align:center">Chart Not Available</div>')
      ;
      return;
    }

    $("#areachart").find("svg").remove();
    svg = d3.select("#areachart")
      .append("svg")
      .attr("height", height)
      .attr("width", width)
      .attr("overflow-x", "auto")
    ;

    renderAxes();

    render(mapData, "mapLine", "mapArea", "mapDot");
    render(reduceData, "reduceLine", "reduceArea", "reduceDot");
  }


  function render(theData, lineClass, areaClass, dotClass) {
    var line = d3.svg.line()
      .x(function(d) { return xFn(d.x); })
      .y(function(d) { return yFn(d.y); })
    ;

    var areaFn = d3.svg.area()
      .x(function(d) { return xFn(d.x); })
      .y0(yFn(0))
      .y1(function(d) { return yFn(d.y); });

    svg.selectAll("path." + areaClass)
      .data([theData])
      .enter()
        .append("path")
        .attr("class", areaClass)
        .attr("d", function(d) { return areaFn(d); });

    svg.selectAll("path." + lineClass)
      .data([theData])
      .enter()
        .append("path")
        .attr("class", lineClass)
        .attr("d", function(d) { return line(d); });

    renderDots(theData, dotClass);
    renderDotTips(dotClass);
  }

  function renderDots(theData, dotClass) {
    svg.append("g").selectAll("circle." + dotClass)
      .data(theData)
      .enter().append("circle")
        .attr("class", dotClass)
        .attr("cx", function(d) { return xFn(d.x); })
        .attr("cy", function(d) { return yFn(d.y); })
        .attr("r", 3)
    ;
    renderDotTips(dotClass);
  }

  function renderDotTips(dotClass) {
    $("circle." + dotClass).each(function(index) {
      $(this)
        .tipsy({ html: true, title: function(){return tipData[index];} })
        .on('mouseover', function(e) { $("#step-image-" + (index + 1)).trigger('mouseover'); return true; })
      ;
    });
  }

  function renderAxes() {
    var xAxis = d3.svg.axis()
      .scale(d3.scale.ordinal()
        .domain(getXRange(dataPoints))
        .range(getXSpacing(dataPoints, 0))
      )
      .orient("bottom");

    var yAxis = d3.svg.axis()
      .scale(d3.scale.linear().domain([0, maxValue]).range([quadrantHeight(), 0]))
      .orient("left");

    svg.append("g")
      .attr("class", "axis")
      .attr("transform", function(){
        return "translate(" + (margin) + "," + yStart() + ")";
      })
      .call(xAxis);

    svg.append("g")
      .attr("class", "axis")
      .attr("transform", function(){
        return "translate(" + (margin * 2) + "," + yEnd() + ")";
      })
      .call(yAxis);
  }

  function yStart(){
      return height - margin;
  }

  function yEnd(){
      return margin;
  }

  function quadrantHeight(){
      return height - 2 * margin;
  }

  // returns array of range [0..N] where N === count === # of job stages in Flow
  function getXRange(count) {
    var temp = [];
    for (var i = 1; i <= count; ++i) { temp.push(i); }
    return temp;
  }

  // return X range adjusted by scaling factor (spacing between entries and constant term (left-side padding)
  function getXSpacing(count, padding) {
    return getXRange(count).map(
      function(d) { return (d * spacing) + padding; }
    );
  }

  return area;
}(jQuery, d3));
