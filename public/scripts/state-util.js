var StateUtil = (function() {
  var state = {};

  var SAHALE_INIT_STATE = {
    tx: 40,
    ty: 40,
    scale: 1,
    curid: 0,
    curcolor: "NONE",
    tabs_state: {},
    chart_state: 0
  };
  var sahale_state = SAHALE_INIT_STATE;

  state.captureGraphViewYOffset = function(offset) {
    SAHALE_INIT_STATE.ty = offset;
    console.log("[Set SAHALE_INIT_STATE]:"); // DEBUG
    console.debug(SAHALE_INIT_STATE); // DEBUG
  }

  state.clearFlowState = function(flow_id) {
    sessionStorage.removeItem('flowid-' + flow_id);
    console.log("[clearFlowState] for Flow ID: " + flow_id); // DEBUG
  }

  state.getFlowState = function(flow_id) {
    var newStateStr = sessionStorage.getItem("flowid-" + flow_id);
    console.log("[getFlowState] Hydrate newStateStr:"); // DEBUG
    console.debug(newStateStr); // DEBUG
    sahale_state = newStateStr === null ? SAHALE_INIT_STATE : JSON.parse(newStateStr);
    console.log("[getFlowState] sahale_state returned:"); // DEBUG
    console.debug(sahale_state); // DEBUG
    return sahale_state;
  }

  state.setFlowState = function(flow_id) {
    sessionStorage.setItem("flowid-" + flow_id, JSON.stringify(sahale_state));
    console.log("[setFlowState] dehydrated sahale_state for browser storage: " + JSON.stringify(sahale_state)); // DEBUG
  }

  state.updateTranslateY = function(val) {
    sahale_state.ty = val;
  }

  state.updateTranslateX = function(val) {
    sahale_state.tx = val;
  }

  state.updateCurrentId = function(val) {
    sahale_state.curid = val;
  }

  state.updateCurrentColor = function(val) {
    sahale_state.curcolor = val;
  }

  state.updateTranslateScale = function(val) {
    sahale_state.scale = val;
  }

  state.setChartState = function(index) {
    sahale_state.tabs_state = index;
  }

  // step is 1-indexed, tab is 0-indexed
  state.setTabState = function(stepnum, tabnum) {
    sahale_state.chart_state[stepnum] = tabnum;
  }

  // called from graph-util just before setFlowState is called on page refresh
  state.updateViewState = function(transx, transy, sc) {
    state.updateTranslateX(transx);
    state.updateTranslateY(transy);
    state.updateTranslateScale(sc);
    // 'curid' stays the same
    // 'curcolor' should be set by getter code as it can change during a page refresh
    state.updateCurrentColor("ERROR_BAD_PARAM");
    console.log("[updateViewState] sahale_state updated to:"); // DEBUG
    console.debug(sahale_state); // DEBUG
  }

  return state;
}());
