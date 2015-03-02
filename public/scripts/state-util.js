var StateUtil = (function() {
  var state = {};

  var SAHALE_INIT_STATE = {
    tx: 40,
    ty: 40,
    scale: 1,
    curid: 0,
    tabs_state: {},
    chart_state: 0
  };
  var sahale_state = null;

  state.captureGraphViewYOffset = function(offset) {
    SAHALE_INIT_STATE.ty = offset;
    console.log("[Set SAHALE_INIT_STATE]: " + JSON.stringify(SAHALE_INIT_STATE)); // DEBUG
  }

  state.clearFlowState = function(flow_id) {
    sessionStorage.removeItem('flowid-' + flow_id);
    console.log("[clearFlowState] for Flow ID: " + flow_id); // DEBUG
  }

  state.getFlowState = function(flow_id) {
    if (sahale_state === null) {
      var new_state = sessionStorage.getItem("flowid-" + flow_id);
      sahale_state = new_state === null ? SAHALE_INIT_STATE : JSON.parse(new_state);
    }
    console.log("[getFlowState] sahale_state returned: " + JSON.stringify(sahale_state)); // DEBUG
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
    console.log("[updateViewState] sahale_state updated to: " + JSON.stringify(sahale_state)); // DEBUG
  }

  return state;
}());
