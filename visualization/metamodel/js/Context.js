var Context = function(config) {
    var self = this;

    var listeners = [];

    var selectednodes = [];

    Context.prototype.register = function(listener) {
    	listeners.push(listener);
    }

    Context.prototype.nodeClicked = function(d) {
    	selectednodes.push(d);

    	for (var i in listeners) {
			listeners[i].highlightNodes(selectednodes);
		}
    }

    Context.prototype.resetGui = function(payload) {
    	selectednodes = [];

    	for (var i in listeners) {
			listeners[i].unhighlightNodes();
			listeners[i].unhighlightLinks();
		}
    }
}