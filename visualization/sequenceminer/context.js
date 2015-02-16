function context(basedata) {
	var self = this;

	self.treeview = basedata.tree;
	self.tabularview = basedata.tabular;
	self.basedata = basedata;

	var listeners = [];

	self.defaultnodecolor = 'black';

	var tooltip = d3.select('body').append("div")
		.attr("class", "tooltip")
		.style("opacity", 0);

	self.getNodeColor = function(node) {
		if (node.name in basedata.nodecolors) {
			return basedata.nodecolors[node.name];
		}
		return self.defaultnodecolor;
	}

	// self.getTabular = function(fn) {

	// 	var filtered = basedata.objects;
	// 	if (fn) {
	// 		filtered = _.filter(basedata.objects, fn);
	// 	}

	// 	var seen = _.countBy(filtered, function(obj) { return obj.sex });

	// 	var data = [];
	// 	_.each(seen, function(v, k) {
	// 		data.push({"var": k, "count": v});
	// 	});

	// 	return data;
	// }

	// self.getTree = function(fn) {
	// 	return t.filter(basedata.tree, function(node, par) {
	// 		if (fn && !fn(node)) return false;

	//     	var nnode = {};
	//     	for (prop in node) if (prop != 'children') nnode[prop] = node[prop];

	//     	return nnode;
	// 	});
	// }

	self.register = function(listener, type) {
		listeners.push({"listener": listener, "type": type});
	}

	self.mouseoverNode = function(d) {
		tooltip.transition()
			.duration(200)
			.style("opacity", 0.8);
		console.log(d);
		tooltip
			.html("name: <b>" + d.name + "</b> <br/> frequency: <b>" +
				d.value + " (" + Math.round((d.value/basedata.nodecount) * 10000) / 100 +
					"%) </b> <br/> depth: <b>" + d.depth + "</b>")
			.style("left", ((d3.event.pageX) + 20) + "px")
			.style("top", (d3.event.pageY) + "px");

		for (var i in listeners) {
			listeners[i].listener.handleMouseoverNode(d);
		}
	}

	self.mouseleaveNode = function(d) {
		tooltip.transition()
			.duration(500)
			.style("opacity", 0);

		for (var i in listeners) {
			listeners[i].listener.handleMouseleaveNode(d);
		}
	}

	self.clickNode = function(d) {
		for (var i in listeners) {
			listeners[i].listener.handleClickNode(d);
		}
	}

	var view_root;
	self.dblclickNode = function(d) {

		// process tree to get subtree
		if (d.parent && view_root && view_root.id == d.id) { // user clicked root / we are going to be drilling up
			view_root = d.parent;
			self.treeview = d.parent;
		} else { // drilling down
			view_root = d;
			self.treeview = d;
		}

		// get tabular subset
		var ids = [];
		var objs = [];
		t.dfs(self.treeview, function(node, par, ctrl) {
			if (!(node.id in ids)) {
		    	ids.push(node.id);
		    	objs = objs.concat(node.obj);
			}
		});

		self.tabularview = _.uniq(objs, function(obj) {
			return obj.id;
		});

		// notify listeners
		for (var i in listeners) {
			listeners[i].listener.handleDblclickNode();
		}
	}

	self.globalKeyDown = function() {
		if (d3.event.keyCode == 27) { // ESCAPE key -> reset all visualizations
			for (var i in listeners) {
				if (listeners[i].type == 'hierarchical') {
					listeners[i].listener.render(basedata.tree);
				}
				if (listeners[i].type == 'tabular') {
					listeners[i].listener.render(basedata.tabular);
				}
			}
		}
	}

	d3.select("body")
	    .on("keydown", function() {
	    	self.globalKeyDown();
	    });
}