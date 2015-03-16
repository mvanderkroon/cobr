function basedata(config) {
	var self = this;

	var default_cfg = {
		//nodecolorpalette: ['#0066ff', '#00cc33', '#fc3307', '#b900ff', '#ffcc00', ],

		nodecolorpalette: ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'],
		nodenullcolor: 'black',
		separator: '$'
	};

	if (typeof config == "undefined") {
		config = default_cfg;
	} else {
		for (var index in default_cfg) {
			if (typeof config[index] == "undefined") config[index] = default_cfg[index];
		}
	}

	var p = d3.layout.partition()
		.value(function(d) {
			return d.size
		});

	self.tabular = {};
	self.tree = {};
	self.partition = {};

	self.nodecount = 0;
	self.nodecounts = {};
	self.nodecolors = {};

	self.init = function(lines) {
		// 1) populate tree
		//	   	* calculate treemetadata
		// 		* calculate nodecounts
		// 2) convert tree to partition layout
		// 3) assign tree totalsize
		// 4) assign node count
		// 5) assign node colors

		self.tabular = lines;
		var result = populatetree(lines, config.separator);

		self.tree = result.tree;
		self.partition = d3.layout.partition()
			.value(function(d) {
				return d.size;
			})(self.tree);

		self.nodecount = lines.length;
		self.nodecounts = result.nodecounts;
		self.nodecolors = assignNodeColors(result.nodecounts, config.nodecolorpalette, config.nodenullcolor);
	}

	self.getNodeList = function(_tree) {
		tree = _tree || self.tree;

		var nodes = [],
			i = 0;

		function recurse(node) {
			if (node.children) node.children.forEach(recurse);
			nodes.push(node);
		}

		recurse(tree);
		return nodes;
	}

	var assignNodeColors = function(dict, colorlst, nullcolor) {
		delete dict['END'];
		delete dict['root'];

		var colors = {
			'END': 'white',
			'root': '#333333'
		};

		var sorted = dictsort(dict);

		for (var i = 0; i < sorted.length; i++) {

			var key = sorted[i][0];
			var value = sorted[i][1];

			//if (i < colorlst.length) colors[key] = colorlst[i];
			// else colors[key] = nullcolor;
			colors[key] = colorlst[i%colorlst.length];

		}

		return colors;
	}

	var dictsort = function(dict) {
		var tuples = [];

		for (var key in dict) tuples.push([key, dict[key]]);

		tuples.sort(function(a, b) {
			a = a[1];
			b = b[1];

			return a > b ? -1 : (a < b ? 1 : 0);
		});

		return tuples;
	}

	var populatetree = function(lines, separator) {
		var getChildByName = function(node, childname) {
			var candidate = null;
			node.children.forEach(function(child, index) {
				if (child.name === childname) {
					candidate = child;
					return;
				}
			});
			return candidate;
		}


		var nc = {};
		var countstate = function(state) {
			if (!(state in nc)) {
				nc[state] = 0;
			}
			nc[state]++;
		}

		var root = {
			"name": "root",
			"id": 0,
			"obj":[],
			"children": []
		};

		var id = 1;

		lines.forEach(function(line) {
			var c = 0;
			var currentNode = root;

			var states = line.sequence.split(separator);
			states.push('END');

			states.forEach(function(state) {
				countstate(state);
				// currentNode.obj.push(line);

				if (c + 1 < states.length) {
					var candidate = getChildByName(currentNode, state);

					if (candidate === null) {
						candidate = {
							"name": state,
							"id": id,
							"children": []
						};

						currentNode.children.push(candidate);
					}

					currentNode = candidate;
				} else {
					var candidate = getChildByName(currentNode, 'END');
					if (candidate === null) {
						candidate = {
							"name": 'END',
							"id": id,
							"obj": [line],
							"size": 1
						};

						currentNode.children.push(candidate);
					} else {
						// console.log(candidate);
						candidate.size++;
					}
				}

				id++;
				c++;
			});
		});

		return {
			tree: root,
			nodecounts: nc
		};
	}
}