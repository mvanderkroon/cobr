function breadcrumb(config, context) {
	var self = this;
	context.register(self);

	var w = $("#sequence").width();

	// Add the svg area.
	var trail = d3.select("#sequence").append("svg:svg")
		.attr("width", w)
		.attr("height", 50)
		.attr("id", "trail");
	// Add the label at the end, for the percentage.
	trail.append("svg:text")
		.attr("id", "endlabel")
		.style("fill", "#000");

	self.render = function() {
		// the breadcrumb by definition only renders data elements upon user interaction
	}

	// measure the width and height of text as it will be rendered on the screen


	function getTextWidth(text, font) {
	    // re-use canvas object for better performance
	    var canvas = getTextWidth.canvas || (getTextWidth.canvas = document.createElement("canvas"));
	    var context = canvas.getContext("2d");
	    context.font = font;
	    var metrics = context.measureText(text);
	    return metrics.width;
	};

	function breadcrumbPoints(d, i) {
		var points = [];
		points.push("0,0");
		points.push(d.w + ",0");
		points.push(d.w + config.dimensions.t + "," + (config.dimensions.h / 2));
		points.push(d.w + "," + config.dimensions.h);
		points.push("0," + config.dimensions.h);
		if (i > 0) { // Leftmost breadcrumb; don't include 6th vertex.
			points.push(config.dimensions.t + "," + (config.dimensions.h / 2));
		}
		return points.join(" ");
	}

	/**
    USER INTERACTION
    **/

	self.handleMouseoverNode = function(node) {
		var ancestors = getAncestors(node)['path'];

		var percentage = (100 * node.value / context.basedata.tabular.length).toPrecision(3);
		var percentageString = percentage + "%";
		if (percentage < 0.1) {
			percentageString = "< 0.1%";
		}
		var absString = node.value;

		// Data join; key function combines name and depth (= position in sequence).
		var g = d3.select("#trail")
			.selectAll("g")
			.data(ancestors, function(d) {
				d.w = getTextWidth(d.name, '10pt Helvetica Neue')

				return d.name + d.depth;
			});

		// Add breadcrumb and label for entering nodes.
		var entering = g.enter().append("svg:g");

		entering.append("svg:polygon")
			.attr("points", breadcrumbPoints)
			.style("fill", function(d) {
				return context.getNodeColor(d);
			});

		entering.append("svg:text")
			.attr("x", function(d, i) {
				return (d.w + config.dimensions.t) / 2
			})
			.attr("y", config.dimensions.h / 2)
			.attr("dy", "0.35em")
			.attr("text-anchor", "middle")
			.text(function(d) {
				return d.name;
			})
			.style("fill", function(d) {
				if (d.name == 'END') return 'black';
				return 'white';
			});

		// Set position for entering and updating nodes.
		g.attr("transform", function(d, i) {
			var sum = 0;
			var c = 0;
			while (c < i) {
				sum += ancestors[c].w;
				sum += config.dimensions.s;
				c++;
			}

			return "translate(" + sum + ", 0)";
		});

		// Remove exiting nodes.
		g.exit().remove();

		// Now move and update the percentage at the end.
		d3.select("#trail").select("#endlabel")
			.attr("x", function(d, i) {
				return _.reduce(ancestors, function(memo, obj) {
					return memo + obj.w + config.dimensions.s;
				}, 0) + getTextWidth(percentageString, '10pt Helvetica Neue') + config.dimensions.s;
			})
			.attr("y", config.dimensions.h / 2)
			.attr("dy", "0.35em")
			.attr("text-anchor", "right")
			.text(percentageString + " (" + absString + ")")
			.style("font-size", "12pt");

		// Make the breadcrumb trail visible, if it's hidden.
		d3.select("#trail")
			.style("visibility", "");
	}

	self.handleMouseleaveNode = function(d) {
		d3.select("#trail")
			.style("visibility", "hidden");
	}

	self.handleClickNode = function(d) {

	}

	self.handleDblclickNode = function(d) {

	}

	var default_cfg = {
		parent: '#sequence',
		dimensions: {
			h: 30,
			s: 5,
			t: 10
		}
	};

	if (typeof config == "undefined") {
		config = default_cfg;
	} else {
		for (var index in default_cfg) {
			if (typeof config[index] == "undefined") config[index] = default_cfg[index];
		}
	}

	self.render.attr = function(key, value) {
		if (typeof value == "undefined") return config[key];
		config[key] = value;
		return self.render();
	}

	/** INIT **/
	self.render();

	return self.render;
}