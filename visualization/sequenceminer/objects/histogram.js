function histogram(config, context) {
	var self = this;
	var type = 'tabular';
	context.register(self, type);

	var margin = {top: 10, right: 30, bottom: 30, left: 30};
	var containerHeight = (($(config.parent).height() == 0) ? $(window).height() : $(config.parent).height()) - margin.top - margin.bottom;
	var containerWidth = (($(config.parent).width() == 0) ? $(window).width() : $(config.parent).width()) - margin.left - margin.right;

	var mouseoverflag = false;

	var svg = d3.select(config.parent).append("svg")
		.attr("width", containerWidth + margin.left + margin.right)
		.attr("height", containerHeight + margin.top + margin.bottom)
		.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

	var xaxis = null;
	

	self.render = function(data) {
		var seen = _.countBy(data, function(obj) { return obj[config.variable] });

		data = [];
		_.each(seen, function(v, k) {
			data.push({"var": k, "count": v});
		});
		data = _.sortBy(data, function(obj){ return obj.var; }); // order by count, asc - do a reverse() to achieve desc

		// A formatter for counts.
		var formatCount = d3.format(",.0f");

		var variables = _.pluck(data, 'var');
		var counts = _.pluck(data, 'count');
		
		var x = d3.scale.ordinal().domain(variables).rangeRoundBands([0, containerWidth], .1)
		var y = d3.scale.linear().domain([0, d3.max(counts)]).range([containerHeight, 0]);

		if (xaxis) {
			xaxis.remove();
		}
		
 		var xAxis = d3.svg.axis()
		    .scale(x)
		    .orient("bottom");

		var bars = svg.selectAll(".bar")
		    .data(data, function(d) { return d.var + ':' + d.count; });

		bars.exit().remove();

		var bar = bars.enter().append("g")
		    .attr("class", "bar");

		bar.append("rect")
		bar.append("text");

		bars.selectAll("rect")
			.attr("y", function(d) {
				return y(d.count);
			})
			.attr("x", function(d) {
				return x(d.var);
			})
		    .attr("width", x.rangeBand())
		    .attr("height", function(d) { return containerHeight - y(d.count); });

		bars.selectAll("text")
		    .attr("dy", ".75em")
		    .attr("y", function(d) {
				return y(d.count) - 10;
			})
			.attr("x", function(d) {
				return x(d.var) + x.rangeBand() / 2;
			})
		    .attr("text-anchor", "middle")
		    .style("fill", "black")
		    .text(function(d) { return formatCount(d.count); });
		
		xaxis = svg.append("g")
		    .attr("class", "axis")
		    .attr("transform", "translate(0," + containerHeight + ")")
		    .call(xAxis);
	}

	/**
	USER INTERACTION
	**/

	self.handleMouseoverNode = function(d) {
		
	}

	self.handleMouseleaveNode = function(d) {
		
	}

	self.handleClickNode = function(d) {
		// TBD

	}

	self.handleDblclickNode = function(d) {
		self.render(context.tabularview);
	}

	/**
	UTILITY METHODS
	 **/

	var default_cfg = {
		parent: '#histogram',
		basecfg: {
			'default_opacity': 1,
			'highlight_opacity': 1,
			'unhighlight_opacity': 0.3
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
	// self.render(context.getTree(function(node) {
	// 	return node.depth < 22;
	// }));

	self.render(context.tabularview);

	return self.render;
}