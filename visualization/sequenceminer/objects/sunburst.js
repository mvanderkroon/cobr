function sunburst(config, context) {
	var self = this;
	var type = 'hierarchical';
	context.register(self, type);

	var containerHeight = ($(config.parent).height() == 0) ? $(window).height() : $(config.parent).height();
	var containerWidth = ($(config.parent).width() == 0) ? $(window).width() : $(config.parent).width();

	var mouseoverflag = false;

	var radius = Math.min(containerWidth, containerHeight) / 2,
		x = d3.scale.linear()
			.range([0, 2 * Math.PI]),
		y = d3.scale.sqrt()
			.range([0, radius]);

	var svg = d3.select(config.parent).append("svg")
		.attr("width", containerWidth)
		.attr("height", containerHeight)
		.append("g")
		.attr("transform", "translate(" + containerWidth / 2 + "," + (containerHeight / 2 + 10) + ")")
		// .on("mouseover", function(d) {
			
		// 	console.log(x.invert(d3.event.pageX));
		// 	console.log(y.invert(d3.event.pageY));
		// 	console.log('');

			
		// })
		;

	

	var partition = d3.layout.partition()
		.value(function(d) {
			return d.size
		});		

	var arc = d3.svg.arc()
		.startAngle(function(d) {
			return Math.max(0, Math.min(2 * Math.PI, x(d.x)));
		})
		.endAngle(function(d) {
			return Math.max(0, Math.min(2 * Math.PI, x(d.x + d.dx)));
		})
		.innerRadius(function(d) {
			return Math.max(0, y(d.y));
		})
		.outerRadius(function(d) {
			return Math.max(0, y(d.y + d.dy));
		});

	var nodes = svg.selectAll("path");

	self.render = function(data) {
		// update the nodes
		nodes = nodes.data(partition(data), function(d) {
			return d.id; //TBD: data identity isn't preserved!!
		});

		nodes.enter()
			.append("path")
			.attr("class", "node")
			.on("click", function(d) {
				context.clickNode(d);
			})
			.on("dblclick", function(d) {
				context.dblclickNode(d);
			})
			.on("mouseover", function(d) {
				context.mouseoverNode(d);
			})
			.on("mouseleave", function(d) {;
				context.mouseleaveNode(d);
			})
			.style("fill", function(d) {
				return context.getNodeColor(d);
			})
			.style("opacity", config.basecfg.default_opacity)
			.attr("d", arc);

		// exit any old nodes
		nodes.exit().remove();

		// update nodes
		nodes
			// .style("fill", function(d) {
			// 	return context.getNodeColor(d);
			// })
			// .style("opacity", config.basecfg.default_opacity)
			// .transition().duration(750)
			.attr("d", arc);
			// .attr("d", function(d, i) {
			// 	var xd = d3.interpolate(x.domain(), [d.x, d.x + d.dx]),
			// 		yd = d3.interpolate(y.domain(), [d.y, 1]),
			// 		yr = d3.interpolate(y.range(), [d.y ? 20 : 0, radius]);

			// 	// x.domain(xd(t));
			// 	// y.domain(yd(t)).range(yr(t));
			// 	return arc(d);


			// 	// return i ? function(t) {
			// 	// 	return arc(d);
			// 	// } : function(t) {
			// 	// 	x.domain(xd(t));
			// 	// 	y.domain(yd(t)).range(yr(t));
			// 	// 	return arc(d);
			// 	// };
			// });
	}

	/**
	USER INTERACTION
	**/

	self.handleMouseoverNode = function(d) {
		var sequenceArray = getAncestors(d);

		svg.selectAll(".node")
			.style("stroke", "transparent")
			.style("opacity", config.basecfg.unhighlight_opacity)
			.filter(function(node) {
				return sequenceArray['ids'].hasOwnProperty(node.id);
			})
			.style("stroke", "black")
			.style("stroke-width", "2")
			.style("opacity", config.basecfg.highlight_opacity);
	}

	self.handleMouseleaveNode = function(d) {
		svg.selectAll(".node")
			.style("stroke", "transparent")
			.style("opacity", config.basecfg.highlight_opacity);

		svg.selectAll(".node")
			.style("stroke", "white")
			.style("stroke-width", ".5px")
			.style("opacity", config.basecfg.highlight_opacity);

		// tooltip.transition()
		// 	.duration(500)
		// 	.style("opacity", 0);
	}

	self.handleClickNode = function(d) {
		// TBD
	}

	self.handleDblclickNode = function(d) {
		self.render(context.treeview);

		// svg.selectAll(".node").transition()
		// 	.duration(750)
		// 	.attrTween("d", arcTween(d));
	}

	/**
	UTILITY METHODS
	 **/

	// function computeTextRotation(d) {
	// 	var angle = x(d.x + d.dx / 2) - Math.PI / 2;
	// 	return angle / Math.PI * 180;
	// }

	// Interpolate the scales!
	// function arcTween(d) {
	// 	var xd = d3.interpolate(x.domain(), [d.x, d.x + d.dx]),
	// 		yd = d3.interpolate(y.domain(), [d.y, 1]),
	// 		yr = d3.interpolate(y.range(), [d.y ? 20 : 0, radius]);

	// 	return function(d, i) {
	// 		return i ? function(t) {
	// 			return arc(d);
	// 		} : function(t) {
	// 			x.domain(xd(t));
	// 			y.domain(yd(t)).range(yr(t));
	// 			return arc(d);
	// 		};
	// 	};
	// }

	var default_cfg = {
		parent: '#sunburst',
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

	self.render(context.treeview);

	return self.render;
}