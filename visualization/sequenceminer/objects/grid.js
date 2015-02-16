function grid(config, context) {
	var self = this;
	var type = 'tabular';
	context.register(self, type);

	var margin = {top: 10, right: 30, bottom: 30, left: 30};
	var containerHeight = (($(config.parent).height() == 0) ? $(window).height() : $(config.parent).height()) - margin.top - margin.bottom;
	var containerWidth = (($(config.parent).width() == 0) ? $(window).width() : $(config.parent).width()) - margin.left - margin.right;

	var table;

	// The table generation function
	function tabulate(data, columns) {
	    var table = d3.select(config.parent)
	    		.append("table")
	    		.attr("class", "table table-hover")
	            .attr("width", "100%"),
	        thead = table.append("thead")
	        tbody = table.append("tbody");

	    // append the header row
	    thead.append("tr")
	        .selectAll("th")
	        .data(columns)
	        .enter()
	        .append("th")
	        .text(function(column) { return column; });

	    // create a row for each object in the data
	    var rows = tbody.selectAll("tr")
	        .data(data)
	        .enter()
	        .append("tr");

	    // create a cell in each row for each column
	    var cells = rows.selectAll("td")
	        .data(function(row) {
	            return columns.map(function(column) {
	                return {column: column, value: row[column]};
	            });
	        })
	        .enter()
	        .append("td")
	        .html(function(d) { return d.value; });

	    return table;
	}

	self.render = function(data) {
		if (table) table.remove();

		table = tabulate(data, ["id", "year", "sequence"]);
	}

	/**
	USER INTERACTION
	**/

	self.handleMouseoverNode = function(d) {

	}

	self.handleMouseleaveNode = function(d) {

	}

	self.handleClickNode = function(d) {
		// get tabular subset
		var ids = [];
		var objs = [];
		t.dfs(d, function(node, par, ctrl) {
			if (!(node.id in ids)) {
		    	ids.push(node.id);
		    	objs = objs.concat(node.obj);
			}
		});

		var subset = _.uniq(objs, function(obj) {
			return obj.id;
		});

		var ids = [];
		_.each(subset, function(obj) {

			if (ids.indexOf(obj.id) < 0) {
				ids.push(obj.id);
			}
		});

		table.select("tbody").selectAll("tr")
			.style("opacity", 0)
			.filter(function(d) {
				return ids.indexOf(d.id) >= 0;
			})
			// .style("background-color", "orange")
			.style("opacity", config.basecfg.highlight_opacity);

	}

	self.handleDblclickNode = function(d) {
		table.select("tbody").selectAll("tr")
			.style("opacity", config.basecfg.highlight_opacity)

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