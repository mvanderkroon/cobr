/*
 * Grid object
 */
var Grid = function(config, ctx) {
    var self = this;
    var context = ctx;
    context.register(self);

    var default_cfg = {
        parent: '#histogram',
        columns: []
    };

    if (typeof config == "undefined") {
        config = default_cfg;
    } else {
        for (var index in default_cfg) {
            if (typeof config[index] == "undefined") config[index] = default_cfg[index];
        }
    }

    var margin = {
        top: 10,
        right: 30,
        bottom: 30,
        left: 30
    };

    var containerHeight = (($(config.parent).height() == 0) ? $(window).height() : $(config.parent).height()) - margin.top - margin.bottom;
    var containerWidth = (($(config.parent).width() == 0) ? $(window).width() : $(config.parent).width()) - margin.left - margin.right;

    var table = d3.select(config.parent)
        .append("table")
        .style("cursor", "default")
        .attr("class", "table table-hover")
        .attr("width", "100%");

    var thead = table.append("thead")
        .append("tr")
        .selectAll("th")
        .data(config.columns)
        .enter()
        .append("th")
        .text(function(column) {
            return column;
        });

    var tbody = table.append("tbody");

    var clickednodes = [];

    Grid.prototype.render = function(data) {
    	// data = _.sortBy(data, function(obj){ return obj.tablename; });
        console.log(data);

        // create a row for each object in the data
        var rowselection = tbody.selectAll("tr")
            .data(data, function(d, i) {
                return d.id;
            })
            .attr("id", function(d, i) {
                return d.id;
            })
            .on('click', context.nodeClicked);

        rowselection.enter()
            .append("tr")
            .attr("id", function(d, i) {
                return d.id;
            })
            .on('click', context.nodeClicked);

        rowselection.exit().remove();

        // create a cell in each row for each column
        var cells = rowselection.selectAll("td")
            .data(function(row) {
                return config.columns.map(function(column) {
                    return {
                        column: column,
                        value: row[column]
                    };
                });
            })
            .html(function(d) {
                return d.value;
            });

        cells.enter()
            .append("td")
            .html(function(d) {
                return d.value;
            });

        cells.exit().remove();

        table.selectAll("tbody tr") 
	        .sort(function(a, b) {
                return d3.ascending(a.tablename, b.tablename);
	        });
    }

    Grid.prototype.handleKeyUp = function(keyCode) {
        if (keyCode == 27) { // escape
            self.unhighlightNodes();
            self.unhighlightLinks();

            self.render(context.nodes());
            
        } else if (keyCode == 69) { // 'e' for expand selection
            var selectednodes = context.selectedNodes();
            
            self.render(selectednodes);
        }
    }

    Grid.prototype.handleClickNode = function(d) {
        var selectednodes = context.selectedNodes();
        self.render(selectednodes);
    }

    Grid.prototype.handleClickLink = function(d) {
        
    }

    Grid.prototype.highlightNodes = function(nodes) {
        var ids = _.pluck(nodes, 'id');

        table.selectAll("tr")
            .filter(function(row) {
                if (row === undefined || row.id === undefined) return false;
                else return true;
            })
            .style("opacity", 0.4)
            .filter(function(row) {
                return ids.indexOf(row.id) != -1;
            })
            .style("opacity", 1)
            .style("font-weight", "bold")
            .style("background-color", "#c5c5c5");
    }

    Grid.prototype.unhighlightNodes = function() {
        table.selectAll("tr")
            .filter(function(row) {
                if (row === undefined || row.id === undefined) return false;
                else return true;
            })
            .style("opacity", 1)
            .style("font-weight", "normal")
            .style("background-color", "white");
    }

    Grid.prototype.unhighlightLinks = function() {
        // not implemented, but called from context - don't delete
    }
}
