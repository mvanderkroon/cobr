/*
 * Grid object
 */
var Grid = function(config) {
    var self = this;

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
        .style("overflow", "auto")
        .style("margin", "10px")
        .append("table")
        .attr("width", "100%");

    var thead = table.append("thead")
        .style("font-size", "10pt")
        .style("background-color", "#414141")
        .style("color", "white")
        .append("tr")
        .selectAll("th")
        .data(config.columns)
        .text(function(column) {
            return column;
        })
        .enter()
        .append("th")
        .text(function(column) {
            return column;
        });

    var tbody = table.append("tbody");

    Grid.prototype.render = function(data) {
        console.log(data);



        // create a row for each object in the data
        var rowselection = tbody.selectAll("tr")
            .data(data, function(d, i) {
                return d.id;
            })
            .attr("id", function(d, i) {
                return d.id;
            });

        rowselection.enter()
            .append("tr")
            .attr("id", function(d, i) {
                return d.id;
            });

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
            .style("padding-left", "10px")
            .style("padding-top", "2px")
            .style("padding-bottom", "2px")
            .html(function(d) {
                return d.value;
            });

        cells.enter()
            .append("td")
            .style("padding-left", "10px")
            .style("padding-top", "2px")
            .style("padding-bottom", "2px")
            .html(function(d) {
                return d.value;
            });

        cells.exit().remove();
    }

    Grid.prototype.highlightNodes = function(nodes) {
        var ids = _.pluck(nodes, 'id');

        table.selectAll("tr")
            .style("opacity", 0.2)
            .filter(function(row) {
                if (row === undefined) return true;
                return ids.indexOf(row.id) != -1;
            })
            .style("opacity", 1);
    }

    Grid.prototype.unhighlightNodes = function() {
        table.selectAll("tr")
            .style("opacity", 1);
    }
}
