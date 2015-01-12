var Grid = function(ctx, id, channel, columns) {
    var self = this;

    self._type = '_TABULAR';
    self._id = id;
    self._channel = channel;

    self._columns = columns;

    self.channel = function(channel) {
        if (channel === undefined || channel === null) return self._channel;
        else self._channel = channel;
    };

    self.type = function() {
        return self._type;
    };

    self.id = function() {
        return self._id;
    };

    var margin = {
        top: 10,
        right: 30,
        bottom: 30,
        left: 30
    };

    var containerHeight = (($(self.id()).height() == 0) ? $(window).height() : $(self.id()).height()) - margin.top - margin.bottom;
    var containerWidth = (($(self.id()).width() == 0) ? $(window).width() : $(self.id()).width()) - margin.left - margin.right;

    var table = d3.select(self.id())
        .append("table")
        .style("cursor", "default")
        .attr("class", "table table-hover")
        .attr("width", "100%");

    var thead = table.append("thead")
        .append("tr")
        .selectAll("th")
        .data(self._columns)
        .enter()
        .append("th")
        .text(function(column) {
            return column;
        });

    var tbody = table.append("tbody");

    self.render = function(data) {
        var rowselection = tbody.selectAll("tr")
            .data(data, function(d, i) {
                return d.id;
            })
            .attr("id", function(d, i) {
                return d.id;
            })
            .on('click', function(d) {
                ctx.notify(self, { type: '_CLICKNODE'}, d);
            });

        rowselection.enter()
            .append("tr")
            .attr("id", function(d, i) {
                return d.id;
            })
            .on('click', function(d) {
                ctx.notify(self, { type: '_CLICKNODE'}, d);
            });

        rowselection.exit().remove();

        // create a cell in each row for each column
        var cells = rowselection.selectAll("td")
            .data(function(row) {
                return self._columns.map(function(column) {
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
    };

    self.highlightNodes = function(lst) {
        var ids = _.pluck(lst, 'id');

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

    self.unhighlightNodes = function() {
        table.selectAll("tr")
            .filter(function(row) {
                if (row === undefined || row.id === undefined) return false;
                else return true;
            })
            .style("opacity", 1)
            .style("font-weight", "normal")
            .style("background-color", "white");
    }
};
