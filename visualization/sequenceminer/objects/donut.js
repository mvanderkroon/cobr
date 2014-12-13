function donut(config, context) {
    var self = this;
    var type = 'tabular';
    context.register(self, type);

    var containerHeight = (($(config.parent).height() == 0) ? $(window).height() : $(config.parent).height());
    var containerWidth = (($(config.parent).width() == 0) ? $(window).width() : $(config.parent).width());

    var radius = Math.min(containerWidth, containerHeight) / 2;

    var color = d3.scale.ordinal()
        .range(["#98abc5", "#8a89a6", "#7b6888", "#6b486b", "#a05d56", "#d0743c", "#ff8c00"]);

    var arc = d3.svg.arc()
        .outerRadius(radius - 10)
        .innerRadius(radius - 70);

    var pie = d3.layout.pie()
        .sort(null)
        .value(function(d) {
            return d.population;
        });

    var svg = d3.select(config.parent).append("svg")
        .attr("width", containerWidth)
        .attr("height", containerHeight)
        .append("g")
        .attr("transform", "translate(" + containerWidth / 2 + "," + containerHeight / 2 + ")");

    self.render = function(data) {
        data.forEach(function(d) {
            d.population = +d.population;
        });

        var g = svg.selectAll(".arc")
            .data(pie(data))
            .enter().append("g")
            .attr("class", "arc");

        g.append("path")
            .attr("d", arc)
            .style("fill", function(d) {
                return color(d.data.age);
            });

        g.append("text")
            .attr("transform", function(d) {
                return "translate(" + arc.centroid(d) + ")";
            })
            .attr("dy", ".35em")
            .style("text-anchor", "middle")
            .text(function(d) {
                return d.data.age;
            });
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
        parent: '#donut',
        basecfg: {}
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
    //  return node.depth < 22;
    // }));

    self.render(context.tabularview);

    return self.render;
}
