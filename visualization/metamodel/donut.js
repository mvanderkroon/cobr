function donut(config) {
    var self = this;
    var type = 'tabular';

    var containerHeight = (($(config.parent).height() == 0) ? $(window).height() : $(config.parent).height());
    var containerWidth = (($(config.parent).width() == 0) ? $(window).width() : $(config.parent).width());

    var radius = Math.min(containerWidth, containerHeight) / 2;

    var color = d3.scale.category10();

    // var color = d3.scale.ordinal()
    //     .range(["#acacac", "#1f77b4", "#d62728", "#17becf"])
    //     .domain(['null', 'positive', 'negative', 'zero']);

// #1f77b4
// #ff7f0e
// #2ca02c
// #d62728
// #9467bd
// #8c564b
// #e377c2
// #7f7f7f
// #bcbd22
// #17becf

    var arc = d3.svg.arc()
        .outerRadius(radius - 0)
        .innerRadius(radius - 12);

    var pie = d3.layout.pie()
        .sort(null)
        .value(function(d) {
            return d.value;
        });
    
    var svg = d3.select(config.parent).append("svg")
        .attr("width", containerWidth)
        .attr("height", containerHeight)
        .append("g");

    var tooltip = d3.select('body').append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);

    self.render = function(data) {
        data.forEach(function(d) {
            d.value = +d.value;
        });

        var g = svg.selectAll(".arc")
            .data(pie(data))
            .enter().append("g")
            .attr("class", "arc");

        g.append("path")
            .attr("d", arc)
            .attr("transform", "translate(" + containerWidth / 2 + "," + containerHeight / 2 + ")")
            .style("fill", function(d) {
                return color(d.data.category);
            })
            .style("opacity", "0.8")
            .on('mouseover', self.handleMouseoverNode)
            .on('mouseout', self.handleMouseleaveNode);

        // CENTER LABEL
        // var pieLabel = svg.append("svg:text")
        //     .attr("transform", "translate(" + (containerWidth / 2) + "," + (containerHeight / 2) + ")")
        //     .attr("dy", ".35em").attr("class", "chartLabel")
        //     .attr("text-anchor", "middle")
        //     .text('A');

        // g.append("text")
        //     .attr("transform", function(d) {
        //         return "translate(" + arc.centroid(d) + ")";
        //     })
        //     .attr("dy", ".35em")
        //     .style("text-anchor", "middle")
        //     .style("fill", "white")
        //     .style('font-size', '6pt')
        //     .text(function(d) {
        //         if (d.data.value > 0) return d.data.value;
        //     });
    }

    /**
    USER INTERACTION
    **/

    self.handleMouseoverNode = function(d) {
        var ttstr = "<b>" + d.data.category + "</b> ";
        ttstr += "<b>" + d.data.value + "</b>";

        tooltip.transition()
            .duration(200)
            .style("opacity", 0.8);

        tooltip
            .html(ttstr)
            .style("left", ((d3.event.pageX) + 20) + "px")
            .style("top", (d3.event.pageY) + "px");
    }

    self.handleMouseleaveNode = function(d) {
        tooltip.transition()
            .duration(200)
            .style("opacity", 0);
    }

    self.handleClickNode = function(d) {
        // TBD

    }

    self.handleDblclickNode = function(d) {

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

    return self;
}
