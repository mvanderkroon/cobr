function forcelayout(config) {
    var self = this;

    var containerHeight = (($(config.parent).height() == 0) ? $(window).height() : $(config.parent).height());
    var containerWidth = (($(config.parent).width() == 0) ? $(window).width() : $(config.parent).width());

    var svg = d3.select(config.parent).append("svg")
        .attr("width", containerWidth)
        .attr("height", containerHeight)
        .append("g");

    var nodes;
    var links;

    var force = d3.layout.force()
        .charge(-120)
        .linkDistance(30)
        .size([containerWidth, containerHeight])
        .theta([1.2])
        .charge([-450])
        .gravity([1])
        .linkStrength(.5);

    var tooltip = d3.select('body').append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);

    var nmax;
    var nmin;
    var maxRadius = 40;
    var minRadius = 6;

    var cscale = d3.scale.category10();
    var sscale = d3.scale.sqrt(); 

    var linkColor = function(d) {
        if (d.type == 'explicit') return 'white';
        if (d.type == 'implicit') return 'orange';
    }

    var nodeColor =  function(d) {
        if (d.comment && d.comment.length > 0) return cscale(d.comment)
        if (d.num_rows == 0) return 'white';
        return cscale(d[config.nodeColorBy]);
    }

    var nodeSize = function(d) {
        return sscale(d[config.nodeSizeBy]);
    }

    self.render = function(data) {
        if (data) {
            nodes = data.nodes;
            links = data.links;

            nmax = _.max(nodes, function(node){ return node[config.nodeSizeBy]; });
            nmin = _.min(nodes, function(node){ return node[config.nodeSizeBy]; });
        }
        
        sscale.range([config.nodeMinRadius, config.nodeMaxRadius]).domain([nmin[config.nodeSizeBy], nmax[config.nodeSizeBy]]);     

        force
            .nodes(nodes)
            .links(links)
            .linkDistance(function(d) {
                return 0.1;
            })
            .on("tick", tick)
            .start();

        var link = svg.selectAll(".link")
            .data(links)
            .enter().append("line")
            .attr("class", "link")
            .attr("id", function(d) {
                return d.id;
            })
            .style("stroke", function(d) {
                return linkColor(d);
            })
            .on('mouseover', self.handleMouseoverLink)
            .on('mouseout', self.handleMouseleaveLink);

        var node = svg.selectAll(".node")
            .data(nodes)
            .enter().append("circle")
            .attr("class", "node")
            .attr("id", function(d) {
                return 'node_' + d.index;
            })
            .attr("r", function(d) {
                return nodeSize(d);
            })
            .style("fill", function(d) {
                return nodeColor(d);
            })
            .call(force.drag)
            .on("dblclick", self.handleDblclickNode)
            .on('mouseover', self.handleMouseoverNode)
            .on('mouseout', self.handleMouseleaveNode);

        var drag = force.drag()
            .on("dragstart", dragstart);

        function dragstart(d) {
            d3.select(this).classed("fixed", d.fixed = true).style("fill", "orange");
        }

        function tick(e) {
            link.attr("x1", function(d) {
                    return d.source.x;
                })
                .attr("y1", function(d) {
                    return d.source.y;
                })
                .attr("x2", function(d) {
                    return d.target.x;
                })
                .attr("y2", function(d) {
                    return d.target.y;
                });

            node.attr("cx", function(d) {
                    return d.x;
                })
                .attr("cy", function(d) {
                    return d.y;
                });

        };
    }

    /**
	USER INTERACTION
	**/

    self.handleMouseoverLink = function(d) {
        var sourcenode = d.source;
        var targetnode = d.target;

        ttstr = "<b>from: </b>" + sourcenode.db_catalog + "." + sourcenode.db_schema + "." + sourcenode.tablename + "<br/>";
        ttstr += "<b>to: </b>" + targetnode.db_catalog + "." + targetnode.db_schema + "." + targetnode.tablename + "<br/>";
        ttstr += "<b>pkcolumns: </b>" + d.pk_columns + "<br/>";
        ttstr += "<b>fkcolumns: </b>" + d.fk_columns + "<br/>";
        ttstr += "<b>type: </b>" + d.type;

        tooltip.transition()
            .duration(200)
            .style("opacity", 0.8);

        tooltip
            .html(ttstr)
            .style("left", ((d3.event.pageX) + 20) + "px")
            .style("top", (d3.event.pageY) + "px");
    }

    self.handleMouseleaveLink = function(d) {
        tooltip.transition()
            .duration(200)
            .style("opacity", 0);   
    }

    self.handleMouseoverNode = function(d) {
        tooltip.transition()
            .duration(200)
            .style("opacity", 0.8);

        tooltip
            .html(d.db_catalog + '.' + d.db_schema + '.' + d.tablename + '<br/><b>row count: </b>' + d.num_rows + '<br/><b>col count: </b>' + d.num_columns + '<br/><b>#inlinks: </b>' + d.num_explicit_inlinks + '<br/><b>#outlinks: </b>' + d.num_explicit_outlinks)
            .style("left", ((d3.event.pageX) + 20) + "px")
            .style("top", (d3.event.pageY) + "px");
    }

    self.handleMouseleaveNode = function(d) {
        tooltip.transition()
            .duration(200)
            .style("opacity", 0);
    }

    self.handleClickNode = function(d) {
        
    }

    self.handleDblclickNode = function(d) {
        d3.select(this).classed("fixed", d.fixed = false).style("fill", function(d) {
            if (d.numrows == 0) return 'white';
            return nodeColor(d);
        });
    }

    /**
	UTILITY METHODS
	 **/

    var default_cfg = {
        parent: '#histogram',
        nodeMinRadius: 6,
        nodeMaxRadius: 40,
        nodeSizeBy: 'num_rows',
        nodeColorBy: 'db_catalog',
        linkColorBy: 'type'
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

    return self;
}
