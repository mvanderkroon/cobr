/*
 * ForceLayout object
 */
var ForceLayout = function(config, ctx) {
    var self = this;
    var context = ctx;
    context.register(self);

    var default_cfg = {
        parent: '#forcelayout',
        nodeMinRadius: 5,
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

    var containerHeight = (($(config.parent).height() == 0) ? $(window).height() : $(config.parent).height());
    var containerWidth = (($(config.parent).width() == 0) ? $(window).width() : $(config.parent).width());

    // var zoom = d3.behavior.zoom()
    //     .scaleExtent([1, 10])
    //     .on("zoom", zoomed);

    var svg = d3.select(config.parent).append('svg')
        .attr('width', containerWidth)
        .attr('height', containerHeight)
        .append('g');
        // .call(zoom);

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

    var cscale;
    var sscale = d3.scale.pow(); 

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

    ForceLayout.prototype.render = function(data) {
        console.log(data);
        cscale = d3.scale.category10();

        if (data) {
            nodes = data.nodes;
            links = data.links;

            nmax = _.max(nodes, function(node){ return node[config.nodeSizeBy]; });
            nmin = _.min(nodes, function(node){ return node[config.nodeSizeBy]; });

            cscale(nodes[0].db_catalog)
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

        var linkselection = svg.selectAll(".link")
            .data(links, function(d, i) {
                return d.id;
            })
            .attr("class", "link")
            .attr("id", function(d) {
                return d.id;
            })
            .style("stroke", function(d) {
                return linkColor(d);
            })
            .on('mouseover', self.handleMouseoverLink)
            .on('mouseout', self.handleMouseleaveLink);

        var link = linkselection.enter()
            .append("line")
            .attr("class", "link")
            .attr("id", function(d) {
                return d.id;
            })
            .style("stroke", function(d) {
                return linkColor(d);
            })
            .on('mouseover', self.handleMouseoverLink)
            .on('mouseout', self.handleMouseleaveLink);

        linkselection.exit().remove();

        var nodeselection = svg.selectAll(".node")
            .data(nodes, function(d, i) {
                return d.id;
            })
            .attr("class", "node")
            .attr("id", function(d) {
                return d.index;
            })
            .attr("r", function(d) {
                return nodeSize(d);
            })
            .style("fill", function(d) {
                return nodeColor(d);
            })
            .call(force.drag)
            .on('click', context.nodeClicked)
            .on('dblclick', self.handleDblclickNode)
            .on('mouseover', self.handleMouseoverNode)
            .on('mouseout', self.handleMouseleaveNode);

        var node = nodeselection.enter()
            .append("circle")
            .attr("class", "node")
            .attr("id", function(d) {
                return d.index;
            })
            .attr("r", function(d) {
                return nodeSize(d);
            })
            .style("fill", function(d) {
                return nodeColor(d);
            })
            // .call(force.drag)
            .on('click', context.nodeClicked)
            .on('dblclick', self.handleDblclickNode)
            .on('mouseover', self.handleMouseoverNode)
            .on('mouseout', self.handleMouseleaveNode);

        nodeselection.exit().remove();

        var drag = force.drag()
            .on("dragstart", dragstart);

        function dragstart(d) {
            d3.select(this).classed("fixed", d.fixed = true);
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

    ForceLayout.prototype.handleKeyUp = function(keyCode) {
        if (keyCode == 27) { // escape
            self.unhighlightNodes();
            self.unhighlightLinks();

            var nodes = context.nodes();
            var links = context.links();

            self.render({links: links, nodes:nodes});
            
        } else if (keyCode == 69) { // 'e' for expand selection
            var selectednodes = context.selectedNodes();
            var selectedlinks = context.selectedLinks();
            
            self.highlightNodes(selectednodes);
            self.highlightLinks(selectedlinks);
        }
    }

    ForceLayout.prototype.handleMouseoverLink = function(d) {
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

    ForceLayout.prototype.handleMouseleaveLink = function(d) {
        tooltip.transition()
            .duration(200)
            .style("opacity", 0);   
    }

    ForceLayout.prototype.handleMouseoverNode = function(d) {
        ttstr = d.db_catalog + '.' + d.db_schema + '.' + d.tablename + '<br/>'
        ttstr += '<b>row count: </b>' + d.num_rows + '<br/>'
        ttstr += '<b>col count: </b>' + d.num_columns + '<br/>'
        ttstr += '<b>#inlinks: </b>' + d.num_explicit_inlinks + '<br/>'
        ttstr += '<b>#outlinks: </b>' + d.num_explicit_outlinks + '<br/>'
        ttstr += '<b>comment: </b>' + d.comment + '<br/>'
        ttstr += '<b>tags: </b>' + d.tags;

        tooltip.transition()
            .duration(200)
            .style("opacity", 0.8);

        tooltip
            .html(ttstr)
            .style("left", ((d3.event.pageX) + 20) + "px")
            .style("top", (d3.event.pageY) + "px");
    }

    ForceLayout.prototype.handleMouseleaveNode = function(d) {
        tooltip.transition()
            .duration(200)
            .style("opacity", 0);
    }

    ForceLayout.prototype.handleClickNode = function(d) {
        var selectednodes = context.selectedNodes();
        var selectedlinks = context.selectedLinks();
        
        self.highlightNodes(selectednodes);
        self.highlightLinks(selectedlinks);
    }

    ForceLayout.prototype.handleClickLink = function(d) {
        
    }

    ForceLayout.prototype.handleDblclickNode = function(d) {
        // d3.select(this).classed("fixed", d.fixed = false).style("fill", function(d) {
        //     if (d.numrows == 0) return 'white';
        //     return nodeColor(d);
        // });
    }

    ForceLayout.prototype.highlightNodes = function(nodes) {
        var ids = _.pluck(nodes, 'id');
        
        svg.selectAll(".node")
            .style("opacity", 0.2)
            .filter(function(node) {
                return ids.indexOf(node.id) != -1;
            })
            .style("opacity", 1)
            .style("fill", "red");
    }

    ForceLayout.prototype.unhighlightNodes = function() {
        svg.selectAll(".node")
            .style("fill", function(d) {
                return nodeColor(d);
            })
            .style("opacity", 0.8);
    }

    ForceLayout.prototype.highlightLinks = function(links) {
        var ids = _.pluck(links, 'id');
        
        svg.selectAll(".link")
            .style("opacity", 0.1)
            .filter(function(link) {
                return ids.indexOf(link.id) != -1;
            })
            .style("opacity", 1)
            .style("fill", "red");

    }

    ForceLayout.prototype.unhighlightLinks = function() {
        svg.selectAll(".link")
            .style("fill", function(d) {
                return linkColor(d);
            })
            .style("opacity", 0.6);
    }

    // function zoomed() {
    //     svg.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
    // }

    /**
	UTILITY METHODS
	 **/


}
