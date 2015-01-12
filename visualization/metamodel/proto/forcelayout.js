var ForceLayout = function(ctx, id, nodeColorBy, nodeSizeBy) {
    var self = this;

    self._type = '_NETWORK';
    self._id = id;

    self.nodeColorBy = nodeColorBy;
    self.nodeSizeBy = nodeSizeBy;
    self.nodeMinRadius = 6;
    self.nodeMaxRadius = 20;

    self.type = function() {
        return self._type;
    };

    self.id = function() {
        return self._id;
    };

    var containerHeight = (($(self.id()).height() == 0) ? $(window).height() : $(self.id()).height());
    var containerWidth = (($(self.id()).width() == 0) ? $(window).width() : $(self.id()).width());

    var svg = d3.select(self.id()).append('svg')
        .attr('width', containerWidth)
        .attr('height', containerHeight)
        .append('g');
    
    var cscale = d3.scale.category10();

    var nmax, nmin; 

    var sscale = d3.scale.pow();
        

    var linkColor = function(d) {
        return 'white';
    };

    var nodeColor = function(d) {
        return cscale(d[self.nodeColorBy]);
    };

    var nodeSize = function(d) {
        return sscale(d[self.nodeSizeBy]);
    };

    self.render = function(data) {
        var links = data.links;
        var nodes = data.nodes;

        nmax = _.max(nodes, function(node) {
            return node[self.nodeSizeBy];
        });

        nmin = _.min(nodes, function(node) {
            return node[self.nodeSizeBy];
        });

        sscale
            .range([self.nodeMinRadius, self.nodeMaxRadius])
            .domain([nmin[self.nodeSizeBy], nmax[self.nodeSizeBy]]);

        var force = d3.layout.force()
            .charge(-120)
            .linkDistance(30)
            .size([containerWidth, containerHeight])
            .theta([1.2])
            .charge([-450])
            .gravity([1])
            .linkStrength(.5);

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
            .on('mouseover', function(d) {
                // to implement
            })
            .on('mouseout', function(d) {
                // to implement
            });

        var link = linkselection.enter()
            .append("line")
            .attr("class", "link")
            .attr("id", function(d) {
                return d.id;
            })
            .style("stroke", function(d) {
                return linkColor(d);
            })
            .on('mouseover', function(d) {
                // to implement
            })
            .on('mouseout', function(d) {
                // to implement
            });

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
            .on('click', function(d) {
                ctx.notify(self, { type: '_CLICKNODE'}, d);
            })
            .on('dblclick', function(d) {
                // to implement
            })
            .on('mouseover', function(d) {
                // to implement
            })
            .on('mouseout', function(d) {
                // to implement
            });

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
            .call(force.drag)
            .on('click', function(d) {
                ctx.notify(self, { type: '_CLICKNODE'}, d);
            })
            .on('dblclick', function(d) {
                // to implement
            })
            .on('mouseover', function(d) {
                // to implement
            })
            .on('mouseout', function(d) {
                // to implement
            });

        nodeselection.exit().remove();

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
    };

    self.highlightNodes = function(lst) {
        var ids = _.pluck(lst, 'id');
        
        svg.selectAll(".node")
            .style("opacity", 0.2)
            .filter(function(node) {
                return ids.indexOf(node.id) != -1;
            })
            .style("opacity", 1)
            .style("fill", "red");
    }

    self.unhighlightNodes = function() {
        svg.selectAll(".node")
            .style("fill", function(d) {
                return nodeColor(d);
            })
            .style("opacity", 0.8);
    }
};
