function force(config, context) {
    var self = this;
    context.register(self);

    var containerHeight = ($(config.parent).height() == 0) ? $(window).height() : $(config.parent).height();
    var containerWidth = ($(config.parent).width() == 0) ? $(window).width() : $(config.parent).width();

    var mouseoverflag = false;

    var svg = d3.select(config.parent).append("svg")
        .attr("width", containerWidth)
        .attr("height", containerHeight);

    var node, 
        link, 
        nodes = [], 
        links = [];

    var getNodeList = function(root) {
        var nodes = [],
            i = 0;

        function recurse(node) {
            nodes.push(node);
            if (node.children) node.children.forEach(recurse);
        }

        recurse(root);
        return nodes;
    }

    var partition = d3.layout.partition()
        .value(function(d) {
            return d.size
        });     

    self.render = function(data) {
        link = svg.selectAll(".link");
        node = svg.selectAll(".node");

        nodes = getNodeList(partition(data)[0]);
        links = d3.layout.tree().links(nodes);

        var force = d3.layout.force()
            .nodes(nodes)
            .links(links)
            .linkDistance(function(d) {
                // return 40;
                return 0.1;
                return d.target.value / 50;
                // return [d.target.weight - d.source.weight] * 1;
            })
            .theta([1.5])
            // .charge(-100)
            .gravity(1)
            .linkStrength(7.5)
            .size([containerWidth, containerHeight])
            .on("tick", tick)
            .start();

        var linkwidthscale =  d3.scale.sqrt().domain([1, nodes[0].value]).range([1, 30]);

        // Update the links…
        link = link.data(links, function(d) {
            return d.source.id + "-" + d.target.id;
        });

        // Exit any old links.
        link.exit().remove();

        // Enter any new links.
        link.enter()
            .insert("line", ".node")
        .attr("class", "link")
            .attr("x1", function(d) {
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
            })
            .style("stroke-width", function(d) {
                return linkwidthscale(d.target.value);
            });

        // Update the nodes…
        node = node.data(nodes, function(d) {
            return d.id;
        })
            .style("fill", function(d) {
                return context.getNodeColor(d);
            })
            .style("opacity", config.basecfg.default_opacity);

        // Exit any old nodes.
        node.exit().remove();

        // Enter any new nodes.
        node.enter()
            .append("circle")
        // .attr("display", function(d) {
        //  if (d.depth > 4) return "none";
        // })
        .attr("class", "node")
            .attr("id", function(d) {
                return d.id;
            })
            .attr("cx", function(d) {
                return d.x;
            })
            .attr("cy", function(d) {
                return d.y;
            })
            .attr("r", function(d) {
                return Math.sqrt(d.value);
            })
            .style("fill", function(d) {
                return context.getNodeColor(d);
            })
            .style("opacity", config.basecfg.default_opacity)
            .on("dblclick", function(d) {
                context.dblclickNode(d);
            })
            .on("mouseover", function(d) {
                context.mouseoverNode(d);
            })
            .on("mouseleave", function(d) {
                context.mouseleaveNode(d);
            })
            .on("click", function(d) {
                context.clickNode(d);
            })
            .call(force.drag());

    }

    function tick() {
        // nodes[0].x = containerWidth / 2;
        // nodes[0].y = containerHeight / 2;

        link
            .attr("x1", function(d) {
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

        node
            .attr("cx", function(d) {
                return d.x;
            })
            .attr("cy", function(d) {
                return d.y;
            });
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
        //     .duration(500)
        //     .style("opacity", 0);
    }

    self.handleClickNode = function(d) {
        // self.render(context.getTree(function(d) {
        //     return d.depth < 5;
        // }));
    }

    var view_root;
    self.handleDblclickNode = function(d) { 
        if (d.parent && view_root && view_root.id == d.id) { // user clicked root / we are going to be drilling up 
            view_root = d.parent;
            self.render(d.parent);
        } else { // drilling down
            view_root = d;
            self.render(d);
        }
        
        // find root
        // var current = d;
        // do {
        //     if (current.parent) current = current.parent;
        // } while (current.parent);

        // self.render(current);

        // d.parent.parent = null;
        // delete d.parent.parent;
            
        // self.render(d);
        // if (!d3.event.defaultPrevented) {
        //     if (d.children) {
        //         d._children = d.children;
        //         d.children = null;
        //     } else {
        //         d.children = d._children;
        //         d._children = null;
        //     }
        //     self.render();
        // }
    }

    var default_cfg = {
        parent: '#force',
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
    self.render(context.getTree());
    
    return self.render;
}