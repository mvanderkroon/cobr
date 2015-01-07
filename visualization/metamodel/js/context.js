var Context = function(config) {
    var self = this;

    var listeners = [];

    var alllinks = [];
    var allnodes = [];

    var selectedlinks = [];
    var selectednodes = [];

    var highlightedlinks = [];
    var highlightednodes = [];


    

    Context.prototype.register = function(listener) {
    	listeners.push(listener);
    }

    Context.prototype.nodes = function(nodes) {
    	if (nodes === undefined) return allnodes;
    	else allnodes = nodes;
    }

    Context.prototype.links = function(links) {
    	if (links === undefined) return alllinks;
    	else alllinks = links;
    }

    Context.prototype.selectedNodes = function() {
    	return selectednodes;
    }

    Context.prototype.selectedLinks = function() {
    	return selectedlinks;
    }





    var seenNodes = {};
    Context.prototype.keyUp = function(e) {
    	var keyCode = e.keyCode || e.which;

    	if (keyCode == 27) { // escape
    		selectedlinks = [];
    		selectednodes = [];
    		seenNodes = {};
        } else if (keyCode == 69) { // 'e' for expand selection
	        var outlinkdict = {};
	    	var inlinkdict = {};

        	_.each(self.links(), function(link) {
        		var srcKey = link.source.db_catalog + "" + link.source.db_schema + "" + link.source.tablename;
            	var tarKey = link.target.db_catalog + "" + link.target.db_schema + "" + link.target.tablename;

            	if (srcKey in outlinkdict) outlinkdict[srcKey].push(link);
            	else outlinkdict[srcKey] = [link];

				if (tarKey in inlinkdict) inlinkdict[tarKey].push(link);
            	else inlinkdict[tarKey] = [link];            	
        	});
        	
        	_.each(selectednodes, function(snode) {
        		var nkey = snode.db_catalog + "" + snode.db_schema + "" + snode.tablename;
        		
        		var outlinks = outlinkdict[nkey];

        		_.each(outlinks, function(olink) {
        			var tkey = olink.target.db_catalog + "" + olink.target.db_schema + "" + olink.target.tablename;
        			if (!(tkey in seenNodes)) {
        				selectednodes.push(olink.target)
        				seenNodes[tkey] = true;

        				_.each(outlinkdict[tkey], function(nlink) {
        					selectedlinks.push(nlink);
        				});
        			}
        		});
        	});
        	
        }

        // emit event
    	for (var i in listeners) {
			listeners[i].handleKeyUp(keyCode);
		}
    }

    Context.prototype.nodeClicked = function(d) {
    	selectednodes.push(d);

    	// determine associated links
    	var nkey = d.db_catalog + "" + d.db_schema + "" + d.tablename;
    	_.each(links, function(link) {

            var lsrcKey = link.source.db_catalog + "" + link.source.db_schema + "" + link.source.tablename;
            var ltarKey = link.target.db_catalog + "" + link.target.db_schema + "" + link.target.tablename;

            if (nkey === lsrcKey || nkey === ltarKey) selectedlinks.push(link);
        });

    	// emit event
    	for (var i in listeners) {
			listeners[i].handleClickNode(d);
		}
    }

    Context.prototype.linkClicked = function(d) {
    	selectedlinks.push(d);

    	for (var i in listeners) {
			listeners[i].handleClickLink(d);
		}
    }

    Context.prototype.nodeHovered = function(d) {
    
    }

    Context.prototype.linkHovered = function(d) {
    
    }
}