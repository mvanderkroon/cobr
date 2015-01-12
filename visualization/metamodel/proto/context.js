var Context = function() {
    var self = this;

    self._basedatadict = {};
    self._listeners = [];
    self._watchers = [];

    self.data = function(channel, values) {
        if (channel === undefined) return undefined;

        // get
        if (values === undefined) {
            if (channel in self._basedatadict) return self._basedatadict[channel];
            else return undefined;
        }
        // set
        else {
            self._basedatadict[channel] = values;
        }
    };

    self.subscribe = function(listener) {
        self._listeners.push(listener);
    };

    self.unsubscribe = function(listener) {
        console.error('Not yet implemented');
    };

    self.watch = function(origin, destination, event, react) {
        self._watchers.push(new Watcher(origin, destination, event, react));
    };

    self.notify = function(sender, e, payload) {
        var seen = {};

        _.each(self._listeners, function(listener) {
            _.each(self._watchers, function(watcher) {

                if (watcher.origin(sender) && watcher.destination(listener) && watcher.event(e) && !seen[listener.id()]) {
                    seen[listener.id()] = true;
                    watcher.react(self, listener, payload);
                }
            });
        });
    }
};
