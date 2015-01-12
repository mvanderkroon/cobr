var Watcher = function(origin, destination, event, react) {
    var self = this;

    self._origin = origin;
    self._destination = destination;
    self._event = event;
    self._react = react;

    self.origin = function(q) {
        if (q === undefined || q === null) return true;

        return self._origin(q);
    };

    self.destination = function(q) {
        if (q === undefined || q === null) return true;

        return self._destination(q);
    };

    self.event = function(q) {
        if (q === undefined || q === null) return true;

        return self._event(q);
    };

    self.react = function(ctx, listener, payload) {
        return self._react(ctx, listener, payload);
    };
};
