var std = require('std'),
    events = require('events'),
    Client = require('./Client'),
    error = require('./error')

module.exports = std.Class(Client, function(supr) {

    var defaults = {
        reconnectInterval: 1000,
    }

    this._init = function(opts) {
        supr(this, '_init', arguments)
        opts = std.extend(opts, defaults)
        this._reconnectInterval = opts.reconnectInterval
        this.on('closed', std.bind(this, '_retry'))
    }

    this.getStats = function() {
        return "producer "
               + supr(this, "getStats", arguments)
               + ", msgs_requested: " + this._msgs_requested
               + ", msgs_sent: " + this._msgs_sent
               + ", msgs_dropped: " + this._msgs_dropped
               + ", total_processed: " + (this._msgs_dropped + this._msgs_sent)
    }

    this.disconnect = function() {
        supr(this, 'disconnect', arguments)
        this._reconnectInterval = -1
    }

    this._retry = function(address) {
        if (this._reconnectInterval < 0) return
        setTimeout(std.bind(this, '_reconnect'), this._reconnectInterval)
    }

    this._reconnect = function() {
        if (!this.connected() && !this.connecting()) this.connect()
    }
})
