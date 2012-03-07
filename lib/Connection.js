var std = require('std'),
    net = require('net'),
    events = require('events')

module.exports = std.Class(events.EventEmitter, function() {

    var defaults = {
        host: 'localhost',
        port: 9092
    }

    this._init = function(opts) {
        opts = std.extend(opts, defaults)
        this._host = opts.host
        this._port = opts.port
    }

    this.connect = function(callback) {
        if (this._connection) {
            // this is really hard to track down if it happens, so printing
            // stack trace to console is a good idea for now
            console.log(new Error().stack)
            throw new Error("connect called twice")
        }
        // the expression here is just to make it easy to inject this error for testing
        // switch true to false and it will inject the error 20% of the time
        if (true || Math.random() > 0.8) {
            this._connection = net.createConnection(this._port, this._host)
        }
        if (!this._connection) {
            this.emit('connection_error', this._address(), "Couldn't create socket")
            this.emit('closed', this._address())
            return this
        }
        this._connection.on('connect', std.bind(this, '_handleSocketConnect'))
        this._connection.on('error', std.bind(this, '_handleSocketError'))
        this._connection.on('end', std.bind(this, '_handleSocketEnd'))
        if (callback != undefined) this._connection.on('connect', callback)
        this.emit("connecting", this._address())
        return this
    }

    this.close = function() {
        if (this.connected()) this._connection.end()
        delete this._connection
        this.emit('closed', this._address())
        return this
    }

    this.disconnect = function() {
        this.close(false)
    }

    this.connecting = function() {
        return this._connection != null && this._connection._connecting;
    }

    this.connected = function() {
        return this._connection != null && !this._connection._connecting
    }

    this._handleSocketEnd = function() {
        this.emit('disconnected',  this._address())
    }

    this._handleSocketError = function(error) {
        this.emit("connection_error", this._address(), error)
        this.close()
    }

    this._handleSocketConnect = function() {
        this._connection.on('close', std.bind(this, 'close'))
        this.emit('connected',  this._address())
    }

    this._address = function() {
        return "kafka://" + this._host + ":" + this._port
    }

    this._bufferPacket = function(packet) {
        var len = packet.length,
            buffer = new Buffer(len)

        for (var i=0; i<len; i++) {
            buffer[i] = packet.charCodeAt(i)
        }

        return buffer
    }
})
