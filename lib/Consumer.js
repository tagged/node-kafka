var std = require('std'),
    events = require('events'),
    Client = require('./Client'),
    error = require('./error')

module.exports = std.Class(Client, function(supr) {

    var defaults = {
        reconnectInterval: 1000,
        pollInterval: 2000,
    }

    var subscription_defaults = {
        offset: 0,
        partition: 0,
    }

    this._init = function(opts) {
        supr(this, '_init', arguments)
        opts = std.extend(opts, defaults)
        this._pollInterval = opts.pollInterval
        this._reconnectInterval = opts.reconnectInterval
        this._topics = []
        this._outstanding = 0
        this._timerTicked = true
        
        this.on('message', std.bind(this, '_processMessage'))
        this.on('lastmessage', std.bind(this, '_processLast'))
        this.on('closed', std.bind(this, '_closed'))

        this._timeoutID = setTimeout(std.bind(this, '_tick'), this._pollInterval)
    }

    this.topics = function() { return this._topics.length }

    this.subscribeTopic = function(opts) {
        var topic = opts.name == undefined ? { name:opts, offset:0, partition: 0 } : std.extend(opts, subscription_defaults)
        this._topics.push(topic)
        if (this._topics.length == 1) this._pollForMessages()
        return this
    }

    this.unsubscribeTopic = function(name) {
        for (var i=0; i<this._topics.length; i++) if (this._topics[i].name == name) {
            this._topics.splice(i, 1)
            break
        }
        return this
    }

    this.getStats = function() {
        return "consumer "
               + supr(this, "getStats", arguments)
               + ", topics: " + this.topics()
    }
        
    this._closed = function(address) {
        if (this._reconnectInterval < 0) return
        
        this._outstanding = 0
        this._timerTicked = false
        this.clearRequests()
        this.emit('debug', "_closed is setting up timer for reconnect")     
        setTimeout(std.bind(this, '_reconnect'), this._reconnectInterval)
    }

    this._reconnect = function() {
        if (!this.connected() && !this.connecting()) {
            this.emit('debug', "_reconnect is calling connect")
            this.connect()
        }
    }

    this.disconnect = function() {
        supr(this, 'disconnect', arguments)
        this._reconnectInterval = -1
    }
    
    this._pollForMessages = function() {
        if (this._outstanding > 0 || !this._timerTicked) return
        
        this._timerTicked = false
        for (var i=0; i<this._topics.length; i++) {
            this._outstanding++
            this.fetchTopic(this._topics[i])
        }
    }

    this._processMessage = function(topic, message, offset) {
        for (var i=0; i<this._topics.length; i++) if (this._topics[i].name == topic) {
            this.emit('debug', "_processMessage is setting new offset for topic:" + topic + " offset: " + offset)
            this._topics[i].offset = offset
            break
        }        
    }
    
    this._processLast = function(topic, offset, errno, error) {
        if (false && Math.random()*100 > 90) {
            for (var i=0; i<this._topics.length; i++) if (this._topics[i].name == topic) {
                console.log("INJECTING ERRONEOUS OFFSET TO TOPIC: " + topic + " OFFSET: " + (offset-1000))
                this._topics[i].offset = offset - 1000
                break
            }        
        }
        this._outstanding--
        this._pollForMessages()            
    }

    this._tick = function() {
        this._timeoutID = setTimeout(std.bind(this, '_tick'), this._pollInterval)
        this._timerTicked = true
        this._pollForMessages()
    }
})