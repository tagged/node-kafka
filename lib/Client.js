// Implements a low-level kafka client which can do the following:
//  - request messages for a topic
//  - get offsets for a topic
//  - send messages to a topic
//
// TODO
//  - queueing of requests when client is down (max sized FIFO)
//  - intelligent collapse (batch) of similar events (e.g. send)
var std = require('std'),
    events = require('events'),
    bigint = require('bigint'),
    Connection = require('./Connection'),
    requestTypes = require('./requestTypes'),
    error = require('./error')

module.exports = std.Class(Connection, function(supr) {
    var states = {
        ERROR: 0,
        HEADER_LEN_0: 1, HEADER_LEN_1: 2, HEADER_LEN_2: 3, HEADER_LEN_3: 4,
        HEADER_EC_0: 5, HEADER_EC_1: 6,
        RESPONSE_MSG_0: 7, RESPONSE_MSG_1: 8, RESPONSE_MSG_2: 9, RESPONSE_MSG_3: 10,
        RESPONSE_MAGIC: 11,
        RESPONSE_COMPRESSION: 12,
        RESPONSE_CHKSUM_0: 13, RESPONSE_CHKSUM_1: 14, RESPONSE_CHKSUM_2: 15, RESPONSE_CHKSUM_3: 16,
        RESPONSE_MSG: 17,

        OFFSET_LEN_0: 18, OFFSET_LEN_1: 19, OFFSET_LEN_2: 20, OFFSET_LEN_3: 21,
        OFFSET_OFFSETS_0: 22, OFFSET_OFFSETS_1: 23, OFFSET_OFFSETS_2: 24, OFFSET_OFFSETS_3: 25,
        OFFSET_OFFSETS_4: 26, OFFSET_OFFSETS_5: 27, OFFSET_OFFSETS_6: 28, OFFSET_OFFSETS_7: 29,
    }

    var LATEST_TIME = -1
    var EARLIEST_TIME = -1
    var MAGIC_VALUE = 0

    var defaults = {
        maxSize: 1048576, //1MB
        autoConnectOnWrite: true,
        sendQueueLength: 10000,
    }

    var fetch_defaults = {
        type: requestTypes.FETCH,
        next: states.RESPONSE_MSG_0,
        last: 'lastmessage',
        encode: function (t) {
            return this._encodeFetchRequest(t)
        },

        partition: 0,
        offset: 0,
    }

    var offset_defaults = {
        type: requestTypes.OFFSETS,
        next: states.OFFSET_LEN_0,
        last: 'lastoffset',
        encode: function (t) {
            return this._encodeOffsetsRequest(t)
        },

        partition: 0,
        offsets: 1,
    }

    var send_defaults = {
        type: requestTypes.PRODUCE,
        encode: function (t) {
            return this._encodeSendRequest(t)
        },

        partition: 0,
    }

    this._init = function(opts) {
        supr(this, '_init', arguments)
        opts = std.extend(opts, defaults)
        this._buffer = new Buffer(opts.maxSize)
        fetch_defaults.encode = fetch_defaults.encode.bind(this)
        offset_defaults.encode = offset_defaults.encode.bind(this)
        send_defaults.encode = send_defaults.encode.bind(this)

        this._autoConnectOnWrite = opts.autoConnectOnWrite
        this._sendQueueLength = opts.sendQueueLength

        this.on('connected', std.bind(this, '_connected'))

        this._msgs_requested = 0
        this._msgs_sent = 0
        this._msgs_dropped = 0

        this._reset()
        this.clearRequests()
    }

    this.getStats = function() {
        return "host: " + this._host + ":" + this._port
             + ", socket.bufferSize: " + (this.connected() ? this._connection.bufferSize : "n/a")
    }

    this.getMaxFetchSize = function() {
        return defaults.maxSize;
    }

    this._reset = function() {
        this._toRead = 0
        this._state = states.HEADER_LEN_0
    }
    
    this.clearRequests = function() {
        this._requests = []        
        this._sendRequests = []
    }

    this.fetchTopic = function(args) {
        var request = std.extend({}, std.extend(args.name == undefined ? { name: args } : args, fetch_defaults))
        request.original_offset = bigint(request.offset)
        request.bytesRead = 0
        this._pushRequest({request: request})
        return this
    }

    this.fetchOffsets = function(args) {
        var request = std.extend({}, std.extend(args.name == undefined ? { name: args } : args, offset_defaults))
        this._pushRequest({request: request})
        return this
    }

    this.send = function(args, messages, callback) {
        var request = std.extend({}, std.extend(messages == undefined ? args : { topic: args, messages: messages}, send_defaults))
        if (!(request.messages instanceof Array)) request.messages = [request.messages]
        var cb = function() {
            this._msgs_sent++
            callback && callback()
        }.bind(this)
        this._msgs_requested++
        this._pushSendRequest({request: request, callback: cb})
        return this
    }

    this._connected = function() {
        this._reset()
        
        // handle data from the connection
        this._connection.on('data', std.bind(this, '_onData'))
        
        // send queued send requests
        // make a copy because socket writes may return immediately and modify the size
        var r = this._sendRequests.slice(0);
        for (var i=0; i<r.length; i++) this._writeRequest(r[i])
        // send queued read requests
        for (var i=0; i<this._requests.length; i++) this._writeRequest(this._requests[i])
    }

    this._pushSendRequest = function(requestObj) {
        var cb = requestObj.callback;
        requestObj.callback = function() {
            this._sendRequests.shift()
            cb && cb()
        }.bind(this)
        // drop entries if too long
        if (this._sendRequests.length >= this._sendQueueLength) {
            this._msgs_dropped++
            this._sendRequests.shift()
        }
        this._sendRequests.push(requestObj)
        this._writeRequest(requestObj)
    }

    this._pushRequest = function(requestObj) {
        this._requests.push(requestObj)
        this._writeRequest(requestObj)
    }

    this._writeRequest = function(requestObj) {
        if (this._autoConnectOnWrite && !this.connected() && !this.connecting()) {
            this.connect()
            return
        }
        if (!this.connected()) {
            return
        }
	    if (!this._connection.writable) {
	        this.close();
	    } else {
            this._connection.write(requestObj.request.encode(requestObj.request), 'utf8', requestObj.callback)
	    }
    }

    this._encodeFetchRequest = function(t) {
        var offset = bigint(t.offset)
        var request = std.pack('n', t.type)
            + std.pack('n', t.name.length)
            + t.name
            + std.pack('N', t.partition)
            + std.pack('N', offset.shiftRight(32).and(0xffffffff))
            + std.pack('N', offset.and(0xffffffff))
            + std.pack('N', (t.maxSize == undefined)? this._buffer.length : t.maxSize)

        var requestSize = 2 + 2 + t.name.length + 4 + 8 + 4

        return this._bufferPacket(std.pack('N', requestSize) + request)
    }

    this._encodeOffsetsRequest = function(t) {
        var request = std.pack('n', t.type)
            + std.pack('n', t.name.length) + t.name
            + std.pack('N', t.partition)
            + std.pack('N2', -1 , -1)
            + std.pack('N', t.offsets)

        var requestSize = 2 + 2 + t.name.length + 4 + 8 + 4
        return this._bufferPacket(std.pack('N', requestSize) + request)
    }

    this._encodeSendRequest = function(t) {
        var encodedMessages = ''
        for (var i = 0; i < t.messages.length; i++) {
            var encodedMessage = this._encodeMessage(t.messages[i])
            encodedMessages += std.pack('N', encodedMessage.length) + encodedMessage
        }

        var request = std.pack('n', t.type)
            + std.pack('n', t.topic.length) + t.topic
            + std.pack('N', t.partition)
            + std.pack('N', encodedMessages.length) + encodedMessages

        return this._bufferPacket(std.pack('N', request.length) + request)
    }

    this._encodeMessage = function(message) {
        return std.pack('CN', MAGIC_VALUE, std.crc32(message)) + message
    }

    this._onData = function(buf) {
        if (this._requests[0] == undefined) return
        var index = 0        
        while (index != buf.length) {
            var bytes = 1
            var next = this._state + 1
            switch (this._state) {
                case states.ERROR:
                    // just eat the bytes until done
                    next = states.ERROR
                    break
                    
                case states.HEADER_LEN_0:
                    this._totalLen = buf[index] << 24
                    break

                case states.HEADER_LEN_1:
                    this._totalLen += buf[index] << 16
                    break

                case states.HEADER_LEN_2:
                    this._totalLen += buf[index] << 8
                    break

                case states.HEADER_LEN_3:
                    this._totalLen += buf[index]
                    break

                case states.HEADER_EC_0:
                    this._error = buf[index] << 8
                    this._totalLen--
                    break

                case states.HEADER_EC_1:
                    this._error += buf[index]
                    this._toRead = this._totalLen
                    next = this._requests[0].request.next
                    this._totalLen--
                    if (this._error != error.NoError) this.emit('messageerror', 
                                                                this._requests[0].request.name, 
                                                                this._requests[0].request.partition, 
                                                                this._error, 
                                                                error[this._error])
                    break

                case states.RESPONSE_MSG_0:
                    this._msgLen = buf[index] << 24
                    this._requests[0].request.last_offset = bigint(this._requests[0].request.offset)
                    this._requests[0].request.offset++
                    this._payloadLen = 0
                    break

                case states.RESPONSE_MSG_1:
                    this._msgLen += buf[index] << 16
                    this._requests[0].request.offset++
                    break

                case states.RESPONSE_MSG_2:
                    this._msgLen += buf[index] << 8
                    this._requests[0].request.offset++
                    break

                case states.RESPONSE_MSG_3:
                    this._msgLen += buf[index]
                    this._requests[0].request.offset++
                    if (this._msgLen > this._totalLen) {
                        console.log(buf)
                        this.emit("parseerror", 
                                  this._requests[0].request.name,
                                  this._requests[0].request.partition,
                                  "unexpected message len " + this._msgLen + " > " + this._totalLen 
                                  + " for topic: " + this._requests[0].request.name 
                                  + ", partition: " + this._requests[0].request.partition
                                  + ", original_offset:" + this._requests[0].request.original_offset
                                  + ", last_offset: " + this._requests[0].request.last_offset)
                       this._error = error.InvalidMessage
                       next = states.ERROR
                    }
                    break

                case states.RESPONSE_MAGIC:
                    this._magic = buf[index]
                    this._requests[0].request.offset++
                    this._msgLen--
                    if (false && Math.random()*20 > 18) this._magic = 5
                    switch (this._magic) {
                        case 0:
                          next = states.RESPONSE_CHKSUM_0
                          break
                        case 1:
                          next = states.RESPONSE_COMPRESSION
                          break
                        default:
                          this.emit("parseerror", 
                                    this._requests[0].request.name,
                                    this._requests[0].request.partition,
                                    "unexpected message format - bad magic value " + this._magic                                     
                                    + " for topic: " + this._requests[0].request.name 
                                    + ", partition: " + this._requests[0].request.partition
                                    + ", original_offset:" + this._requests[0].request.original_offset
                                    + ", last_offset: " + this._requests[0].request.last_offset)
                          this._error = error.InvalidMessage
                          next = states.ERROR
                    }
                    break

                case states.RESPONSE_COMPRESSION:
                    this._msgLen--
                    this._requests[0].request.offset++
                    if (buf[index] > 0) {
                        console.log(buf)
                        this.emit("parseerror",
                                  this._requests[0].request.name,
                                  this._requests[0].request.partition,
                                  "unexpected message format - bad compression flag " 
                                  + " for topic: " + this._requests[0].request.name 
                                  + ", partition: " + this._requests[0].request.partition
                                  + ", original_offset:" + this._requests[0].request.original_offset
                                  + ", last_offset: " + this._requests[0].request.last_offset)
                        this._error = error.InvalidMessage
                        next = states.ERROR
                    }
                    break
                
                case states.RESPONSE_CHKSUM_0:
                    this._chksum = buf[index] << 24
                    this._requests[0].request.offset++
                    this._msgLen--
                    break

                case states.RESPONSE_CHKSUM_1:
                    this._chksum += buf[index] << 16
                    this._requests[0].request.offset++
                    this._msgLen--
                    break

                case states.RESPONSE_CHKSUM_2:
                    this._chksum += buf[index] << 8
                    this._requests[0].request.offset++
                    this._msgLen--
                    break

                case states.RESPONSE_CHKSUM_3:
                    this._chksum += buf[index]
                    this._requests[0].request.offset++
                    this._msgLen--
                    break

                case states.RESPONSE_MSG:
                    next = states.RESPONSE_MSG

                    // try to avoid a memcpy if possible
                    var payload = null
                    if (this._payloadLen == 0 && buf.length - index >= this._msgLen) {
                        payload = buf.toString('utf8', index, index + this._msgLen)
                        bytes = this._msgLen
                    } else {
                        var end = index + this._msgLen - this._payloadLen
                        if (end > buf.length) end = buf.length
                        buf.copy(this._buffer, this._payloadLen, index, end)
                        this._payloadLen += end - index
                        bytes = end - index
                        if (this._payloadLen == this._msgLen) {
                            payload = this._buffer.toString('utf8', 0, this._payloadLen)
                        }
                    }
                    if (payload != null) {
                        this._requests[0].request.offset += this._msgLen
                        next = states.RESPONSE_MSG_0
                        this.emit('message', this._requests[0].request.name, payload, bigint(this._requests[0].request.offset))
                    }
                    break

                case states.OFFSET_LEN_0:
                    this._msgLen = buf[index] << 24
                    break

                case states.OFFSET_LEN_1:
                    this._msgLen += buf[index] << 16
                    break

                case states.OFFSET_LEN_2:
                    this._msgLen += buf[index] << 8
                    break

                case states.OFFSET_LEN_3:
                    this._msgLen += buf[index]
                    break

                case states.OFFSET_OFFSETS_0:
                    this._requests[0].request.offset_buffer = new Buffer(8)
                    this._requests[0].request.offset_buffer[0] = buf[index]
                    break

                case states.OFFSET_OFFSETS_1:
                    this._requests[0].request.offset_buffer[1] = buf[index]
                    break

                case states.OFFSET_OFFSETS_2:
                    this._requests[0].request.offset_buffer[2] = buf[index]
                    break

                case states.OFFSET_OFFSETS_3:
                    this._requests[0].request.offset_buffer[3] = buf[index]
                    break

                case states.OFFSET_OFFSETS_4:
                    this._requests[0].request.offset_buffer[4] = buf[index]
                    break

                case states.OFFSET_OFFSETS_5:
                    this._requests[0].request.offset_buffer[5] = buf[index]
                    break

                case states.OFFSET_OFFSETS_6:
                    this._requests[0].request.offset_buffer[6] = buf[index]
                    break

                case states.OFFSET_OFFSETS_7:
                    this._requests[0].request.offset_buffer[7] = buf[index]
                    this._requests[0].request.offset = bigint.fromBuffer(this._requests[0].request.offset_buffer)
                    next = states.OFFSET_OFFSETS_0
                    this.emit('offset', this._requests[0].request.name, bigint(this._requests[0].request.offset))
            }
            if (this._requests[0] == undefined) break
            this._requests[0].request.bytesRead += bytes
            index += bytes
            this._toRead -= bytes
            this._state = next
            if (this._toRead == 0) this._last()
        }
    }

    this._last = function() {
        var last = this._requests.shift()

        // we don't know if we got all the messages if we got a buffer full of data
        // so re-request the topic at the last parsed offset, otherwise, emit last
        // message to tell client we are done
        if (last.request.bytesRead >= this._buffer.length) {
            // when we request data from kafka, it just sends us a buffer from disk, limited
            // by the maximum amount of data we asked for (plus a few more for the header len)
            // the end of this data may or may not end on an actual message boundary, and we
            // may have processed an offset header, but not the actual message
            // because the state machine automatically sets the request offset to the offset
            // that is read, we have to detect here if we stopped before the message
            // boundary (the message state will be other than the start of a new mesage).
            //
            // If we did, reset the offset to the last known offset which is
            // saved before processing the offset bytes.
            if (this._state != states.RESPONSE_MSG_0) {
                last.request.offset = bigint(last.request.last_offset)
            }

            this.fetchTopic(last.request)
        } else {
            this.emit(last.request.last, last.request.name, bigint(last.request.offset), this._error, error[this._error])
        }
        this._state = states.HEADER_LEN_0
    }
})
