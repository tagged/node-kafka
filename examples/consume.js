/* 
 * A simple demonstration of using the consumer
 * class which allows for subscribing to and 
 * unsubscribing from topics.
 */
var kafka = require('../kafka')
var optimist = require('../../optimist')
    .usage('Consume a topic from the most recent message\nUsage: $0')
    .options('h', {
        alias: 'host',
        default: 'localhost'
    })
    .options('p', {
        alias: 'port',
        default: '9092',
    })
    .options('t', {
        alias: 'topic',
        default: 'testtopic'
    })

var argv = optimist.argv

if (argv.help) {
    optimist.showHelp(console.error)
    process.exit()
}

var consumer = new kafka.Consumer({
	host:argv.host,
	port:argv.port,
    pollInterval: 100,
    reconnectInterval: 1000,
    autoConnectOnWrite: false,
})

var first = true

consumer.on('debug', function(msg) {
    console.log(msg)
})
consumer.on('connecting', function(address) {
    console.error("Connecting to " + address + "...")
})
consumer.on('connected', function(address) {
    console.error('Connected to ' + address)
})
consumer.on('disconnected', function(address) {
    console.error('Disconnected from ' + address)
})
consumer.on('message', function(topic, message, offset) {
	console.log('Consumed message:' + message + ' topic:' + topic + ' offset:' + offset)
})
consumer.on('messageerror', function(topic, partition, error, name) {
	console.error('KAFKA ERROR:' + error + ' topic:' + topic + ' name:' + name)
    consumer.unsubscribeTopic(topic).close().once('connected', function() {
        consumer.fetchOffsets({name: topic, partition: partition, offsets: 1})
    })
})
consumer.on('parseerror', function(topic, partition, msg) {
    console.log('PARSE ERROR -------------', msg)
    consumer.unsubscribeTopic(topic).close().once('connected', function() {
        consumer.fetchOffsets({name: topic, partition: partition, offsets: 1})
    })
})
consumer.on('lastoffset', function(topic, offset) {
    console.error("got last offset for topic: " + topic + " offset: " + offset)
    consumer.subscribeTopic({name: topic, offset: offset})
})

if (argv.topic instanceof Array == false) argv.topic = [argv.topic]
for (i in argv.topic) consumer.fetchOffsets({name: argv.topic[i], offsets: 1})
consumer.connect()
