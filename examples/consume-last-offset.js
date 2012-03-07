/*
 * This example demonstrates using the Basic Consumer
 *
 * The Basic Consumer is an advanced API - if you are
 * looking for something simple and efficient use 
 * the Consumer API instead.  
 *
 * The use of Basic Consumer shown in this example 
 * would be very inefficient since it polls kafka 
 * as fast as it possibly can.  You should not
 * do this.  The example is written only to 
 * demonstrate how to use the Basic Consumer API
 *
 * A real world implementation using this class 
 * should implement a more efficient strategy
 * and backoff when there are no messages to consume
 *
 * See Consumer.js for a more realistic use case
 */

 // Do this very early so all exceptions can be caught
 process.on('uncaughtException', function (err) {
     // if (nodeServer) {
         //   nodeServer.sendFailureCount++;
             // }
                 console.log('ERROR: Caught exception: ' + err);
                     console.log(err.stack.split('\n'));
                     });

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

var client = new kafka.Client({
    host:argv.host,
    port:argv.port,
})

client.on('connecting', function(address) {
    console.error("Connecting to " + address)
})
client.on('connected', function(address) {
    console.error("Connected to " + address)
})
client.on('disconnected', function(address) {
    console.error("Disconnected from " + address)
})
client.on('connection_error', function(address, error) {
    console.error("Couldn't connect to " + address + " (" + error + ")")
})
client.on('error', function(address, err) {
    console.error("Couldn't connect to " + address)
})
client.on('message', function(topic, message, offset) {
    console.log("Consumed topic:" + topic + " message:" + message + " offset: " + offset)
})
client.on('lastmessage', function(topic, offset) {
    setTimeout(function() { client.fetchTopic({name: topic, offset: offset})}, 100)
})
client.on('offset', function(topic, offset) {
    console.error("offset: " + topic + " offset: " + offset)
})
client.on('lastoffset', function(topic, offset) {
    console.error("last offset: " + topic + " offset: " + offset)
    client.fetchTopic({name: topic, offset: offset})
})
client.on('connected', function() {
    console.error('Fetching topic ', argv.topic)
    client.fetchOffsets({name: argv.topic, offsets: 1})
})
client.connect()
