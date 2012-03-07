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
    .options('m', {
        alias: 'message',
        default: 'hello world'
    })
    .options('r', {
        alias: 'repeat-interval',
        default: '0'
    })
var argv = optimist.argv

if (argv.help) {
    optimist.showHelp(console.error)
    process.exit()
}

var producer = new kafka.Producer({ 
    host: argv.host,
    port: argv.port })

var f = function() {
  console.error("Sending to topic: " + argv.topic + " msg: " + argv.message)
  producer.send(argv.topic, argv.message)
}

producer.on('connecting', function(address) {
    console.error("Connecting to " + address + "...")
})
producer.on('connected', function(address) {
    console.error('Connected to ' + address)
    if (argv.r > 0) setInterval(f, argv.r)
    f()
})
producer.on('disconnected', function(address) {
    console.error('Disconnected from ' + address)
})
producer.connect()

