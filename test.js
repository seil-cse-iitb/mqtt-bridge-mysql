var mqtt = require('mqtt');

var client  = mqtt.connect('mqtt://10.129.23.41',{clientId:"mqtt-bridge-influx-test"})

client.on('connect', function () {

	client.subscribe('nodemcu/#',{qos:0}) //default qos0
})
 
client.on('message', function (topic, message) {
  // message is Buffer
	console.log(topic)
	console.log(message.toString())

})
