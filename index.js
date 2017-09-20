var API_ROOT = "http://10.129.23.41:8080/meta/"
var mqtt = require('mqtt');
var request = require('request');
const Influx = require('influx');
var CHANNELS=[];
const influx = new Influx.InfluxDB({
	host: '10.129.23.161',
	database: 'seil_realtime',
})
function find_channel(id,channel,sensor_type){

	if(id){
		for (var i = CHANNELS.length - 1; i >= 0; i--) {
			if(CHANNELS[i].id==id)
				return CHANNELS[i];
		}
	}
	else{
		for (var i = CHANNELS.length - 1; i >= 0; i--) {
			if(CHANNELS[i].channel==channel && CHANNELS[i].sensor_type ==sensor_type)
				return CHANNELS[i];
		}	
	}
}

function map_insert(topic, payload){
	topic_arr = topic.split('/')
	channel = find_channel(null,topic_arr[0],topic_arr[2])
	payload = payload.split(',')
	var dataset = {};
	if(!channel.fields.error && channel.fields.length == payload.length){
		for (var i = channel.fields.length - 1; i >= 0; i--) {
			switch(channel.fields[i].field_type){
				case 'F': //float
					if(!isNaN(parseFloat(payload[i])))
						dataset[channel.fields[i].field_name] = parseFloat(payload[i]);
				break;

				case 'I': //Integer
					if(!isNaN(parseInt(payload[i])))
						dataset[channel.fields[i].field_name] = parseInt(payload[i]);
				break;

				default: //string
					dataset[channel.fields[i].field_name] = payload[i];
				break;
			}
		}
		influx.writePoints([
			{
				measurement: topic_arr[0]+topic_arr[2]+"_"+channel.id,
				tags: { topic:topic, location: topic_arr[1] },
				fields: dataset,
				timestamp: dataset['TS']*1000000000
			}
		])
		.then(function(){
			console.log("Inserted")
		})
	}

}

function mqtt_init(){
	var client  = mqtt.connect('mqtt://10.129.23.41',{clientId:"mqtt-bridge-influx"})

	client.on('connect', function () {
  		client.subscribe('data/#',{qos:0}) //default qos0
	})
	 
	client.on('message', function (topic, message) {
	  // message is Buffer
	  map_insert(topic,message.toString())
	})
}
request(API_ROOT+'channels/', function (error, response, channels) {
	var count=0;
	if (!error && response.statusCode === 200) {
		CHANNELS = JSON.parse(channels)
		// console.log(channels)
		for (var i = CHANNELS.length - 1; i >= 0; i--) {
			request(API_ROOT+'channel_fields/'+CHANNELS[i].id,function(error, response, fields){
				// console.log(JSON.parse(fields));
				count++;
				channel_id =response.request.uri.path.split('/')[3]
				// console.log(channel_id);
				find_channel(channel_id).fields = JSON.parse(fields);
				if(count==CHANNELS.length){
					mqtt_init()
				}
			})
		}
	}
	else
		console.log(error)
});

