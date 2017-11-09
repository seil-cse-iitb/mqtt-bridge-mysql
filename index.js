process.env.TZ = 'Asia/Kolkata' 
var API_ROOT = "http://10.129.23.41:8080/meta/"
var mqtt = require('mqtt');
var request = require('request');
const config = require('./config')
var CHANNELS=[];
const Sequelize = require('sequelize');
const sequelize = new Sequelize(config.database.mysql.name, config.database.mysql.username, config.database.mysql.password, {
  host: config.database.mysql.host,
  dialect: 'mysql',
  pool: {
    max: 5,
    min: 0,
    acquire: 30000,
    idle: 10000
  },
  // http://docs.sequelizejs.com/manual/tutorial/querying.html#operators
  operatorsAliases: false
});
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
			// if(channel.fields[i].field_name === 'TS'){
			// 	dataset["TS"] = new Date(parseFloat(payload[i]) * 1000);
			// 	continue;
			// }
			if(channel.fields[i].field_name === 'id'){
				dataset['sensor_id'] = parseInt(payload[i]);
				continue;
			}
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
		dataset['location'] = topic_arr[1];
		dataset['mac_id'] = topic_arr[3];
		channel.model.create(dataset);
		// influx.writePoints([
		// 	{
		// 		measurement: topic_arr[0]+topic_arr[2]+"_"+channel.id,
		// 		tags: { topic:topic, location: topic_arr[1], sensor_id: topic_arr[3] },
		// 		fields: dataset,
		// 		timestamp: dataset['TS']*1000000000
		// 	}
		// ])
		// .then(function(){
		// 	//console.log("Inserted")
		// })
	}
	else
		console.log("API length mismatch")

}

function mqtt_init(){
	var client  = mqtt.connect(config.mqtt.url,{clientId:config.mqtt.clientId})

	client.on('connect', function () {
  		client.subscribe('data/#',{qos:0}) //default qos0
		// client.subscribe('nodemcu/#',{qos:0}) //default qos0
	})
	 
	client.on('message', function (topic, message) {
	  // message is Buffer
	  map_insert(topic,message.toString())
	})
}
function prepare_sequelize_models(){

	for( channel_idx in CHANNELS){
		var model={
			mac_id : Sequelize.INTEGER,
			location : Sequelize.STRING
		};
		if(CHANNELS[channel_idx].fields.error){
			continue;
		}

		for (var i = CHANNELS[channel_idx].fields.length - 1; i >= 0; i--) {
			// if(CHANNELS[channel_idx].fields[i].field_name === 'TS'){
			// 	model["TS"] = Sequelize.DATE;
			// 	continue;
			// }
			if(CHANNELS[channel_idx].fields[i].field_name === 'id'){
				model['sensor_id'] = Sequelize.INTEGER;
				continue;
			}
			switch(CHANNELS[channel_idx].fields[i].field_type){
				case 'F': //float
						model[CHANNELS[channel_idx].fields[i].field_name] = Sequelize.DOUBLE;
				break;

				case 'I': //Integer
						model[CHANNELS[channel_idx].fields[i].field_name] = Sequelize.INTEGER;
				break;

				default: //string
					model[CHANNELS[channel_idx].fields[i].field_name] = Sequelize.STRING;
				break;
			}
		}

		CHANNELS[channel_idx].model = sequelize.define(CHANNELS[channel_idx].sensor_type + "_" + CHANNELS[channel_idx].id, model,{
			freezeTableName: true,
			tableName: CHANNELS[channel_idx].sensor_type + "_" + CHANNELS[channel_idx].id
		});
		CHANNELS[channel_idx].model.removeAttribute('id');
	}
	sequelize.sync()
	  .then(() => mqtt_init());
}
request(API_ROOT+'channels/', function (error, response, channels) {
	var count=0;
	if (!error && response.statusCode === 200) {
		CHANNELS = JSON.parse(channels)
		// console.log(channels)

		// remove non-data channels
		for (var i = CHANNELS.length - 1; i >= 0; i--) {
			if(CHANNELS[i].channel !== 'data'){
				CHANNELS.splice(i,1);
				continue;
			}
		}
		//get fields for each channel
		for (var i = CHANNELS.length - 1; i >= 0; i--) {
			request(API_ROOT+'channel_fields/'+CHANNELS[i].id,function(error, response, fields){
				// console.log(JSON.parse(fields));
				count++;
				channel_id =response.request.uri.path.split('/')[3]
				// console.log(channel_id);
				find_channel(channel_id).fields = JSON.parse(fields);
				if(count==CHANNELS.length){
					// mqtt_init()
					// console.log(CHANNELS);
					prepare_sequelize_models();
				}
			})
		}
	}
	else
		console.log(error)
});

