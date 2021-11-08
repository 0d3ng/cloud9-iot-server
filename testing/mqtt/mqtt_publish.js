var mqtt = require('mqtt')
var dateFormat = require('dateformat');
var client  = mqtt.connect({ host: 'localhost', port: 1883 })
client.on('connect', function () {  
	send = {
		device_code:"92868a-vr88",
		date_add:dateFormat(new Date(), "yyyy-mm-dd HH:MM:ss"),
		gyro:{
			x:11.1,
			y:12.4,
			z:15
		},
		proximity:111.5,
		rssi:10.5
	}
	client.publish('message/sensor/92868a',JSON.stringify(send));
  	console.log(send);
})