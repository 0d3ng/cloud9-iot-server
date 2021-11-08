var mqtt = require('mqtt')
var dateFormat = require('dateformat')
var client  = mqtt.connect({ host: 'localhost', port: 1883 })
client.on('connect', async function () {  
	for(i=0; i<200; i++){
		sendingdata("jd2090-eh06");
		await sleep(5000);
		// sendingdata("py787b-mw47");
		// await sleep(1500);
		// sendingdata("py787b-qo06");
		// await sleep(1500);
	}

});
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getRandomArbitrary(min, max) {
  return Math.random() * (max - min) + min;
}

function sendingdata(device_code){
	send = {
		device_code:device_code,
		date_add:dateFormat(new Date(), "yyyy-mm-dd HH:MM:ss"),
		indoor_temperature: Math.floor(getRandomArbitrary(1500,3500)) / 100,
		indoor_humidity: Math.floor(getRandomArbitrary(4000,10000)) / 100,
		indoor_di:1,
		ac_state:1,
		outdoor_temperature:10,
		outdoor_humidity:85.4,
		outdoor_di:0,
	}
	client.publish('message/sensor/jd2090',JSON.stringify(send));
  	console.log(send);
}

