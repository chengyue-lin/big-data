'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4]), encoding: 'latin1'})

function counterToNumber(c) {
	return Number(Buffer.from(c, 'latin1').readBigInt64BE());
}
function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

hclient.table('weather_crime_by_month').row('20011').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


hclient.table('weather_crime_by_month').row('20012').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


app.use(express.static('public'));
app.get('/crime.html',function (req, res) {
    const year=req.query['year'] ;
	const month = req.query['month']
    console.log(year);
	console.log(month)
	hclient.table('weather_crime_by_month').row(year+month).get(function (err, cells) {
		const weatherInfo = rowToMap(cells);
		console.log(weatherInfo)
		function weather_crime(weather) {
			var total = weatherInfo["crime:total_crime" ];
			var delays = weatherInfo["crime:" + weather + "_total"];
			if(delays == 0)
				return " - ";
			return ((delays/total)*100).toFixed(5); /* five decimal place */
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			year : req.query['year'],
			month : req.query['month'],
			day: req.query['day'],
			clear_pro : weather_crime("clear"),
			fog_pro : weather_crime("fog"),
			rain_pro : weather_crime("rain"),
			snow_pro : weather_crime("snow"),
			hail_pro : weather_crime("hail"),
			thunder_pro : weather_crime("thunder"),
			tornado_pro : weather_crime("tornado")
		});
		res.send(html);
	});
});

/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);


app.get('/weather.html',function (req, res) {
	var year_val = req.query['year'];
	var month_val = req.query['month'];
	var day_val = req.query['day'];
	var fog_val = (req.query['fog']) ? true : false;
	var rain_val = (req.query['rain']) ? true : false;
	var snow_val = (req.query['snow']) ? true : false;
	var hail_val = (req.query['hail']) ? true : false;
	var thunder_val = (req.query['thunder']) ? true : false;
	var tornado_val = (req.query['tornado']) ? true : false;
	var report = {
		year : year_val,
		month: month_val,
		day: day_val,
		clear : !fog_val && !rain_val && !snow_val && !hail_val && !thunder_val && !tornado_val,
		fog : fog_val,
		rain : rain_val,
		snow : snow_val,
		hail : hail_val,
		thunder : thunder_val,
		tornado : tornado_val
	};

	kafkaProducer.send([{ topic: 'chengyuel_final_weather', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit-weather.html');
		});
});



/* Send simulated crime to kafka */
app.get('/report.html',function (req, res) {
	var year_val = req.query['year'];
	var month_val = req.query['month'];
	var day_val = req.query['day'];
	var report = {
		year : year_val,
		month: month_val,
		day: day_val
	};

	kafkaProducer.send([{ topic: 'chengyuel_final_crime', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit-crime.html');
		});
});


app.listen(port);
