const Influx = require('influx');
var os = require('os')
const influx = new Influx.InfluxDB({
 host: '10.129.23.161',
 database: 'express_response_db',

})

influx.writePoints([
  {
    measurement: 'response_times',
    tags: { host: os.hostname() },
    fields: { duration:3, path: "zgod.local" },
    timestamp: 1505925836.9*1000000000
  }
])