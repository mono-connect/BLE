const lambda = require('./server.js');

//ダミーデータ読み込み
var fs = require('fs');
var event = JSON.parse(fs.readFileSync('./kinesisevent.json', 'utf8'));
var context = "";
function callback(error, result) {
  if (typeof error !== 'null') {
    console.error(result);
    process.exit(1);
  }
  console.log(result);
  process.exit(0);
}

lambda.handler(event, context, callback);