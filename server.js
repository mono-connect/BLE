const http = require('http');
const fs = require('fs');
const url = require('url');
const moment = require('moment');
const mysql = require('mysql');
const log4js = require('log4js')

var con = mysql.createConnection({
    host : 'localhost',
    user : 'root',
    password: 'Today123',
    port: 3306,
    database:"sensor"
});
con.connect((err) => {
    if (err) throw err;
  
    console.log('connected to mysql');
});

var server = http.createServer(getFromClient);
server.listen(3000);
console.log('Server start!');

function getFromClient(request, response){
    var url_parts = url.parse(request.url, true);
    switch (url_parts.pathname) {

        case '/magnet':
            response_magnet(request, response);
            break;

        case '/temperature':
            response_temperature(request, response);
            break;
    }
}

//ログ設定
log4js.configure({
    appenders: { 'file': { type: 'file', filename: 'logs/access.log' } },
    categories: { default: { appenders: ['file'], level: 'debug' } }
});
var logger = log4js.getLogger('default');

//magnet処理インクルード
eval(fs.readFileSync('magnet.js')+'');
eval(fs.readFileSync('temperature.js')+'');
    
//エンディアン変換関数
function hexBufferReverse (text) {
    var src = new Buffer(text, 'hex');
    var buffer = new Buffer(src.length);
      
    for (var i = 0, j = src.length - 1; i <= j; ++i, --j) {
        buffer[i] = src[j];
        buffer[j] = src[i];
    }
    return buffer.toString('hex');
}
