//モジュール読み込み
const http = require('http');
const fs = require('fs');
const url = require('url');
const moment = require('moment');
const mysql = require('mysql');
const log4js = require('log4js')

//HTTP Server起動
var server = http.createServer(getFromClient);
server.listen(3000);
console.log('Server start!');

function getFromClient(request, response){
    var url_parts = url.parse(request.url, true);
    switch (url_parts.pathname) {

        case '/sensor':
            response_sensor(request, response);
            break;
    }
}

//ログ設定
log4js.configure({
    appenders: { 'file': { type: 'file', filename: 'logs/access.log' } },
    categories: { default: { appenders: ['file'], level: 'debug' } }
});
var logger = log4js.getLogger('default');

//mysql接続用コンフィグ
var db_config = {
    host: 'localhost',
    user: 'root',
    password: 'Today123',
    port:3306,
    database: 'sensor'
};

//mysql接続
var connection;

function handleDisconnect() {
    console.log('INFO.CONNECTION_DB: ');
    connection = mysql.createConnection(db_config);
    
    //connection取得
    connection.connect(function(err) {
        if (err) {
            console.log('ERROR.CONNECTION_DB: ', err);
            setTimeout(handleDisconnect, 1000);
        }
    });
    
    //error('PROTOCOL_CONNECTION_LOST')時に再接続
    connection.on('error', function(err) {
        console.log('ERROR.DB: ', err);
        if (err.code === 'PROTOCOL_CONNECTION_LOST') {
            console.log('ERROR.CONNECTION_LOST: ', err);
            handleDisconnect();
        } else {
            throw err;
        }
    });
}

handleDisconnect();

//POSTデータ受信
function response_sensor(request, response) {
    if (request.method == 'POST'){
        var body='';

        request.on('data', function (data) {
            body +=data;
        });
        
        request.on('end', function() {
            //生ログデータ出力
            logger.debug(body);

            //配列格納
            var list = body.split(',');

            //ビーコンMACが登録されているかチェック
            var beaconmac = list[1];
            var selectQuery = 'SELECT * FROM ?? where beaconmac IN (?)';
            connection.query(selectQuery, [ 'master', beaconmac ], function(err, rows, fields) {
                if(err) {
                    console.log('Error1');
                    return;
                  }
                  else if (!rows.length) {                                            
                    console.log(beaconmac + ' check NG');
                    return;
                  }
                  else if (rows[0].something == 'NULL' || rows[0].something == '') {
                    console.log('Error3');
                    return;
                  }
                  
                  console.log(beaconmac + ' check OK');
                  console.log("sensor type = "+rows[0].type);

                  //センサー共通処理
                  var code = list[0];
                  var gatewaymac = list[2];
                  var rssi = list[3];
                  var payload = hexBufferReverse(list[4]);
                  var battery = parseInt(payload.slice(22,26), 16)/100;
                  var date = moment(list[5],'X').format();
                    
                  //マグネットセンサー処理
                  if(rows[0].type == "magnet") {
                    var status = parseInt(payload.slice(20,22));
                    console.log(battery);
                    console.log(status);
                    console.log(date);

                    connection.query('INSERT INTO magnet set ?', 
                    {beaconmac:beaconmac,gatewaymac:gatewaymac,rssi:rssi,battery:battery,status:status,date:date}, 
                        (err, res)  => {
                            if (err) throw err;
                            console.log('mysql insert');
                        });
                    }
                    //温度・湿度センサー処理
                    else if(rows[0].type == "temperature") {
                        var temp = parseInt(payload.slice(16,20), 16)/100;
                        var humid = parseInt(payload.slice(12,16), 16)/100;
                        console.log(battery);
                        console.log(humid);
                        console.log(temp);
                        console.log(date);
                        connection.query('INSERT INTO temperature set ?', 
                        {beaconmac:beaconmac,gatewaymac:gatewaymac,rssi:rssi,battery:battery,temp:temp,humid:humid,date:date}, 
                        (err, res)  => {
                            if (err) throw err;
                            console.log('mysql insert');
                        });
                    }
                    //加速度センサー処理
                    //else if () {}
                });

                //200ステータス返却
                response.statusCode = 200;
                response.setHeader('Content-type', 'text/plain');
                response.end();
            });

        } else {
            response.statusCode = 404;
            response.setHeader('Content-type', 'text/plain');
            response.write('Bad request');
            response.end();
        }
    }
    
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