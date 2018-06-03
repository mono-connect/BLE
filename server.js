//モジュール読み込み
const moment = require('moment');
const mysql = require('mysql');
const dateutils = require('date-utils');

//時刻フォーマット
var date = new Date();
var formatted = date.toFormat("YYYY/MM/DD HH24時MI分SS秒");

//イベント発生処理
console.log('Loading function');
exports.handler = function(event, context, callback) {
    //mysql接続用コンフィグ
    var db_config = {
        host: 'visualizationdb001.cya5resp0zlq.ap-northeast-1.rds.amazonaws.com',
        user: 'rsuser',
        password: 'Today123',
        port:3306,
        database: 'visualizationdb'
    };
    //mysql接続
    var connection;
    function handleDisconnect() {
        console.log(formatted + ' INFO.CONNECTION_DB: ');
        connection = mysql.createConnection(db_config);
        //connection取得
        connection.connect(function(err) {
            if (err) {
                console.log(formatted + ' ERROR.CONNECTION_DB: ', err);
                setTimeout(handleDisconnect, 1000);
            }
        });
        
        //error('PROTOCOL_CONNECTION_LOST')時に再接続
        connection.on(formatted + 'error', function(err) {
            console.log(formatted + ' ERROR.DB: ', err);
            if (err.code === 'PROTOCOL_CONNECTION_LOST') {
                console.log(formatted + 'ERROR.CONNECTION_LOST: ', err);
                handleDisconnect();
            } else {
                throw err;
            }
        });
    }
    handleDisconnect();
    //センサーデータ取得
    event.Records.forEach(function(record) {
        console.log('kinesis:', record.kinesis.data);
        
        // Kinesis data is base64 encoded so decode here
        // make object by json decode
        const payloadAll = JSON.parse(new Buffer(record.kinesis.data, 'base64').toString('ascii'));
        console.log('payloadAll:', payloadAll);
        const payload = payloadAll[0];
        
        //配列格納
        var list = payload.split(',');
        
        //ビーコンMACが登録されているかチェック
        var beaconmac = list[1];
        var selectQuery = 'SELECT * FROM ?? where beaconmac IN (?)';
        connection.query(selectQuery, [ 'master', beaconmac ], function(err, rows, fields) {
            if(err) {
                console.log(formatted + 'Error1');
                return;
            }
            
            else if (!rows.length) {                                            
                console.log(formatted + beaconmac + ' check NG');
                return;
            }
            
            else if (rows[0].something == 'NULL' || rows[0].something == '') {
                console.log(formatted + 'Error3');
                return;
            }
            console.log(formatted + ' beaconmac:' + beaconmac + ' check OK');
            console.log(formatted + ' sensor type = ' + rows[0].type);
            
            //センサー共通処理
            var code = list[0];
            var gatewaymac = list[2];
            var rssi = list[3];
            var payload = hexBufferReverse(list[4]);
            var battery = parseInt(payload.slice(22,26), 16)/100;
            var date = moment(list[5],'X').format();
            
            var inseartQuery = 'INSERT INTO magnet set ?';
            
            //マグネットセンサー処理
            if(rows[0].type == "magnet") {
                var status = parseInt(payload.slice(20,22));
                console.log(formatted + ' battery:' + battery);
                console.log(formatted + ' status:' + status);
                console.log(formatted + ' timestamp:' + date);
                
                if(status == 4) {
                    connection.query('INSERT INTO magnet set ?', 
                    {beaconmac:beaconmac,gatewaymac:gatewaymac,rssi:rssi,battery:battery,status:status,status2:'ON',date:date}, 
                    (err, res)  => {
                        if (err) throw err;
                        console.log(formatted + ' mysql insert OK');
                    });
                }
                if(status == 0) {
                    connection.query('INSERT INTO magnet set ?', 
                    {beaconmac:beaconmac,gatewaymac:gatewaymac,rssi:rssi,battery:battery,status:status,status2:'OFF',date:date}, 
                    (err, res)  => {
                        if (err) throw err;
                        console.log(formatted + ' mysql insert OK');
                    });
                }
            }
            //温度・湿度センサー処理
            else if(rows[0].type == "temperature") {
                var temp = parseInt(payload.slice(16,20), 16)/100;
                var humid = parseInt(payload.slice(12,16), 16)/100;
                console.log(formatted + ' battery:' + battery);
                console.log(formatted + ' humidity:' + humid);
                console.log(formatted + ' temp:' + temp);
                console.log(formatted + ' timestamp:' + date);
                connection.query('INSERT INTO temperature set ?', 
                {beaconmac:beaconmac,gatewaymac:gatewaymac,rssi:rssi,battery:battery,temp:temp,humid:humid,date:date}, 
                (err, res)  => {
                    if (err) throw err;
                    console.log(formatted + 'mysql insert OK');
                });
            }
        });
    });
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