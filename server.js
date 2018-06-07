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
        password: '',
        port:3306,
        database: ''
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
        const buffer = new Buffer(record.kinesis.data, 'base64');
        const payloadAll = buffer.toString('utf-8');
        console.log('payloadAll:', payloadAll);
        
        //配列格納
        var list = payloadAll.split(',');
        
        //ビーコンMACが登録されているかチェック
        var sensor_macaddrs = list[1];
        var selectQuery = 'SELECT * FROM ?? where sensor_macaddrs IN (?)';
        connection.query(selectQuery, [ 'master', sensor_macaddrs ], function(err, rows, fields) {
            if(err) {
                console.log(formatted + 'Error1');
                return;
            }
            
            else if (!rows.length) {                                            
                console.log(formatted + sensor_macaddrs + ' check NG');
                return;
            }
            
            else if (rows[0].something == 'NULL' || rows[0].something == '') {
                console.log(formatted + 'Error3');
                return;
            }
            console.log(formatted + ' sensor_macaddrs:' + sensor_macaddrs + ' check OK');
            console.log(formatted + ' sensor type = ' + rows[0].sensor_type);
            
            //センサー共通処理
            var box_name = rows[0].box_name;
            var box_beacon_id = rows[0].box_beacon_id;
            var box_style = rows[0].box_style;
            var sensor_type = rows[0].sensor_type;
            var sensor_tenant_id = rows[0].sensor_tenant_id;
            var tenant_name = rows[0].tenant_name;
            var gatewaymac = list[2];
            var rssi = list[3];
            var payload = hexBufferReverse(list[4]);
            var battery = parseInt(payload.slice(22,26), 16)/100;
            var date = moment(list[5],'X').format("YYYY-MM-DD HH:mm:ss");
            var hour_minutes = moment(list[5],'X').format("HH:mm");
            
            var inseartQuery = 'INSERT INTO sample_table set ?';
            
            //マグネットセンサー処理
            if(sensor_type == "MAGNET") {
                var status = parseInt(payload.slice(20,22));
                
                if(status == 4) {
                    connection.query(inseartQuery, 
                    {box_status:'CLOSE',last_status_confirmed_at:date,hour_minutes:hour_minutes,status_changed_at:date,box_name:box_name,box_beacon_id:box_beacon_id,
                    box_style:box_style,sensor_macaddrs:sensor_macaddrs,sensor_type:sensor_type,sensor_battery:battery,sensor_tenant_id:sensor_tenant_id,tenant_name:tenant_name}, 
                    (err, res)  => {
                        if (err) throw err;
                        console.log(formatted + ' mysql insert OK');
                    });
                }
                if(status == 0) {
                    connection.query(inseartQuery, 
                    {box_status:'OPEN',last_status_confirmed_at:date,hour_minutes:hour_minutes,status_changed_at:date,box_name:box_name,box_beacon_id:box_beacon_id,
                    box_style:box_style,sensor_macaddrs:sensor_macaddrs,sensor_type:sensor_type,sensor_battery:battery,sensor_tenant_id:sensor_tenant_id,tenant_name:tenant_name}, 
                    (err, res)  => {
                        if (err) throw err;
                        console.log(formatted + ' mysql insert OK');
                    });
                }
            }

            //遮断、人感、マット、加速度(衝撃)センサー処理
            if(sensor_type == "INTERCEPTION" || sensor_type == "HUMAN" || sensor_type == "MATT" || sensor_type == "GRAVITY") {
                var status = parseInt(payload.slice(20,22));
                
                if(status == 4 || status == 2) {
                    connection.query(inseartQuery, 
                    {box_status:'ACTIVE',last_status_confirmed_at:date,hour_minutes:hour_minutes,status_changed_at:date,box_name:box_name,box_beacon_id:box_beacon_id,
                    box_style:box_style,sensor_macaddrs:sensor_macaddrs,sensor_type:sensor_type,sensor_battery:battery,sensor_tenant_id:sensor_tenant_id,tenant_name:tenant_name}, 
                    (err, res)  => {
                        if (err) throw err;
                        console.log(formatted + ' mysql insert OK');
                    });
                }
                if(status == 0) {
                    connection.query(inseartQuery, 
                    {box_status:'INACTIVE',last_status_confirmed_at:date,hour_minutes:hour_minutes,status_changed_at:date,box_name:box_name,box_beacon_id:box_beacon_id,
                    box_style:box_style,sensor_macaddrs:sensor_macaddrs,sensor_type:sensor_type,sensor_battery:battery,sensor_tenant_id:sensor_tenant_id,tenant_name:tenant_name}, 
                    (err, res)  => {
                        if (err) throw err;
                        console.log(formatted + ' mysql insert OK');
                    });
                }
            }

            //温度・湿度センサー処理
            if(sensor_type == "TEMP") {
                var temp = parseInt(payload.slice(16,20), 16)/100;
                var humid = parseInt(payload.slice(12,16), 16)/100;
                console.log(formatted + ' battery:' + battery);
                console.log(formatted + ' humidity:' + humid);
                console.log(formatted + ' temp:' + temp);
                console.log(formatted + ' timestamp:' + date);
                connection.query(inseartQuery, 
                {box_status:'NULL',last_status_confirmed_at:date,hour_minutes:hour_minutes,status_changed_at:date,box_name:'NULL',box_beacon_id:"1",box_style:'NULL',
                sensor_macaddrs:sensor_macaddrs,sensor_type:sensor_type,sensor_battery:battery,sensor_tenant_id:sensor_tenant_id,tenant_name:'NULL',temp:temp,humidity:humid}, 
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