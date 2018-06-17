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
        connection.query(selectQuery, [ 'sample_table', sensor_macaddrs ], function(err, rows, fields) {
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
            var rssi = list[3];
            var payload = hexBufferReverse(list[4]);
            var battery = parseInt(payload.slice(22,26), 16)/100;
            var date = moment(list[5],'X').format("YYYY-MM-DD HH:mm:ss");
            var hour_minutes = moment(list[5],'X').format("HH:mm");
            var status = parseInt(payload.slice(20,22));
            var temp = parseInt(payload.slice(16,20), 16)/100;
            var humid = parseInt(payload.slice(12,16), 16)/100;
            var last_status = rows[0].last_status;
            var sensor_type = rows[0].sensor_type;
            var box_name = rows[0].box_name;
            var box_beacon_id = rows[0].box_beacon_id;
            var box_style = rows[0].box_style;
            var sensor_tenant_id = rows[0].sensor_tenant_id;
            var tenant_name = rows[0].tenant_name;
            var box_status = rows[0].box_status;

            //SQL共通処理
            var updateQuery = 'UPDATE sample_table SET ?? = ? WHERE sensor_macaddrs = ?';
            var inseartQuery = 'INSERT INTO log_table set ?';
            var obj1 = { 'last_status_confirmed_at':date, 'hour_minutes':hour_minutes, 'sensor_battery': battery };
            var obj2 = { 'last_status_confirmed_at':date, 'hour_minutes':hour_minutes, 'sensor_battery': battery, 'box_status': 'ACTIVE', 'last_status': status, 'status_changed_at': date };
            var obj3 = { 'last_status_confirmed_at':date, 'hour_minutes':hour_minutes, 'sensor_battery': battery, 'box_status': 'INACTIVE', 'last_status': status, 'status_changed_at': date };
            var obj4 = { 'last_status_confirmed_at':date, 'hour_minutes':hour_minutes, 'sensor_battery': battery, 'temp': temp, 'humidity': humid, 'status_changed_at': date };
            
            //マグネットセンサー、遮断、人感、マット、加速度(衝撃)センサー処理処理
            if(sensor_type == "MAGNET" || sensor_type == "INTERCEPTION" || sensor_type == "HUMAN" || sensor_type == "MATT" || sensor_type == "GRAVITY" ) { 
                if(status == last_status) {
                    Object.keys(obj1).forEach(function(key) {
                        var val = this[key];
                        connection.query(updateQuery, [key, val, sensor_macaddrs],
                            (err, res)  => {
                                if (err) throw err;
                                console.log(formatted + key +":" +val + 'update OK');
                            }
                        );
                    }, obj1);
                    //ログ記録
                    connection.query(inseartQuery,
                    {box_status:box_status,last_status_confirmed_at:date,hour_minutes:hour_minutes,status_changed_at:date,box_name:box_name,box_beacon_id:box_beacon_id,
                    box_style:box_style,sensor_macaddrs:sensor_macaddrs,sensor_type:sensor_type,sensor_battery:battery,sensor_tenant_id:sensor_tenant_id,tenant_name:tenant_name},
                    (err, res)  => {
                        if (err) throw err;
                        console.log(formatted + ' log insert OK');
                    });
                }
                else if(status == 4 || status == 2) {
                    Object.keys(obj2).forEach(function(key) {
                        var val = this[key];
                        connection.query(updateQuery, [key, val, sensor_macaddrs],
                            (err, res)  => {
                                if (err) throw err;
                                console.log(formatted + key +":" +val + 'update OK');
                            }
                        );
                    }, obj2);
                    //ログ記録
                    connection.query(inseartQuery,
                        {box_status:'ACTIVE',last_status_confirmed_at:date,hour_minutes:hour_minutes,status_changed_at:date,box_name:box_name,box_beacon_id:box_beacon_id,
                        box_style:box_style,sensor_macaddrs:sensor_macaddrs,sensor_type:sensor_type,sensor_battery:battery,sensor_tenant_id:sensor_tenant_id,tenant_name:tenant_name},
                        (err, res)  => {
                            if (err) throw err;
                            console.log(formatted + ' log insert OK');
                        }
                    );
                }
                else if(status == 0) {
                    Object.keys(obj3).forEach(function(key) {
                        var val = this[key];
                        connection.query(updateQuery, [key, val, sensor_macaddrs],
                            (err, res)  => {
                                if (err) throw err;
                                console.log(formatted + key +":" +val + 'update OK');
                            }
                        );
                    }, obj3);
                    //ログ記録
                    connection.query(inseartQuery,
                        {box_status:'INACTIVE',last_status_confirmed_at:date,hour_minutes:hour_minutes,status_changed_at:date,box_name:box_name,box_beacon_id:box_beacon_id,
                        box_style:box_style,sensor_macaddrs:sensor_macaddrs,sensor_type:sensor_type,sensor_battery:battery,sensor_tenant_id:sensor_tenant_id,tenant_name:tenant_name},
                        (err, res)  => {
                            if (err) throw err;
                            console.log(formatted + ' log insert OK');
                        }
                    );
                }
            }

            //温度・湿度センサー処理
            if(sensor_type == "TEMP") {
                Object.keys(obj4).forEach(function(key) {
                    var val = this[key];
                    connection.query(updateQuery, [key, val, sensor_macaddrs],
                        (err, res)  => {
                            if (err) throw err;
                            console.log(formatted + key +":" +val + 'update OK');
                        }
                    );
                }, obj4);

                //ログ記録
                connection.query(inseartQuery,
                    {box_status:'NULL',last_status_confirmed_at:date,hour_minutes:hour_minutes,status_changed_at:date,box_name:'NULL',box_beacon_id:"1",box_style:'NULL',
                    sensor_macaddrs:sensor_macaddrs,sensor_type:sensor_type,sensor_battery:battery,sensor_tenant_id:sensor_tenant_id,tenant_name:'NULL',temp:temp,humidity:humid},
                    (err, res)  => {
                    if (err) throw err;
                        console.log(formatted + ' log insert OK');
                    }
                );
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