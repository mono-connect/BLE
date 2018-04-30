// magnetのアクセス処理
function response_magnet(request, response) {
    //POSTアクセス時の処理
    if (request.method == 'POST'){
        var body='';
        
        //データ受信時の処理
        request.on('data', function (data) {
            body +=data;
        });
    
        //データ受信終了のイベント処理
        request.on('end', function() {
            //生ログデータ出力
            logger.debug(body);
            
            //配列格納
            var list = body.split(',');

            //対象ビーコンかチェック
            var beaconmac = list[1];
            var selectQuery = 'SELECT * FROM ?? where beaconmac IN (?)';
            con.query(selectQuery, [ 'master', beaconmac ], function(err, row, fields) {
            if(err) {
                console.log('Error1');
                return;
              }
              else if (!row.length) {                                                   
                console.log(beaconmac + ' check NG');
                return;
              }
              else if (row[0].something == 'NULL' || row[0].something == '') {
                console.log('Error3');
                return;
              }
              console.log(beaconmac + ' check OK');
              var code = list[0];
                var gatewaymac = list[2];
                var rssi = list[3];
                var payload = hexBufferReverse(list[4]);
                var battery = parseInt(payload.slice(22,26), 16)/100;
                var status = parseInt(payload.slice(20,22));
                var date = moment(list[5],'X').format();
                console.log(battery);
                console.log(status);
                console.log(date);
                con.query('INSERT INTO magnet set ?', 
                {beaconmac:beaconmac,gatewaymac:gatewaymac,rssi:rssi,battery:battery,status:status,date:date}, 
                (err, res)  => {
                    if (err) throw err;
                    console.log('mysql insert');
                });
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