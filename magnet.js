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
            //var post_data = qs.parse(body) + ''; //データのパース
            var list = body.split(',');
                var code = list[0];
                var beaconmac = list[1];
                var gatewaymac = list[2];
                var rssi = list[3];
                var payload = hexBufferReverse(list[4]);
                var battery = parseInt(payload.slice(22,26), 16)/100;
                var status = parseInt(list[4].slice(22,24));
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

                //con.end((err) => {
                //    if (err) throw err;
                //    console.log('disconnected to mysql');
                //  });
        
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

//end