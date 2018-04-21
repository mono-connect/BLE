'use strict';

var log4js = require('log4js');
var config = require('config');
log4js.configure(config.log4js);
var logger = { 
  access: log4js.getLogger('access')
};

module.exports = function (req, res, next) {

    var oldWrite = res.write,
        oldEnd = res.end;

    var chunks = [];

    res.write = function (chunk) {
      chunks.push(chunk);

      oldWrite.apply(res, arguments);
    };

    res.end = function (chunk) {
      if (chunk)
        chunks.push(chunk);

      var body = Buffer.concat(chunks).toString('utf8');

      // ここで必要なログをくっつける
      var logs = {
        requestHeader: req.headers,
        requestBody: req.body,
        responseHeader: {},
        // 取得したbodyはテキスト型であるため、一旦Json形式に変換しておく
        responseBody: JSON.parse(body)
      };
      logger.access.info(req.method, req.path, JSON.stringify(logs));

      oldEnd.apply(res, arguments);
    };

    next();
  };