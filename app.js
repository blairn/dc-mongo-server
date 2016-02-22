var express = require('express');
var app = express();
var async = require('async');
var bodyParser = require('body-parser');
var MongoClient = require('mongodb').MongoClient;

// Connection URL
var url = 'mongodb://localhost:27017/rivers';

function processAgg(db, collectionName, agg, callback) {
  db.collection(collectionName).aggregate(agg, callback);
}

function startApp(db) {
  
  app.use(function (req, res, next) {
    // Website you wish to allow to connect
    res.setHeader('Access-Control-Allow-Origin', '*');

    // Request methods you wish to allow
    res.setHeader('Access-Control-Allow-Methods', 'POST');

    // Request headers you wish to allow
    res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');

    // Set to true if you need the website to include cookies in the requests sent
    // to the API (e.g. in case you use sessions)
    res.setHeader('Access-Control-Allow-Credentials', false);

    // Pass to next layer of middleware
    next();
  });
  
  app.use(bodyParser.json());
  
  // set up handler of agg requests
  app.post('/agg/:collection', function (req, res, next) {
    console.log('request!', req.body);
    
    function process(request, callback) {
      var time = new Date().getTime();
      db.collection(req.params.collection).aggregate(request.query, function (err, results) {
        
        console.log('query for', request.chart, 'is', JSON.stringify(request.query), 'successful?', !err, 'result lines?', results ? results.length : 0, 'response time', new Date().getTime() - time);
        callback(err, {chart:request.chart, results: results})
      });
    }

    var time = new Date().getTime();
    async.map(req.body, process, function(err, result) {
      if (!err) {
        res.json({
          success: true,
          data: result
        });
      } else {
        res.json({
          success: false,
          error:err
        });
      }
      console.log('overall response time', new Date().getTime() - time);
      res.end();
    });
  });
              
    
//    responses = {};
//    var respond = function() {
//      res.json(responses);
//      res.end();
//    }
//    var finished = _.after(req.body.length, respond);
//    db.collection(req.params.collection).aggregate(req.body.query, function (err, results) {
//      console.log('err',err);
//      console.log('results',results);
//      if (!err) {
//        res.json({
//          success: true,
//          data: results
//        });
//      } else {
//        res.json({
//          success: false,
//          error:err
//        });
//      }
//      res.end();
//    });
  
//  app.post('/chart/:chart', function (req, res, next) {
//    db.collection.charts.find({"_id":req.params.chart}, function (err, results) {
//      if (!err) {
//        res.json({
//          success: true,
//          data: results
//        });
//      } else {
//        res.json({
//          success: false,
//          error:err
//        });
//        res.end();
//      }
//    });
//  });
  
  
  // and finally listen
  var server = app.listen(3000, function () {
    var host = server.address().address;
    var port = server.address().port;
    console.log('agg processor listening at http://%s:%s', host, port);
  });
}

MongoClient.connect(url, function(err, db) {
  startApp(db);
});