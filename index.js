const AWS = require('aws-sdk');
const async = require('async');
const _ = require('underscore');

AWS.config.update({region:'us-east-1'});
const logGroupName='useast1-logs01.dev.fusion.autodesk.com';

var cloudwatchlogs = new AWS.CloudWatchLogs({apiVersion: '2014-03-28'});
let logstreams = [];
const streamsToken = {};

const startTime = new Date().getTime() - parseInt(process.argv[2] || 3600)*1000;
const nop = () => {};

function enumerateLogStreams(cb) {
  cb = cb || nop;
  let nextToken=undefined;
  let overlap = false;
  var params = {
    logGroupName,
    descending: true,
    limit: 50,
    orderBy: 'LastEventTime'
  };
  async.until(() => overlap || nextToken==='', (cb) =>
    cloudwatchlogs.describeLogStreams(params, function(err, data) {
      let streams = _.pluck(data.logStreams || [], 'logStreamName');
      const matches = _.intersection(logstreams, streams);
      if(matches.length) {
        overlap = true;
      }
      streams = _.without(streams, logstreams);
      // delete everything not fzc
      async.each(streams.filter(s => s.indexOf('fzc')!==0), (logStreamName, cb) => cloudwatchlogs.deleteLogStream({logStreamName, logGroupName}, ()=>{console.info('Deleted',logStreamName); cb();} ), nop);
      async.filter(streams, deleteStreamIfEmpty, (err, streams) => {
        logstreams = logstreams.concat(streams);
        params.nextToken = data.nextToken;
        cb(err);
      });
    }),
  cb);
}


function deleteStreamIfEmpty(logStreamName, cb) {
  var params = {
      logGroupName,
      logStreamName,
      startTime: 0,
      limit: 1,
      startFromHead: true
  };
  cloudwatchlogs.getLogEvents(params, function(err, data) {
    if(!err && data.events.length === 0) {
      async.retry( { interval: 1000 }, cb => cloudwatchlogs.deleteLogStream({logStreamName, logGroupName}, cb), (err) => {
        console.info('Stream', logStreamName, 'is empty, delete', err || '');
        cb(null, false);
      });
    } else {
      // console.info('Stream', logStreamName, 'is not empty');
      cb(null, true);
    }
  });
}

function scanStreams(cb) {
  cb = cb || nop;
  async.each(logstreams, (logStreamName, cb) => {
    const nextToken = streamsToken[logStreamName];
    var params = {
        logGroupName,
        logStreamName,
        nextToken,
        startTime,
        startFromHead: true
    };
    let token = '<no token>';
    async.until(() => token === nextToken, (cb) => {
      cloudwatchlogs.getLogEvents(params, function(err, data) {
        nextToken = token;
        if(err) return cb(err);
        _.each(data.events, e=>console.info(logStreamName, new Date(e).toISOString(), e.message));
        streamsToken[logStreamName] = token = data.nextForwardToken;
        cb();
      });
    });
  }, cb);
}
enumerateLogStreams(err => {
  setInterval(scanStreams, 10000);
  scanStreams();
  const redo = (err) => enumerateLogStreams(redo);
  redo();
});
