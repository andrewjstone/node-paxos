var oplog = module.exports = {};

// TODO: Back this on disk with a sane format 
var log = [];

// TODO: Actually stream from the disk log
oplog.stream = function(counter, endpoint) {
  var data = log.slice(counter);
  endpoint.writeMessage(data);
};

