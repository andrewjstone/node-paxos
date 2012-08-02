// Message oriented abstraction of client and server sockets for use 

var net = require('net'),
    util = require('util'),
    oplog = require('./oplog'),
    EventEmitter = require('events').EventEmitter,
    log = require('./log');

var Endpoint = module.exports = function(socket) {
  socket.setEncoding('utf8');
  var self = this;
  this.socket = socket;
  this.address = null;
  this.data = '';

  socket.on('end', function() {
    self.emit('end');
  });

  socket.on('error', function(err) {
    self.emit('error', err);
  });

  socket.on('drain', function() {
    self.emit('drain');
  });

  socket.on('data', function(data) {
    self.parse(data);
  });

};

util.inherits(Endpoint, EventEmitter);

Endpoint.createServer = function(port, host, group, callback) {
  var server = net.createServer(function(socket) {
    var endpoint = new Endpoint(socket);
    group.addEndpoint(endpoint);
  }).listen(port, host, function() {
    log.info('Replica bound to '+host+':'+port);
    if (callback) callback();
  });
  return server;
};

Endpoint.prototype.parse = function(data) {
  var self = this;
  var parsedMsg = null;
  var msg = null;
  this.data += data;
  var lineEnd = data.indexOf('\r\n');

  while (lineEnd !== -1) {
    msg = this.data.slice(0, lineEnd);
    this.data = this.data.slice(lineEnd+2);

    try {
      parsedMsg = JSON.parse(msg);
      self.emit('message', parsedMsg);
    } catch(e) {
      console.error('Failed JSON.parse('+msg+') '+e);
      self.emit('badMessage', msg);
    }

    lineEnd = this.data.indexOf('\r\n');
  };
};

Endpoint.prototype.writeMessage = function(msg) {
  var json = JSON.stringify(msg)+"\r\n";
  this.socket.write(json);
};
