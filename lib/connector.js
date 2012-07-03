var net = require('net'),
    util = require('util'),
    Endpoint = require('./endpoint'),
    EventEmitter = require('events').EventEmitter;

var Connector = module.exports = function(address) {
  var splitAddress = address.split(':');
  this.address = address;
  this.port = Number(splitAddress[1]);
  this.host = splitAddress[0];
  this.connected = false;
  this.attempts = 0;
  this.connect();
};

util.inherits(Connector, EventEmitter);

Connector.prototype.connect = function() {
  var self = this;

  var socket = this.socket = net.connect(this.port, this.host, function() {
    console.log('Connected to '+self.address);
    self.connected = true;
    var endpoint = new Endpoint(socket);
    self.emit('connect', endpoint);
  });

  socket.on('end', function() {
    self.retry();
  });

  socket.on('error', function(err) {
    console.log('socket error for '+self.address+' '+err);
    self.retry();
  });

};

Connector.prototype.retry = function() {
  var self = this;
  console.log('Disconnected from '+self.address);
  self.socket = null;
  self.connected = false;


  // TODO: Use an intelligent backoff strategy
  setTimeout(function() {
    self.attempts++;
    self.connect();
  }, 500);
};
