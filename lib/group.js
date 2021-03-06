var Endpoint = require('./endpoint'),
    Connector = require('./connector'),
    EventEmitter = require('events').EventEmitter,
    util = require('util'),
    uuid = require('node-uuid'),
    log = require('./log');

var Group = module.exports = function(localAddress, peerAddresses) {
  this.addresses = peerAddresses.slice(0);
  this.addresses.push(localAddress);
  this.localAddress = localAddress;
  this.server = null;
  this.majority = Math.floor(this.addresses.length/2) + 1;
  this.connectedMembers = 0; 
  this.members = {};
};

util.inherits(Group, EventEmitter);

Group.prototype.start = function(callback) {
  this.initializeMembers();
  this.createServer(callback);
};

Group.prototype.stop = function(callback) {
  var self = this;
  this.server.close(callback);
  Object.keys(this.members).forEach(function(key) {
    var endpoint = self.members[key].endpoint;
    if (endpoint) endpoint.socket.end();
  });
};

Group.prototype.hasHighestAddress = function() {
  var self = this;
  var hasHighest = true;
  Object.keys(this.members).forEach(function(address) {
    var member = self.members[address];
    if (member.endpoint && (address > self.localAddress)) {
      hasHighest = false;
    }
  });
  return hasHighest;
};

Group.prototype.broadcast = function(proposal) {
  log.info('broadcast ', Object.keys(this.members));
  var self = this;
  if (this.connectedMembers >= this.majority) {
    Object.keys(this.members).forEach(function(address) {
      var member = self.members[address];
      if (member && member.endpoint) {
        member.endpoint.writeMessage(proposal);
      }
    });
  }
};

Group.prototype.createServer = function(listenCallback) {
  var hostPort = this.localAddress.split(':');
  this.server = Endpoint.createServer(hostPort[1], hostPort[0], this, listenCallback);
};

Group.prototype.initializeMembers = function() {
  var self = this;
  var peers = this.addresses;

  peers.forEach(function(address) {
    var member = {
      endpoint: null
    };
    self.members[address] = member;

    // Only Connect to peers that sort higher and ourself. This prevents conflicts from
    // two peers connecting to each other.
    if (address >= self.localAddress) {
      member.connector = new Connector(address);
      member.connector.on('connect', function(endpoint) {
        log.info(self.localAddress+' connected to '+address);
        endpoint.address = address;
        self.sendId(endpoint);
        self.addEndpoint(endpoint);
      });
    }
  });
};

Group.prototype.sendId = function(endpoint) {
  endpoint.writeMessage({
    type: 'id',
    requestId: uuid.v1(),
    address: this.localAddress
  });
};

Group.prototype.addEndpoint = function(endpoint) {
  log.info(this.localAddress+' addEndpoint '+endpoint.address);
  var self = this;
  var member = this.members[endpoint.address];

  if (member) {
    member.endpoint = endpoint; 
    this.connectedMembers++;
    if (this.connectedMembers >= this.majority) {
      self.emit('majorityConnected');
    }
  }

  if (endpoint.listeners('message').length) return;

  endpoint.on('message', function(msg) {
    if (msg.type === 'id') {
      self.handleId(endpoint, msg);
    } else {
      self.emit('message', endpoint, msg); 
    }
  });

  endpoint.on('badMessage', function(msg) {
    console.error('badMesssage '+msg);
  });

  endpoint.on('end', function() {
    self.handleDisconnect(endpoint);
  });

  endpoint.on('error', function(err) {
    console.error('Error at server endpoint '+endpoint.address+' '+err);
    self.handleDisconnect(endpoint);
  });
};

Group.prototype.handleId = function(endpoint, msg) {
  endpoint.address = msg.address;
  this.addEndpoint(endpoint);
};

Group.prototype.handleDisconnect = function(endpoint) {
  console.error('Disconnected from endpoint '+endpoint.address);
  var member = this.members[endpoint.address];
  if (member) {
    member.endpoint = null;
    this.connectedMembers--;
    if (this.connectedMembers < this.majority) {
      this.emit('majorityDisconnected');
    } else {
      log.info('disconnect majority');
      this.emit('majorityConnected');
    }
  }
  endpoint.removeAllListeners();
  endpoint = null;
};
