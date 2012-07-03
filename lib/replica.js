var oplog = require('./oplog'),
    Group = require('./group');

var Replica = module.exports = function(opts) {
  var self = this;
  var port = opts.port || '11527';
  var host = opts.host || '127.0.0.1';

  this.address = host+':'+port;
  this.name = opts.name || this.address;
  this.leader = null;
  this.requestId = 0;
  this.counter = 0;
  this.highestProposer = null;
  this.lastAcceptedCounter = 0;
  this.lastAcceptedValue = null;
  this.outstandingProposal = null;
  this.outstandingAcceptence = null;
  this.group = new Group(this.address, opts.peers);

  this.group.on('message', function(endpoint, msg) {
    self.handleMessage(endpoint, msg);
  });
  this.group.on('majorityConnected', function() {
    self.proposeLeader();
  });
};

Replica.prototype.start = function(callback) {
  this.replayOplog();
  this.group.start(callback);
};

Replica.prototype.handleMessage = function(endpoint, msg) {
  switch(msg.type) {
    case 'propose': 
      return this.handlePropose(endpoint, msg);
    case 'proposeReply': 
      return this.handleProposeReply(endpoint, msg);
    case 'accept': 
      return this.handleAccept(endpoint, msg);
    case 'acceptReply':
      return this.handleAcceptReply(endpoint, msg);
  };
};

Replica.prototype.handleAccept = function(endpoint, msg) {
  var reply = {
    type: 'acceptReply',
    requestId: msg.requestId
  }

  // Have we seen a proposal with a higher counter?
  if (this.counter > msg.counter) {
    reply.success = false;
  } else {
    // TODO: actually write the oplog
    // oplog.write(endpoint, msg);
    reply.success = true;
    this.lastAcceptedCounter = msg.counter;
    this.lastAcceptedValue = msg.value;
    this.leader = endpoint.address;
  }

  endpoint.writeMessage(reply);
};


Replica.prototype.handlePropose = function(endpoint, msg) {
  var reply = {
    type: 'proposeReply',
    requestId: msg.requestId,
    lastAcceptedCounter: this.lastAcceptedCounter,
    lastAcceptedValue: this.lastAcceptedValue,
    lastAcceptedReplica: this.lastAcceptedReplica
  }

  if (msg.counter > this.counter) {
    this.counter = msg.counter;
    this.highestProposer = msg.address;
    reply.success = true;
    return endpoint.writeMessage(reply);
  }

  reply.success = false
  endpoint.writeMessage(reply);
};

Replica.prototype.handleProposeReply = function(endpoint, msg) {
  var self = this;
  if (!this.outstandingProposal) return;
  if (msg.requestId !== this.outstandingProposal.requestId) return;

  if (msg.success === true) {
    if (msg.lastAcceptedCounter > this.outstandingProposal.lastAcceptedCounter) {
      this.outstandingProposal.lastAcceptedCounter = msg.lastAcceptedCounter;
      this.outstandingProposal.lastAcceptedValue = msg.lastAcceptedValue;
      this.outstandingProposal.lastAcceptedReplica = endpoint.address;
    }

    this.outstandingProposal.successfulResponses++;
    if (this.outstandingProposal.successfulResponses >= this.group.majority) {
        self.sendAccept();
    }
  } else if (msg.success === false) {
    this.outstandingProposal = null;
  }
};

Replica.prototype.handleAcceptReply = function(endpoint, msg) {
  if (!this.outstandingAccept) return;
  if (msg.requestId !== this.outstandingAccept.requestId) return;

  if (!msg.success) {
    this.outstandingAccept = null;
  } else {
    this.outstandingAccept.successfulResponses++;
    if (this.outstandingAccept.successfulResponses >= this.group.majority) {
      // TODO: Log this to the oplog
      // oplog.write(endpoint,  msg);
      this.leader = this.address;
    }
  }
};

Replica.prototype.sendAccept = function() {
  var value = this.outstandingProposal.lastAcceptedValue;

  var accept = {
    type: 'accept',
    requestId: ++this.requestId,
    counter: this.counter,
    value: this.outstandingProposal.lastAcceptedValue || null
  };

  this.outstandingAccept = {
    requestId: this.requestId,
    counter: this.counter,
    successfulResponses: 0
  };

  this.group.broadcast(accept);
  this.outstandingProposal = null;
};

Replica.prototype.sendProposal = function() {
  var proposal = {
    type: 'propose',
    requestId: ++this.requestId,
    counter: ++this.counter
  };

  this.highestProposer = this.address;
  this.outstandingProposal = {
    requestId: proposal.requestId,
    counter: proposal.counter,
    successfulResponses: 0,
    lastAcceptedCounter: 0,
    lastAcceptedValue: null,
    lastAcceptedReplica: null
  };
  
  this.group.broadcast(proposal);
};

Replica.prototype.proposeLeader = function() {
  if (this.leader != this.address && this.group.hasHighestAddress()) {
    console.log('proposeLeader '+this.address);
    this.sendProposal();
  };
};

// TODO; Actually replay the oplog
Replica.prototype.replayOplog = function() {
};




