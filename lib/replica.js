var oplog = require('./oplog'),
    Group = require('./group'),
    Acceptor = require('./acceptor'),
    Proposer = require('./proposer');

var Replica = module.exports = function(opts) {
  var self = this;

  this.address = opts.address || '127.0.0.1:11527';
  this.group = new Group(this.address, opts.peers);
  this.acceptor = new Acceptor(this.address);
  this.proposer = new Proposer(this.address, this.group);
  this.state = 'init';
  this.leader = null;
  this.epoch = 0;
  this.timeout = null;

  this.group.on('message', function(endpoint, msg) {
    self.handleMessage(endpoint, msg);
  });
  this.group.on('majorityConnected', function() {
    self.electLeader();
  });
  this.group.on('majorityDisconnected', function() {
    clearTimeout(self.timeout);
    self.state = 'waitForConnections';
  });

  this.acceptor.on('leader', function(leader, epoch) {
    self.setLeader(leader, epoch);
  });

  this.proposer.on('leader', function(leader, epoch) {
    self.setLeader(leader, epoch);
  });
  this.proposer.on('failedProposal', function(proposalReply) {
    if (proposalReply.lastAcceptedEpoch > this.epoch) {
      self.catchup();
    } else {
      self.electLeader();
    }
  });
};

Replica.prototype.setLeader = function(leader, epoch) {
  this.leader = leader;
  this.epoch = epoch;
  cancelTimeout(this.timeout);
  if (this.leader === this.address) {
    this.state = 'leader';
  } else {
    this.state = 'follower';
  }
};

Replica.prototype.start = function(callback) {
  var self = this;
  this.state = 'oplog';
  this.replayOplog(function() {
    self.state = 'waitForConnections';
    self.group.start(callback);
  });
};

Replica.prototype.stop = function(callback) {
  this.group.stop(callback);
};

Replica.prototype.replayOplog = function(callback) {
  callback();
};

Replica.prototype.electLeader = function() {
  var self = this;
  var wait = Math.random()*2000;
  this.timeout = setTimeout(function() {
    if (self.acceptor.proposedEpoch <= self.epoch) {
      self.proposer.sendProposal(++self.epoch);
    }
  }, wait);
};

Replica.prototype.handleMessage = function(endpoint, msg) {
  switch(msg.type) {
    case 'sync':
      return this.handleSync(endpoint, msg);
    case 'bulk':
      return this.handleBulk(endpoint, msg);
    case 'propose': 
      return this.acceptor.handleProposal(endpoint, msg);
    case 'proposeReply': 
      return this.proposer.handleProposalReply(endpoint, msg);
    case 'accept': 
      return this.acceptor.handleAccept(endpoint, msg);
    case 'acceptReply':
      return this.proposer.handleAcceptReply(endpoint, msg);
  };
};
