var oplog = require('./oplog'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter;

var Proposer = module.exports = function(address, group) {
  this.address = address;
  this.group = group;
  this.requestId = 0;
  this.outstandingProposal = null;
  this.outstandingAccept = null;
};

util.inherits(Proposer, EventEmitter);

Proposer.prototype.sendProposal = function(epoch) {
  var proposal = {
    type: 'propose',
    requestId: ++this.requestId,
    epoch: epoch,
    leader: this.address
  };

  this.outstandingProposal = {
    requestId: proposal.requestId,
    epoch: epoch,
    successfulResponses: 0,
    lastAcceptedEpoch: 0,
    lastAcceptedLeader: null
  };
  
  this.group.broadcast(proposal);
};

Proposer.prototype.handleProposalReply = function(endpoint, msg) {
  var self = this;
  if (!this.outstandingProposal) return;
  if (msg.requestId !== this.outstandingProposal.requestId) return;

  if (msg.success === true) {
    if (msg.lastAcceptedEpoch > this.outstandingProposal.lastAcceptedEpoch) {
      this.outstandingProposal.lastAcceptedEpoch = msg.lastAcceptedEpoch;
      this.outstandingProposal.lastAcceptedLeader = msg.lastAcceptedLeader;
    }
    this.outstandingProposal.successfulResponses++;
    if (this.outstandingProposal.successfulResponses === (this.group.majority - 1)) {
        self.sendAccept(this.outstandingProposal.epoch);
    }
  } else if (msg.success === false) {
    this.emit('failedProposal', msg);
    this.outstandingProposal = null;
  }
};

Proposer.prototype.sendAccept = function(epoch, value) {
  value = value || null;

  var accept = {
    type: 'accept',
    requestId: ++this.requestId,
    epoch: epoch,
    value: value
  };

  this.outstandingAccept = {
    requestId: this.requestId,
    epoch: epoch,
    value: value,
    successfulResponses: 0
  };

  this.group.broadcast(accept);
  this.outstandingProposal = null;
};

Proposer.prototype.handleAcceptReply = function(endpoint, msg) {
  if (!this.outstandingAccept) return;
  if (msg.requestId !== this.outstandingAccept.requestId) return;

  if (!msg.success) {
    this.emit('failedAccept', msg);
    this.outstandingAccept = null;
  } else {
    this.outstandingAccept.successfulResponses++;
    if (this.outstandingAccept.successfulResponses === (this.group.majority-1)) {
      this.emit('leader', this.address, this.outstandingAccept.epoch);
      this.outstandingAccept = null;
    }
  }
};

