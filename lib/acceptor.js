var util = require('util'),
    EventEmitter = require('events').EventEmitter;

var Acceptor = module.exports = function(address) {
  this.address = address;
  this.lastAcceptedLeader = null;
  this.lastAcceptedEpoch = 0;
  this.proposedEpoch = 0;
  this.proposedLeader = null;
};

util.inherits(Acceptor, EventEmitter);

Acceptor.prototype.handleProposal = function(endpoint, msg) {
  var reply = {
    type: 'proposeReply',
    requestId: msg.requestId,
    lastAcceptedLeader: this.lastAcceptedLeader,
    lastAcceptedEpoch: this.lastAcceptedEpoch,
    success: false
  }

  if (msg.epoch > this.proposedEpoch) {
    this.proposedEpoch = msg.epoch;
    this.proposedLeader = endpoint.address;
    reply.success = true;
    return endpoint.writeMessage(reply);
  }

  endpoint.writeMessage(reply);
};

Acceptor.prototype.handleAccept = function(endpoint, msg) {
  var reply = {
    type: 'acceptReply',
    requestId: msg.requestId,
    txid: msg.txid,
    lastAcceptedEpoch: this.lastAcceptedEpoch,
    lastAcceptedTxid: this.lastAcceptedTxid,
    lastAcceptedLeader: this.lastAcceptedLeader,
    success: false
  }

  // A new leader was just elected
  if (msg.epoch === this.proposedEpoch && msg.txid === 0 && 
      this.proposedLeader === endpoint.address)
  {
    this.emit('leader', endpoint.address, msg.epoch);
    this.lastAcceptedEpoch = msg.epoch;
    this.lastAcceptedTxid = msg.txid;
    this.lastAcceptedLeader = endpoint.address;
    reply.success = true;
  }

  if (msg.epoch === this.lastAcceptedEpoch && msg.txid === (this.lastAcceptedTxid + 1) &&
      this.proposedLeader === endpoint.address)
  {
    this.lastAcceptedEpoch = msg.epoch;
    this.lastAcceptedTxid = msg.txid;
    this.lastAcceptedLeader = endpoint.address;
    reply.success = true;
  }

  endpoint.writeMessage(reply);
};

