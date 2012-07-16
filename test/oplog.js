var fs = require('fs');
var assert = require('assert');
var Oplog = require('../lib/oplog');

try {
  fs.unlinkSync('./output/testOplog.log');
} catch(e) {
}

var oplog = new Oplog('./output/testOplog.log');
var txid = 1;
var op1 = 's table1 key value\n';

describe('append to the oplog', function() {
  it('succeeds', function(done) {
    oplog.append(txid, op1, done);
  });
});

describe('play the oplog', function() {
  it('emits only one transaction', function(done) {
    var txct = 0;
    oplog.on('data', function(data) {
      txct++;
      assert.equal(data.txid, txid);
      assert.equal(data.ops, op1);
    });
    oplog.on('end', function() {
      assert.equal(txct, 1);
      done();
    });
    oplog.play();
  });
});

