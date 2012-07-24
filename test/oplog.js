var fs = require('fs');
var assert = require('assert');
var Oplog = require('../lib/oplog');

try {
  fs.unlinkSync('./output/testOplog.log');
} catch(e) {
}

var oplog = new Oplog('./output/testOplog.log');
var epoch = 1;
var txid = 1;
var op1 = 's table1 key value\n';

describe('append to the oplog', function() {
  it('succeeds', function(done) {
    oplog.append(epoch, txid, op1, done);
  });
});

describe('play the oplog', function() {
  it('emits only one transaction', function(done) {
    var txct = 0;
    var stream = oplog.play();
    stream.on('data', function(data) {
      txct++;
      assert.equal(data.txid, txid);
      assert.equal(data.ops, op1);
    });
    stream.on('end', function() {
      assert.equal(txct, 1);
      done();
    });
  });
});

describe('append another transaction with two ops', function() {
  it('succeeds', function(done) {
    oplog.append(epoch, ++txid, 's table2 key1 value1\ns table2 key2 value2\n', done);
  });

  it('playing the oplog again will emit 2 transcations', function(done) {
    var txct = 0;
    var stream = oplog.play();
    stream.on('data', function(data) {
      txct++;
      assert.equal(data.txid, txct);
    });
    stream.on('end', function() {
      assert.equal(txct, 2);
      done();
    });
  });
});
