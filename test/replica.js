var Replica = require('../lib/replica');
var assert = require('assert');

var replica1 = null,
    replica2 = null,
    replica3 = null;

describe('create a cluster of three replicas', function() {
  it('start the first replica', function(done) {
    replica1 = new Replica({
      name: 'replica1',
      address: '127.0.0.1:11527',
      peers: ['127.0.0.1:11528', '127.0.0.1:11529']
    });
    replica1.start(done);
  });

  it('start the second replica', function(done) {
    replica2 = new Replica({
      name: 'replica2',
      address: '127.0.0.1:11528',
      peers: ['127.0.0.1:11527', '127.0.0.1:11529']
    });
    replica2.start(done);
  });

  it('start the third replica', function(done) {
    replica3 = new Replica({
      name: 'replica3',
      address: '127.0.0.1:11529',
      peers: ['127.0.0.1:11527', '127.0.0.1:11528']
    });
    replica3.start(done);
  });
});

describe('ensure cluster forms correctly', function() {
  it('replicas are connected', function(done) {
    setTimeout(function() {
      assert(replica1.group.members['127.0.0.1:11528'].endpoint);
      assert(replica1.group.members['127.0.0.1:11529'].endpoint);
      done();
    }, 1000);
  });

  it('one replica is elected as leader', function(done) {
    setTimeout(function() {
      assert(replica1.leader);
      assert.equal(replica1.leader, replica2.leader);
      assert.equal(replica2.leader, replica3.leader);
      console.log('LEADER = '+replica1.leader);
      done();
    }, 1000);
  });
});

//describe('leader is stopped', function() {
//  it('stop replica3 (127.0.0.1:11529)', function(done) {
//    replica3.stop();
//    setTimeout(function() {
//      done();
//    }, 1000);
//  });
//
//  it('replica2 is elected as leader', function() {
//      assert.equal(replica1.group.leader, '127.0.0.1:11528');
//      assert.equal(replica2.group.leader, '127.0.0.1:11528');
//      assert.equal(replica3.group.leader, null);
//  });
//});
