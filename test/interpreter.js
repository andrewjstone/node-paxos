var assert = require('assert');
var db = require('../lib/db');
var interpreter = require('../lib/interpreter');

describe('parse 1 instruction', function() {
  var instructions = null;
  it('succeeds and returns a list of functions', function() {
    instructions = interpreter.parse('s users andrew hashedPass');
    assert(Array.isArray(instructions));
    instructions.forEach(function(instruction) {
      assert.equal(typeof instruction, 'function');
    });
  });

  it('running instructions sets the key andrew to hashedPass in the users hash', function() {
    assert(!db.users);
    instructions.forEach(function(exec) {
      exec();
    });
    db.users.andrew = 'hashedPass';
  });
});
