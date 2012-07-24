var db = require('./db');
var interpreter = module.exports = {};

// All operations implemented by the interpreter
var ops = {
  's': {
    description: 'set table key value',
    validate: function(args) {
      if (args.length != 3) return false;
      for (var i = 0; i < args.length; i++) {
        if (typeof args[i] !== 'string') return false;
      }
      return true;
    },
    compile: function(args) {
      var table = args[0],
          key = args[1],
          val = args[2];
      return function() {
        if (!db[table]) {
          db[table] = {};
        }
        db[table][key] = val;
      };
    }
  }
};

interpreter.parse = function(text) {
  var lines = text.split('\n');
  var instructions = [];
  lines.forEach(function(line) {
    var args = line.split(' ');
    var op = ops[args[0]];
    args = args.slice(1);
    if (op.validate(args)) {
      instructions.push(op.compile(args));
    } else {
      // Log the failing instruction ?
      // Send a notification ?
    }
  });
  return instructions;
};
