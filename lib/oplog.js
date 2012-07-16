var fs = require('fs'),
    util = require('util'),
    Stream = require('stream'),
    async = require('async');

var TX_HEADER_SIZE = 8; // bytes

var Oplog = module.exports = function(filename) {
  this.filename = filename;
  this.fd = fs.openSync(filename, 'a+');
  this.writeBuffer = new Buffer(1024*1024); // 1 MB
};

util.inherits(Oplog, Stream);

Oplog.prototype.append = function(txid, data, callback) {
  var fd = this.fd;
  var buffer = this.writeBuffer;
  var offset = 0;

  // 8 byte transaction id
  buffer.writeUInt32LE(txid, offset);
  offset += 4;

  // 4 byte data length
  buffer.writeUInt32LE(data.length, offset);
  offset += 4;

  // data
  buffer.write(data, offset, data.length);
  offset += data.length;

  // TODO: checksum?

  fs.write(fd, buffer, 0, offset, null, function(err, written, buffer) {
    if (err) return callback(err);
    if (written != offset) {
      console.log('written', written, 'buf length', offset);
    };
    fs.fsync(fd, callback);
  });
};

// TODO: Stream reads for large files
Oplog.prototype.play = function() {
  var self = this;
  fs.readFile(this.filename, function(err, buffer) {
    if (err) return self.emit('error', err);
    var done = false;
    var offset = 0;
    var newOffset = 0;
    var txid = 0;
    var dataSize = 0;
    var data = null;

    while (!done) {
      if (offset + TX_HEADER_SIZE > buffer.length) {
       done = true;
       break;
      }
      txid = buffer.readUInt32LE(offset);
      offset += 4;
      dataSize = buffer.readUInt32LE(offset);
      offset += 4;
      newOffset = offset + dataSize;
      if (newOffset > buffer.length) {
       done = true;
       break;
      }
      data = buffer.toString('utf8', offset, newOffset);
      self.emit('data', {txid: txid, ops: data});
      offset = newOffset;
    }

    self.emit('end');
  });
};
