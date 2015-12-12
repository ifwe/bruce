/* ----------------------------------------------------------------------------
   Copyright 2015 Ben Diamant @ PerimeterX <ben@perimeterx.com>

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
   ----------------------------------------------------------------------------

   This is an example NodeJS Client that sends messages to Bruce.  It requires
   the unix-dgram module, as shown here:

       https://github.com/bnoordhuis/node-unix-dgram
       https://www.npmjs.com/package/unix-dgram

   To install unix-dgram, you need to be running at least version 0.10.38 of
   Node.js.  One way to install a recent enough version of Node.js is by using
   nvm (see https://github.com/creationix/nvm) as described here:

       https://www.digitalocean.com/community/tutorials/how-to-install-node-js-on-a-centos-7-server

   Then you can install unix-dgram as follows:

       npm install unix-dgram

   and run this client script as follows:

       node send_to_bruce.js
 */

"use strict";

var unix = require('unix-dgram');

function GetEpochMilliseconds() {
    return Date.now();
}

/* Return the low 32 bits of 'n'.  This is a workaround for Javascript's
   limited support for large integers. */
function Low32Bits(n) {
    var result = n & 0x7fffffff;

    if (n & 0x80000000) {
        result += 0x80000000;
    }

    return result;
}

/* Return a two-item array.  The second item is the low 32 bits of 'n', and the
   first item is the high bits.  This is a workaround for Javascript's limited
   support for large integers. */
function BreakInt(n) {
    var low_32_bits = Low32Bits(n);
    var hi_bits = (n - low_32_bits) / (256 * 256 * 256 * 256);
    return [hi_bits, low_32_bits];
}

function BruceTopicTooLarge() {
    this.name = "BruceTopicTooLarge";
    this.message = "Kafka topic is too large";
}

BruceTopicTooLarge.prototype = Object.create(Error.prototype);
BruceTopicTooLarge.prototype.constructor = BruceTopicTooLarge;

function BruceMsgTooLarge() {
    this.name = "BruceMsgTooLarge";
    this.message = "Cannot create a message that large";
}

BruceMsgTooLarge.prototype = Object.create(Error.prototype);
BruceMsgTooLarge.prototype.constructor = BruceMsgTooLarge;

function BruceMsgCreator() {
    this.MSG_SIZE_FIELD_SIZE = 4;
    this.API_KEY_FIELD_SIZE = 2;
    this.API_VERSION_FIELD_SIZE = 2;
    this.FLAGS_FIELD_SIZE = 2;
    this.PARTITION_KEY_FIELD_SIZE = 4;
    this.TOPIC_SIZE_FIELD_SIZE = 2;
    this.TIMESTAMP_FIELD_SIZE = 8;
    this.KEY_SIZE_FIELD_SIZE = 4;
    this.VALUE_SIZE_FIELD_SIZE = 4;

    this.ANY_PARTITION_FIXED_BYTES = this.MSG_SIZE_FIELD_SIZE +
    this.API_KEY_FIELD_SIZE + this.API_VERSION_FIELD_SIZE +
    this.FLAGS_FIELD_SIZE + this.TOPIC_SIZE_FIELD_SIZE +
    this.TIMESTAMP_FIELD_SIZE + this.KEY_SIZE_FIELD_SIZE +
    this.VALUE_SIZE_FIELD_SIZE;

    this.PARTITION_KEY_FIXED_BYTES = this.ANY_PARTITION_FIXED_BYTES +
    this.PARTITION_KEY_FIELD_SIZE;

    this.ANY_PARTITION_API_KEY = 256;
    this.ANY_PARTITION_API_VERSION = 0;

    this.PARTITION_KEY_API_KEY = 257;
    this.PARTITION_KEY_API_VERSION = 0;
}

BruceMsgCreator.getMaxTopicSize = function () {
    /* This is the maximum topic size allowed by Kafka. */
    return 0x7fff;
}

BruceMsgCreator.getMaxMsgSize = function () {
    /* This is an extremely loose upper bound, based on the maximum value that
       can be stored in a 32-bit signed integer field.  The actual maximum is a
       much smaller value: the maximum UNIX domain datagram size supported by
       the operating system, which has been observed to be 212959 bytes on a
       CentOS 7 x86_64 system. */
    return 0x7fffffff;
}

BruceMsgCreator.prototype = {
    createAnyPartitionMsg: function (topic, timestamp, key, value) {
        if (topic.length > BruceMsgCreator.getMaxTopicSize()) {
            throw new BruceTopicTooLarge();
        }

        var msg_size = this.ANY_PARTITION_FIXED_BYTES + topic.length +
            key.length + Buffer.byteLength(value, 'utf8');

        if ((msg_size > BruceMsgCreator.getMaxMsgSize())) {
            throw new BruceMsgTooLarge();
        }

        var buf = new Buffer(msg_size);
        var offset = 0;
        buf.writeInt32BE(msg_size, offset);
        offset += this.MSG_SIZE_FIELD_SIZE;
        buf.writeUInt16BE(this.ANY_PARTITION_API_KEY, offset);
        offset += this.API_KEY_FIELD_SIZE;
        buf.writeUInt16BE(this.ANY_PARTITION_API_VERSION, offset);
        offset += this.API_VERSION_FIELD_SIZE;
        buf.writeUInt16BE(0, offset);  // flags
        offset += this.FLAGS_FIELD_SIZE;
        buf.writeInt16BE(topic.length, offset);
        offset += this.TOPIC_SIZE_FIELD_SIZE;
        buf.write(topic, offset);
        offset += topic.length;
        var pieces = BreakInt(timestamp);
        buf.writeUInt32BE(pieces[0], offset);
        offset += 4;
        buf.writeUInt32BE(pieces[1], offset);
        offset += 4;
        buf.writeInt32BE(key.length, offset);
        offset += this.KEY_SIZE_FIELD_SIZE;
        buf.write(key, offset);
        offset += key.length;
        buf.writeInt32BE(Buffer.byteLength(value, 'utf8'), offset);
        offset += this.VALUE_SIZE_FIELD_SIZE;
        buf.write(value, offset);
        return buf;
    },
    createPartitionKeyMsg: function (partition_key, topic, timestamp, key,
                                     value) {
        if (topic.length > BruceMsgCreator.getMaxTopicSize()) {
            throw new BruceTopicTooLarge();
        }
        var msg_size = this.PARTITION_KEY_FIXED_BYTES + topic.length +
            key.length + Buffer.byteLength(value, 'utf8');

        if ((msg_size > BruceMsgCreator.getMaxMsgSize())) {
            throw new BruceMsgTooLarge();
        }

        var buf = new Buffer(msg_size);
        var offset = 0;
        buf.writeInt32BE(msg_size, offset);
        offset += this.MSG_SIZE_FIELD_SIZE;
        buf.writeUInt16BE(this.PARTITION_KEY_API_KEY, offset);
        offset += this.API_KEY_FIELD_SIZE;
        buf.writeUInt16BE(this.PARTITION_KEY_API_VERSION, offset);
        offset += this.API_VERSION_FIELD_SIZE;
        buf.writeUInt16BE(0, offset);  // flags
        offset += this.FLAGS_FIELD_SIZE;
        buf.writeUInt32BE(partition_key, offset);
        offset += this.PARTITION_KEY_FIELD_SIZE;
        buf.writeInt16BE(topic.length, offset);
        offset += this.TOPIC_SIZE_FIELD_SIZE;
        buf.write(topic, offset);
        offset += topic.length;
        var pieces = BreakInt(timestamp);
        buf.writeUInt32BE(pieces[0], offset);
        offset += 4;
        buf.writeUInt32BE(pieces[1], offset);
        offset += 4;
        buf.writeInt32BE(key.length, offset);
        offset += this.KEY_SIZE_FIELD_SIZE;
        buf.write(key, offset);
        offset += key.length;
        buf.writeInt32BE(Buffer.byteLength(value, 'utf8'), offset);
        offset += this.VALUE_SIZE_FIELD_SIZE;
        buf.write(value, offset);
        return buf;
    }
};

var Bruce = new BruceMsgCreator();
var bruce_path = '/path/to/bruce/socket';
var topic = 'some topic';
var msg_key = '';
var msg_value = 'hello world';
var partition_key = 12345;

/* Create AnyPartition message */
var anyPartitionMessage = Bruce.createAnyPartitionMsg(topic, Date.now(),
        msg_key, msg_value);

/* Create PartitionKey message */
var partitionKeyMessage = Bruce.createPartitionKeyMsg(partition_key, topic,
        Date.now(), msg_key, msg_value);

var error = null;
var client = unix.createSocket('unix_dgram');

client.on('error', function (err) {
    console.error('error while connecting to bruce socket, closing connection',
            err);
    process.exit(0);
});

client.on('connect', function () {
    client.send(anyPartitionMessage);
    client.send(partitionKeyMessage);
    client.close();
});

client.connect(bruce_path);
