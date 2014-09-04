#!/usr/bin/env python

import io
import socket
import struct


class BruceMsgCreator(object):
    '''Class for creating datagrams to be sent to Bruce.'''

    MSG_SIZE_FIELD_SIZE = 4
    API_KEY_FIELD_SIZE = 2
    API_VERSION_FIELD_SIZE = 2
    FLAGS_FIELD_SIZE = 2
    PARTITION_KEY_FIELD_SIZE = 4
    TOPIC_SIZE_FIELD_SIZE = 2
    TIMESTAMP_FIELD_SIZE = 8
    KEY_SIZE_FIELD_SIZE = 4
    VALUE_SIZE_FIELD_SIZE = 4

    ANY_PARTITION_FIXED_BYTES = MSG_SIZE_FIELD_SIZE + API_KEY_FIELD_SIZE + \
            API_VERSION_FIELD_SIZE + FLAGS_FIELD_SIZE + \
            TOPIC_SIZE_FIELD_SIZE + TIMESTAMP_FIELD_SIZE + \
            KEY_SIZE_FIELD_SIZE + VALUE_SIZE_FIELD_SIZE

    PARTITION_KEY_FIXED_BYTES = ANY_PARTITION_FIXED_BYTES + \
            PARTITION_KEY_FIELD_SIZE

    ANY_PARTITION_API_KEY = 256
    ANY_PARTITION_API_VERSION = 0

    PARTITION_KEY_API_KEY = 257
    PARTITION_KEY_API_VERSION = 0

    def __init__(self):
        pass

    def create_any_partition_msg(self, topic, timestamp, key_bytes,
            value_bytes):
        topic_bytes = bytes(topic)
        msg_size = BruceMsgCreator.ANY_PARTITION_FIXED_BYTES + \
                len(topic_bytes) + len(key_bytes) + len(value_bytes)
        buf = io.BytesIO()
        flags = 0
        buf.write(struct.pack('>ihhhh', msg_size,
                BruceMsgCreator.ANY_PARTITION_API_KEY,
                BruceMsgCreator.ANY_PARTITION_API_VERSION, flags,
                len(topic_bytes)))
        buf.write(topic_bytes)
        buf.write(struct.pack('>qi', timestamp, len(key_bytes)))
        buf.write(key_bytes)
        buf.write(struct.pack('>i', len(value_bytes)))
        buf.write(value_bytes)
        result_bytes = buf.getvalue()
        buf.close()
        return result_bytes

    def create_partition_key_msg(self, partition_key, topic, timestamp, \
            key_bytes, value_bytes):
        topic_bytes = bytes(topic)
        msg_size = BruceMsgCreator.PARTITION_KEY_FIXED_BYTES + \
                len(topic_bytes) + len(key_bytes) + len(value_bytes)
        buf = io.BytesIO()
        flags = 0
        buf.write(struct.pack('>ihhhih', msg_size,
                BruceMsgCreator.PARTITION_KEY_API_KEY,
                BruceMsgCreator.PARTITION_KEY_API_VERSION, flags,
                partition_key, len(topic_bytes)))
        buf.write(topic_bytes)
        buf.write(struct.pack('>qi', timestamp, len(key_bytes)))
        buf.write(key_bytes)
        buf.write(struct.pack('>i', len(value_bytes)))
        buf.write(value_bytes)
        result_bytes = buf.getvalue()
        buf.close()
        return result_bytes


bruce_path = '/path/to/bruce/socket'
topic = 'some topic'  # Kafka topic
msg_key = ''
msg_value = 'hello world'
timestamp = 0  # should be current time in milliseconds since the epoch
partition_key = 12345

mc = BruceMsgCreator()

# Create AnyPartition message.
any_partition_msg = mc.create_any_partition_msg(topic, timestamp,
        bytes(msg_key), bytes(msg_value))

# Create PartitionKey message.
partition_key_msg = mc.create_partition_key_msg(partition_key, topic,
        timestamp, bytes(msg_key), bytes(msg_value))

# Create socket for sending to Bruce.
bruce_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)

# Send AnyPartition message to Bruce.
bruce_sock.sendto(any_partition_msg, bruce_path)

# Send PartitionKey message to Bruce.
bruce_sock.sendto(partition_key_msg, bruce_path)

bruce_sock.close()
