/* ----------------------------------------------------------------------------
   Copyright 2014 if(we)

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

    This is an example Java class for creating messages to be sent to Bruce.
 */

package com.tagged.bruce.example_bruce_client;

import java.nio.charset.Charset;

public class DatagramCreator {
    private static final int SIZE_FIELD_SIZE = 4;
    private static final int API_KEY_FIELD_SIZE = 2;
    private static final int API_VERSION_FIELD_SIZE = 2;
    private static final int FLAGS_FIELD_SIZE = 2;
    private static final int PARTITION_KEY_FIELD_SIZE = 4;
    private static final int TOPIC_SIZE_FIELD_SIZE = 2;
    private static final int TIMESTAMP_FIELD_SIZE = 8;
    private static final int KEY_SIZE_FIELD_SIZE = 4;
    private static final int VALUE_SIZE_FIELD_SIZE = 4;

    private static final int ANY_PARTITION_FIXED_SIZE = SIZE_FIELD_SIZE +
            API_KEY_FIELD_SIZE + API_VERSION_FIELD_SIZE + FLAGS_FIELD_SIZE +
            TOPIC_SIZE_FIELD_SIZE + TIMESTAMP_FIELD_SIZE +
            KEY_SIZE_FIELD_SIZE + VALUE_SIZE_FIELD_SIZE;

    private static final int PARTITION_KEY_FIXED_SIZE =
            ANY_PARTITION_FIXED_SIZE + PARTITION_KEY_FIELD_SIZE;

    private static final short ANY_PARTITION_API_KEY = 256;
    private static final short PARTITION_KEY_API_KEY = 257;

    private static short ANY_PARTITION_API_VERSION = 0;
    private static short PARTITION_KEY_API_VERSION = 0;

    /* Write 'value' to array 'out' starting at array index 'offset'.  'value'
       is written in network byte oprder (big endian). */
    private static void serializeShortToByteArray(short value, byte[] out,
            int offset) {
        out[offset] = (byte) ((value & 0xff00) >> 8);
        out[offset + 1] = (byte) (value & 0x00ff);
    }

    /* Write 'value' to array 'out' starting at array index 'offset'.  'value'
       is written in network byte oprder (big endian). */
    private static void serializeIntToByteArray(int value, byte[] out,
            int offset) {
        out[offset] = (byte) ((value & 0xff000000) >> 24);
        out[offset + 1] = (byte) ((value & 0x00ff0000) >> 16);
        out[offset + 2] = (byte) ((value & 0x0000ff00) >> 8);
        out[offset + 3] = (byte) (value & 0x000000ff);
    }

    /* Write 'value' to array 'out' starting at array index 'offset'.  'value'
       is written in network byte oprder (big endian). */
    private static void serializeLongToByteArray(long value, byte[] out,
            int offset) {
        out[offset] = (byte) ((value & 0xff00000000000000L) >> 56);
        out[offset + 1] = (byte) ((value & 0x00ff000000000000L) >> 48);
        out[offset + 2] = (byte) ((value & 0x0000ff0000000000L) >> 40);
        out[offset + 3] = (byte) ((value & 0x000000ff00000000L) >> 32);
        out[offset + 4] = (byte) ((value & 0x00000000ff000000L) >> 24);
        out[offset + 5] = (byte) ((value & 0x0000000000ff0000L) >> 16);
        out[offset + 6] = (byte) ((value & 0x000000000000ff00L) >> 8);
        out[offset + 7] = (byte) (value & 0x00000000000000ffL);
    }

    /* Copy entire contents of 'src' to 'dst', starting at array index 'offset'
       in array 'dst'.  It is assumed that 'dst' is large enough to complete
       this operation without indexing past the end of the array. */
    private static void copyToByteArray(byte[] dst, byte[] src, int offset) {
        if (src != null) {
            for (int i = 0; i < src.length; ++i) {
                dst[offset + i] = src[i];
            }
        }
    }

    /* This is the maximum topic size allowed by Kafka. */
    private static int maxTopicSize() {
        return Short.MAX_VALUE;
    }

    /* This is an extremely loose upper bound, based on the maximum value that
       can be stored in a 32-bit signed integer field.  The actual maximum is a
       much smaller value: the maximum UNIX domain datagram size supported by
       the operating system, which has been observed to be 212959 bytes on a
       CentOS 7 x86_64 system. */
    private static int maxMsgSize() {
        return Integer.MAX_VALUE;
    }

    public class TopicTooLong extends Exception {
        public TopicTooLong() {
            super();
        }
    }

    public class DatagramTooLarge extends Exception {
        public DatagramTooLarge() {
            super();
        }
    }

    /* Create an AnyPartition message with given topic, timestamp, key, and
       value.  Return the resulting message as an array of bytes.  The result
       can then be sent to Bruce's UNIX domain datagram socket. */
    public byte[] createAnyPartitionDatagram(String topic, long timestamp,
            byte[] key, byte[] value) throws TopicTooLong, DatagramTooLarge {
        byte[] topicBytes = topic.getBytes(Charset.forName("UTF-8"));

        if (topicBytes.length > maxTopicSize()) {
            throw new TopicTooLong();
        }

        int keyLength = (key == null) ? 0 : key.length;
        int valueLength = (value == null) ? 0 : value.length;
        long dgSizeLong = ANY_PARTITION_FIXED_SIZE +
                ((long) topicBytes.length) + ((long) keyLength) +
                ((long) valueLength);

        if (dgSizeLong > maxMsgSize()) {
            throw new DatagramTooLarge();
        }

        int dgSize = (int) dgSizeLong;
        byte[] result = new byte[dgSize];
        int pos = 0;
        serializeIntToByteArray(dgSize, result, pos);
        pos += SIZE_FIELD_SIZE;
        serializeShortToByteArray(ANY_PARTITION_API_KEY, result, pos);
        pos += API_KEY_FIELD_SIZE;
        serializeShortToByteArray(ANY_PARTITION_API_VERSION, result, pos);
        pos += API_VERSION_FIELD_SIZE;
        serializeShortToByteArray((short) 0, result, pos);
        pos += FLAGS_FIELD_SIZE;
        serializeShortToByteArray((short) topic.length(), result, pos);
        pos += TOPIC_SIZE_FIELD_SIZE;
        copyToByteArray(result, topicBytes, pos);
        pos += topicBytes.length;
        serializeLongToByteArray(timestamp, result, pos);
        pos += TIMESTAMP_FIELD_SIZE;
        serializeIntToByteArray(keyLength, result, pos);
        pos += KEY_SIZE_FIELD_SIZE;
        copyToByteArray(result, key, pos);
        pos += keyLength;
        serializeIntToByteArray(valueLength, result, pos);
        pos += VALUE_SIZE_FIELD_SIZE;
        copyToByteArray(result, value, pos);
        return result;
    }

    /* Create a PartitionKey message with given partition key, topic,
       timestamp, key, and value.  Return the resulting message as an array of
       bytes.  The result can then be sent to Bruce's UNIX domain datagram
       socket. */
    public byte[] createPartitionKeyDatagram(int partitionKey, String topic,
            long timestamp, byte[] key, byte[] value)
                    throws TopicTooLong, DatagramTooLarge {
        byte[] topicBytes = topic.getBytes(Charset.forName("UTF-8"));

        if (topicBytes.length > maxTopicSize()) {
            throw new TopicTooLong();
        }

        int keyLength = (key == null) ? 0 : key.length;
        int valueLength = (value == null) ? 0 : value.length;
        long dgSizeLong = PARTITION_KEY_FIXED_SIZE +
                ((long) topicBytes.length) + ((long) keyLength) +
                ((long) valueLength);

        if (dgSizeLong > maxMsgSize()) {
            throw new DatagramTooLarge();
        }

        int dgSize = (int) dgSizeLong;
        byte[] result = new byte[dgSize];
        int pos = 0;
        serializeIntToByteArray(dgSize, result, pos);
        pos += SIZE_FIELD_SIZE;
        serializeShortToByteArray(PARTITION_KEY_API_KEY, result, pos);
        pos += API_KEY_FIELD_SIZE;
        serializeShortToByteArray(PARTITION_KEY_API_VERSION, result, pos);
        pos += API_VERSION_FIELD_SIZE;
        serializeShortToByteArray((short) 0, result, pos);
        pos += FLAGS_FIELD_SIZE;
        serializeIntToByteArray(partitionKey, result, pos);
        pos += PARTITION_KEY_FIELD_SIZE;
        serializeShortToByteArray((short) topic.length(), result, pos);
        pos += TOPIC_SIZE_FIELD_SIZE;
        copyToByteArray(result, topicBytes, pos);
        pos += topicBytes.length;
        serializeLongToByteArray(timestamp, result, pos);
        pos += TIMESTAMP_FIELD_SIZE;
        serializeIntToByteArray(keyLength, result, pos);
        pos += KEY_SIZE_FIELD_SIZE;
        copyToByteArray(result, key, pos);
        pos += keyLength;
        serializeIntToByteArray(valueLength, result, pos);
        pos += VALUE_SIZE_FIELD_SIZE;
        copyToByteArray(result, value, pos);
        return result;
    }
}
