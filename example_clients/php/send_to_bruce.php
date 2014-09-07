#!/usr/bin/env php

<?php

/* ----------------------------------------------------------------------------
   Copyright 2014 Tagged

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

    This is an example PHP script that sends messages to Bruce.
 */

/* According to the PHP documentation, the maximum value of an integer is
   platform-dependent.  On 32-bit systems, integers are 32 bits wide.  On
   64-bit systems, integers are 64 bits wide.  Bruce's representation of
   timestamps as milliseconds since the epoch requires more than 32 bits.  To
   guarantee that things work properly on both 32-bit and 64-bit systems, this
   timestamp generator class is provided.  It allows you to specify a timestamp
   as two components: a seconds part and a microseconds part.  Once a time has
   been specified, you can extract it as a pair of values, H and L, which are
   defined as follows:

       Let T be a 64-bit integer representation of the desired timestamp in
       milliseconds since the epoch.  Then H and L are 32-bit values such that
       H contains the high 32 bits of T, and L contains the low 32 bits of T.

   For instance, suppose T is 0xabcd123456.  Then H will be 0xab and L will be
   0xcd123456.  Although the result is in milliseconds, the second component of
   the input is provided in microseconds for easy interoperability with PHP's
   built in microtime() function. */
class BruceTimestampGenerator {
    /* This stores the "seconds" part of a timestamp. */
    private $epochSecPart = 0;

    /* This stores the "milliseconds" part of a timestamp. */
    private $epochMsecPart = 0;

    private function getSecHigh() {
        return ($this->epochSecPart & 0xffff0000) >> 16;
    }

    /* Set the internally stored time to the value specified by $seconds and
       $microseconds.  For instance, if the desired time in microseconds since
       the epoch is 2005000, then $seconds would be 2 and $microseconds would
       be 5000.  See the implementaion of setEpochTimeNow() below for an
       illustration of how to convert a result obtained from microtime() to a
       pair of parameter values for passing to this method. */
    public function setEpochTime($seconds, $microseconds) {
        $this->epochSecPart = $seconds;
        $this->epochMsecPart = intval($microseconds / 1000);
    }

    /* Set the internally stored time to the current time, as reported by
       microtime(). */
    public function setEpochTimeNow() {
        list($microseconds, $seconds) = explode(" ", microtime());
        $this->setEpochTime($seconds, intval($microseconds * 1000000));
    }

    /* Get the value H, as described above. */
    public function getEpochMsHigh() {
        return ((1000 * $this->getSecHigh()) & 0xffff0000) >> 16;
    }

    /* Get the value L, as described above. */
    public function getEpochMsLow() {
        $n = ((1000 * $this->getSecHigh()) & 0x0000ffff) << 16;
        return $n + (1000 * ($this->epochSecPart & 0x0000ffff)) +
                $this->epochMsecPart;
    }
}

/* Class for creating datagrams to send to Bruce. */
class BruceMsgCreator {
    private static $MSG_SIZE_FIELD_SIZE = 4;
    private static $API_KEY_FIELD_SIZE = 2;
    private static $API_VERSION_FIELD_SIZE = 2;
    private static $FLAGS_FIELD_SIZE = 2;
    private static $PARTITION_KEY_FIELD_SIZE = 4;
    private static $TOPIC_SIZE_FIELD_SIZE = 2;
    private static $TIMESTAMP_FIELD_SIZE = 8;
    private static $KEY_SIZE_FIELD_SIZE = 4;
    private static $VALUE_SIZE_FIELD_SIZE = 4;

    private static $ANY_PARTITION_FIXED_BYTES;

    private static $PARTITION_KEY_FIXED_BYTES;

    private static $ANY_PARTITION_API_KEY = 256;
    private static $ANY_PARTITION_API_VERSION = 0;

    private static $PARTITION_KEY_API_KEY = 257;
    private static $PARTITION_KEY_API_VERSION = 0;

    /* Initialize classwide state.  Call this before creating any instances. */
    static function init() {
        self::$ANY_PARTITION_FIXED_BYTES = self::$MSG_SIZE_FIELD_SIZE +
                self::$API_KEY_FIELD_SIZE + self::$API_VERSION_FIELD_SIZE +
                self::$FLAGS_FIELD_SIZE + self::$TOPIC_SIZE_FIELD_SIZE +
                self::$TIMESTAMP_FIELD_SIZE + self::$KEY_SIZE_FIELD_SIZE +
                self::$VALUE_SIZE_FIELD_SIZE;
        self::$PARTITION_KEY_FIXED_BYTES = self::$ANY_PARTITION_FIXED_BYTES +
                self::$PARTITION_KEY_FIELD_SIZE;
    }

    /* Create and return an AnyPartition message that is ready to send to
       Bruce.  $timestampHigh and $timestampLow should be values returned by
       methods getEpochMsHigh() and getEpochMsLow() of class
       BruceTimestampGenerator. */
    public function createAnyPartitionMsg($topic, $timestampHigh,
            $timestampLow, $key, $value) {
        $msgSize = self::$ANY_PARTITION_FIXED_BYTES + strlen($topic) +
                strlen($key) + strlen($value);
        $result = pack("Nnnnn", $msgSize, self::$ANY_PARTITION_API_KEY,
                self::$ANY_PARTITION_API_VERSION, 0, strlen($topic));
        $result = $result . $topic;
        $result = $result . pack("NNN", $timestampHigh, $timestampLow,
                                 strlen($key));
        $result = $result . $key;
        $result = $result . pack("N", strlen($value));
        $result = $result . $value;
        return $result;
    }

    /* Create and return a PartitionKey message that is ready to send to
       Bruce.  $timestampHigh and $timestampLow should be values returned by
       methods getEpochMsHigh() and getEpochMsLow() of class
       BruceTimestampGenerator. */
    public function createPartitionKeyMsg($partitionKey, $topic,
            $timestampHigh, $timestampLow, $key, $value) {
        $msgSize = self::$PARTITION_KEY_FIXED_BYTES + strlen($topic) +
                strlen($key) + strlen($value);
        $result = pack("NnnnNn", $msgSize, self::$PARTITION_KEY_API_KEY,
                self::$PARTITION_KEY_API_VERSION, 0, $partitionKey,
                strlen($topic));
        $result = $result . $topic;
        $result = $result . pack("NNN", $timestampHigh, $timestampLow,
                                 strlen($key));
        $result = $result . $key;
        $result = $result . pack("N", strlen($value));
        $result = $result . $value;
        return $result;
    }
}

// initialize BruceMsgCreator class
BruceMsgCreator::init();

$brucePath = "/path/to/bruce/socket";
$topic = "some topic";  // Kafka topic
$msgKey = "";
$msgValue = "hello world";
$partitionKey = 12345;
$g = new BruceTimestampGenerator;
$mc = new BruceMsgCreator;

// create AnyPartition message
$g->setEpochTimeNow();
$msg1 = $mc->createAnyPartitionMsg($topic, $g->getEpochMsHigh(),
        $g->getEpochMsLow(), $msgKey, $msgValue);

// create PartitionKey message
$g->setEpochTimeNow();
$msg2 = $mc->createPartitionKeyMsg($partitionKey, $topic, $g->getEpochMsHigh(),
        $g->getEpochMsLow(), $msgKey, $msgValue);

// create socket for sending to Bruce
if (($sock = socket_create(AF_UNIX, SOCK_DGRAM, 0)) === false) {
    print "Failed to create UNIX datagram socket\n";
    exit(1);
}

$tmp_filename = uniqid("/tmp/bruce_client_", true);

// bind socket to temporary filename
if (socket_bind($sock, $tmp_filename) === false) {
    print "Failed to bind UNIX datagram socket\n";
    socket_close($sock);
    exit(1);
}

// send AnyPartition message to Bruce
if (socket_sendto($sock, $msg1, strlen($msg1), 0, $brucePath) === false) {
    print "Failed to send to Bruce\n";
    socket_close($sock);
    unlink($tmp_filename);
    exit(1);
}

// send PartitionKey message to Bruce
if (socket_sendto($sock, $msg2, strlen($msg2), 0, $brucePath) === false) {
    print "Failed to send to Bruce\n";
    socket_close($sock);
    unlink($tmp_filename);
    exit(1);
}

// clean up
socket_close($sock);
unlink($tmp_filename);

?>
