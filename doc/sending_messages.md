## Sending Messages

### Using the Simple Command Line Client

The quickest way to get started sending messages to Bruce is by using the
simple command line client.  This client is included in Bruce's RPM package,
and can be built separately as described
[here](build_install.md#building-bruces-client-library).  Sending a message to
Bruce can then be done as follows:

```
simple_bruce_client --socket_path /var/run/bruce/bruce.socket \
        --topic test_topic --value "hello world"
```

A full listing of the simple client's command line options may be obatined by
typing `simple_bruce_client --help`.

### Other Clients

Example client code for sending messages to Bruce in various programming
languages may be found in the [example_clients](../example_clients) directory
of Bruce's Git repository.  Community contributions for additional programming
languages are much appreciated.

### Message Types

Bruce supports two input message types: *AnyPartition* messages
and *PartitionKey* messages.  The difference between these two message types is
how a partition is chosen for topics with multiple partitions.

If you wish to give Bruce full control over partition selection, then send an
AnyPartition message.  Bruce will distrubute AnyPartition messages across the
partitions for a topic using essentially a round-robin distribution scheme to
balance the load.

The PartitionKey message type provides the sender with greater control over
which partition a message is sent to.  When sending this type of message, you
specify a 32-bit partition key.  Bruce then chooses a partition by applying a
mod function to the key and using it as an index into an array of available
partitions.  For instance, suppose that messages for topic T can be sent to
partitions 1, 3, and 5.  Then Bruce's available partition array for T will look
like this:

```
{1, 3, 5}
```

For key K, Bruce then chooses (K % 3) as the array index of the partition to
use.  For instance, if K is 6, then array index 0 will be chosen.  Since
partition ID 1 is at position 0, a message with key K will be sent to partition
1.  In practice, this might be useful in a scenario where messages are
associated with users of a web site, and all messages associated with a given
user need to be sent to the same partition.  In this case, a hash of the user
ID can be used as a partition key.  The above-mentioned partition array is
guaranteed to be sorted in ascending order.  Therefore, a sender with full
knowledge of the partition layout for a given topic can use the partition key
mechanism to directly choose a partition.  However, one word of caution is
necessary.  If a partition becomes temporarily unavailable, then Bruce will
fetch new metadata, and the array of available partitions will change.  For
instance, if partition 3 in the above example becomes unavailable, then the
array of available partitions becomes `{1, 5}`, and the mapping function from
partition key to partition therefore changes.

### Message Formats

Here, low-level details are presented for the message formats that Bruce
expects to receive from its UNIX domain datagram socket.  The same notation
described [here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
is used below.

#### Generic Message Format

```
GenericMessage => Size ApiKey ApiVersion Message

Size => int32
ApiKey => int16
ApiVersion => int16
Message => AnyPartitionMessage | PartitionKeyMessage
```

Field Descriptions:
* `Size`: This is the size in bytes of the entire message, including the `Size`
field.
* `ApiKey`: This identifies a particular message type.  Currently, the only
message types are AnyPartition and PartitionKey.  A value of 256 identifies an
AnyPartition message and a value of 257 identifies a PartitionKey message.
* `ApiVersion`: This identifies the version of a given message type.  The
current version is 0 for both AnyPartition and PartitionKey messages.
* `Message`: This is the data for the message format identified by `ApiKey` and
`ApiVersion`.

#### AnyPartition Message Format

```
AnyPartitionMessage => Flags TopicSize Topic Timestamp KeySize Key ValueSize
        Value

Flags => int16
TopicSize => int16
Topic => array of TopicSize bytes
Timestamp => int64
KeySize => int32
Key => array of KeySize bytes
ValueSize => int32
Value => array of ValueSize bytes
```

Field Descriptions:
* `Flags`: Currently this value must be 0.
* `TopicSize`: This is the size in bytes of the topic.  It must be > 0, since
the topic must be nonempty.
* `Topic`: This is the Kafka topic that the message will be sent to.
* `Timestamp`: This is a timestamp for the message, represented as milliseconds
since the epoch (January 1 1970 12:00am UTC).
* `KeySize`: This is the size in bytes of the key for the message, as described
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets).
For an empty key, 0 must be specified.
* `Key`: This is the message key.
* `ValueSize`: This is the size in bytes of the value for the message, as
described
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets).
* `Value`: This is the message value.

#### PartitionKey Message Format

```
PartitionKeyMessage => Flags PartitionKey TopicSize Topic Timestamp KeySize Key
        ValueSize Value

Flags => int16
PartitionKey => int32
TopicSize => int16
Topic => array of TopicSize bytes
Timestamp => int64
KeySize => int32
Key => array of KeySize bytes
ValueSize => int32
Value => array of ValueSize bytes
```

Field Descriptions:
* `Flags`: Currently this value must be 0.
* `PartitionKey`: This is the partition key, as described above, which is used
for partition selection.
* `TopicSize`: This is the size in bytes of the topic.  It must be > 0, since
the topic must be nonempty.
* `Topic`: This is the Kafka topic that the message will be sent to.
* `Timestamp`: This is a timestamp for the message, represented as milliseconds
since the epoch (January 1 1970 12:00am UTC).
* `KeySize`: This is the size in bytes of the key for the message, as described
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets).
For an empty key, 0 must be specified.
* `Key`: This is the message key.
* `ValueSize`: This is the size in bytes of the value for the message, as
described
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets).
* `Value`: This is the message value.

Notice that the PartitionKey format is identical to the AnyPartition format
except for the presence of the `PartitionKey` field.

Once you are able to send messages to Bruce, you will probably be interested
in learning about its
[status monitoring interface](../README.md#status-monitoring).

-----

sending_messages.md: Copyright 2014 if(we), Inc.

sending_messages.md is licensed under a Creative Commons Attribution-ShareAlike
4.0 International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
