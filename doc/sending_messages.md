## Using the Simple Command Line Client

The quickest way to get started sending messages to Bruce is by using the
simple command line client.  This client is included in Bruce's RPM package,
and can be built separately as described
[here](https://github.com/tagged/bruce/blob/master/doc/build_install.md#building-bruce-directly).
Sending a message to Bruce can then be done as follows:

```
simple_bruce_client --socket_path /var/run/bruce/bruce.socket \
        --topic test_topic --value "hello world"
```

A full listing of the simple client's command line options may be obatined by
typing `simple_bruce_client --help`.

## Other Clients

Example client code for writing to Bruce's input socket in Java, Python, PHP,
and C will soon be available.  For example C++ code, you can examine and borrow
code from the implementation of `simple_bruce_client`.  A client library that C
and C++ clients can link to should be available soon.  Client code for other
programming languages is needed.  Contributions from the community would be
much appreciated.

## Message Types

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

## Message Formats

Here, low-level details are presented for the message formats that Bruce
expects to receive from its UNIX domain datagram socket.  The same notation
described [here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
is used below.

### AnyPartition Message Format

(details will appear here shortly)

### PartitionKey Message Format

(details will appear here shortly)

Once you are able to send messages to Bruce, you will probably be interested
in learning about its
[status monitoring interface](https://github.com/tagged/bruce#status-monitoring).
