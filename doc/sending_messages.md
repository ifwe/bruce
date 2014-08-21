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

## Message Format for Sending to Bruce's Input Socket

Bruce supports two input message types: *AnyPartition* messages
and *PartitionKey* messages.  The difference between these two message types is
how a partition is chosen for topics with multiple partitions.  If you wish to
give Bruce full control over partition selection, then send an AnyPartition
message.  Bruce will distrubute AnyPartition messages across the partitions for
a topic using essentially a round-robin distribution scheme to balance the
load.

(more content will appear here shortly)

Once you are able to send messages to Bruce, you will probably be interested
in learning about its
[status monitoring interface](https://github.com/tagged/bruce#status-monitoring).
