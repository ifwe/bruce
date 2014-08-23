## Design Overview

Bruce's core implementation consists of an input thread, a router thread, and a
message dispatcher that maintains a pair of threads for each Kafka broker: a
send thread for sending produce requests and a receive thread for receiving
produce responses.  There are also threads created by a third party HTTP server
library that implements Bruce's status monitoring interface and a main thread
that starts the input thread and waits for a shutdown signal.  The input thread
creates and monitors Bruce's input socket for messages from clients.  The
router thread starts and manages the dispatcher threads.

### Input Thread

The input thread's behavior is designed to be as simple as possible so it can
respond immediately to messages on its input socket.  This is important to
prevent messages from queueing up to the point where clients get blocked trying
to write.  More complex and possibly time-consuming behaviors are delegated to
the router thread and dispatcher threads.  When Bruce starts up, it
preallocates a fixed but configurable amount of memory for storing message
content.  When a message arrives on its input socket, the input thread attempts
to allocate memory from this pool to store the message contents.  If the pool
does not contain enough free memory, Bruce will discard the message.  All
messages discarded for any reason are tracked and reported through Bruce's web
interface, as described
[here](https://github.com/tagged/bruce/blob/master/doc/status_monitoring.md#discard-reporting).  On successful message creation, Bruce queues the message for
processing by the router thread and continues monitoring its input socket.

Communication between Bruce and its clients is purely one-way in nature.
Clients write messages as individual datagrams to Bruce's input socket and the
input thread reads the messages.  There is no need for clients to wait for an
ACK, since the operating system provides the same reliability guarantee for
UNIX domain sockets as for other local IPC mechanisms such as pipes.  Note that
this differs from UDP datagrams, which do not provide a reliable delivery
guarantee.

### Router Thread

The router thread's primary responsibility is to route messages to Kafka
brokers based on Metadata received from Kafka.  The Metadata provides
information such as the set of brokers in the cluster, a list of known topics,
a list of partitions for each topic along with which brokers store their
replicas, and status information for the brokers, topics, and partitions.
Bruce uses this information to choose a partition for each message and send it
to the proper broker.  As documented
[here](https://github.com/tagged/bruce/blob/master/doc/sending_messages.md#message-types),
Bruce provides two different message types, *AnyPartition* messages and
*PartitionKey* messages, which implement different types of routing behavior.
The Router thread also shares responsibility for message batching with the
dispatcher threads, as detailed below.

(more content will be added soon)
