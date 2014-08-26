## Design Overview

Bruce's core implementation consists of an input thread, a router thread, and a
message dispatcher that maintains a pair of threads for each Kafka broker: a
send thread for sending produce requests and a receive thread for receiving
produce responses.  There are also threads created by a third party HTTP server
library that implements Bruce's status monitoring interface and a main thread
that starts the input thread and waits for a shutdown signal.  The input thread
creates and monitors Bruce's input socket for messages from clients.  It also
starts the router thread during system initialization.  The router thread
starts and manages the dispatcher threads.

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
brokers based on metadata received from Kafka.  The metadata provides
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

Once the router thread has chosen a broker for a message or batch of messages,
it queues the message(s) for receipt by the corresponding dispatcher send
thread.  The router thread monitors the dispatcher for conditions referred to
as *pause events*.  These occur due to socket-related errors and certain types
of error ACKs which indicate that the metadata is likely no longer accurate.
On detection of a pause event, the router thread waits for the dispatcher
threads to shut down and extracts all messages from the dispatcher.  It then
fetches new metadata, starts new dispatcher threads, and reroutes the extracted
messages based on the new metadata.  The router thread also periodically
refreshes its metadata and responds to user-initiated metadata update requests.
In these cases, it fetches new metadata, which it compares with the existing
metadata.  If the new metadata differs, it shuts down the dispatcher threads
and then proceeds in a manner similar to the handling of a pause event.

### Dispatcher

The dispatcher opens a TCP connection to each Kafka broker that serves as
leader for at least one currently available partition.  As described above,
each connection is serviced by a send thread and a receive thread.  A pause
event initiated by any dispatcher thread will alert the router thread and cause
all of the other dispatcher threads to shut down.  As detailed below,
responsibility for message batching is divided between the dispatcher threads
and the router thread, according to the types of messages being sent and how
batching is configured.  Compression is handled completely by the dispatcher
send threads, which are also responsible for assembling message batches into
produce requests and doing final partition selection as described in the
section on batching below.

An error ACK received from Kafka will cause the receive thread
that got the ACK to respond in one of four ways:

1. Queue the corresponding message set to be resent to the same broker by the
   send thread.
2. Discard the corresponding message set and continue processing ACKs.
3. Discard the corresponding message set and initiate a pause event.
4. Initiate a pause event without discarding the corresponding message set.  In
   this case, the router thread will collect and reroute the messages once it
   has updated the metadata and restarted the dispatcher.

To see which types of error ACKs cause wich types of responses, look in
`src/bruce/kafka_proto/v0/wire_proto.cc`.  Socket-related errors cause the
fourth type of response above.  Additionally, when a response of type 4 occurs
due to an error ACK, a failed delivery attempt count is incremented for each
message in the corresponding message set.  Once a message's failed delivery
attempt count exceeds a certain configurable threshold, the message is
discarded.

### Message Batching

Batching is configurable on a per-topic basis.  Specifically, topics can be
configured with individual batching thresholds that consist of any combination
of the following limits:

- Maximum batching delay, specified in milliseconds.
- Maximum combined message data size, specified in bytes.
- Maximum message count.

Batching can also be configured so that topics without individual batching
configurations are grouped together into combined (mixed) topic batches subject
to configurable limits of the same types that govern per-topic batching.  It is
also possible to specify that batching for certain topics is completely
disabled, although this is not recommended due to performance considerations.

When a batch is completed, it is queued for transmission by the send thread
connected to the destination broker.  While a send thread is busy sending a
produce request, multiple batches may arrive in its queue.  When it finishes
sending, it will then try to combine all queued batches into the next produce
request, up to a configurable data size limit.

#### Batching of AnyPartition Messages

For AnyPartition messages, per-topic batching is done by the router thread.
When a batch for a topic is complete, Bruce chooses a destination broker for
the batch as follows.  For each topic, Bruce maintains an array of available
partitions.  An index in this array is chosen in a round-robin manner, and then
the broker that hosts the lead replica for that partition is chosen as the
destination.  However, at this point only the destination broker is determined,
and the batch may ultimately be sent to a different partition hosted by the
same broker.  Final partition selection is done by the send thread for the
destination broker while it is building a produce request.  In this manner,
batches for a given topic are sent to brokers proportionally according to how
many partitions for the topic each broker hosts.  For instance, suppose topic T
has a total of 10 partitions.  If 3 of the partitions are hosted by broker B1
and 7 are hosted by broker B2, then 30% of the batches for topic T will be sent
to B1 and 70% will be sent to B2.

Combined topics batching (in which a single batch contains multiple topics) can
be configured for topics that do not have per-topic batching configurations.
For AnyPartition messages with these topics, a broker is chosen in the same
manner as described above for per-topic batches.  However, this type of message
is batched at the broker level after the router thread has chosen a destination
broker and transferred the message to the dispatcher.

#### Batching of PartitionKey Messages

#### Produce Request Creation and Final Partition Selection

(more content will be added soon)
