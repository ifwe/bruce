## Design Overview

Bruce's core implementation consists of an input thread, a router thread, and a
message dispatcher that maintains a pair of threads for each Kafka broker: a
send thread for sending produce requests and a receive thread for receiving
produce responses.  There are also threads created by a third party HTTP server
library that implements Bruce's status monitoring interface and a main thread
that starts the input thread and waits for a shutdown signal.  The input thread
creates Bruce's input socket and monitors it for messages from clients.  It
also starts the router thread during system initialization.  The router thread
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
interface, as described [here](status_monitoring.md#discard-reporting).  On
successful message creation, the input thread queues the message for processing
by the router thread and continues monitoring its input socket.

Communication between Bruce and its clients is purely one-way in nature.
Clients write messages as individual datagrams to Bruce's input socket and the
input thread reads the messages.  There is no need for clients to wait for an
ACK, since the operating system provides the same reliability guarantee for
UNIX domain sockets as for other local IPC mechanisms such as pipes.  Note that
this differs from UDP datagrams, which do not provide a reliable delivery
guarantee.  Like their network-oriented counterparts, UNIX domain sockets exist
in both datagram and stream variants.  The datagram option was chosen for Bruce
because of the greater simplicity of the programming model on both client and
server ends.  However, a limitation of this approach is the maximum size of a
single datagram, which has been found to be 212959 bytes on a CentOS 7 x86_64
system.  This limit increases to 425951 bytes if clients are willing to
increase SO_SNDBUF above the default value, although this may be inconvenient
for many.  Hopefully the lower limit will be adequate for most users.  If not,
optional support for stream sockets can be implemented as a future extension.

### Router Thread

The router thread's primary responsibility is to route messages to Kafka
brokers based on metadata obtained from Kafka.  The metadata provides
information such as the set of brokers in the cluster, a list of known topics,
a list of partitions for each topic along with the locations of the replicas
for each partition, and status information for the brokers, topics, and
partitions.  Bruce uses this information to choose a destination broker and
partition for each message.  If Bruce receives a message for an unknown topic,
it will discard the message unless automatic topic creation is enabled.  In the
case where automatic topic creation is enabled, Bruce first sends a single
topic metadata request to one of the brokers, which is the mechanism for
requesting creation of a new topic.  Assuming that a response indicating
success is received, Bruce then does a complete refresh of its metadata, so
that the metadata shows information about the new topic, and then handles the
message as usual.  As documented [here](sending_messages.md#message-types),
Bruce provides two different message types, *AnyPartition* messages and
*PartitionKey* messages, which implement different types of routing behavior.
The Router thread shares responsibility for message batching with the
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
all of the dispatcher threads to shut down.  As detailed below, responsibility
for message batching is divided between the dispatcher threads and the router
thread, according to the type of message being sent and how batching is
configured.  Compression is handled completely by the dispatcher send threads,
which are also responsible for assembling message batches into produce requests
and doing final partition selection as described in the section on batching
below.

An error ACK received from Kafka will cause the receive thread that got the ACK
to respond in one of four ways:

1. *Resend*: Queue the corresponding message set to be resent to the same
   broker by the send thread.
2. *Discard*: Discard the corresponding message set and continue processing
   ACKs.
3. *Discard and Pause*: Discard the corresponding message set and initiate a
   pause event.
4. *Pause*: Initiate a pause event without discarding the corresponding message
   set.  In this case, the router thread will collect the messages and reroute
   them once it has updated the metadata and restarted the dispatcher.

The Kafka error ACK values are documented
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes).
The following table summarizes which response Bruce implements for each type of
error ACK:

| Error                      | Code | Response from Bruce |
|:---------------------------|-----:|:--------------------|
| Unknown                    |   -1 | Discard             |
| OffsetOutOfRange           |    1 | Discard             |
| InvalidMessage             |    2 | Resend              |
| UnknownTopicOrPartition    |    3 | Discard and Pause   |
| InvalidMessageSize         |    4 | Discard             |
| LeaderNotAvailable         |    5 | Pause               |
| NotLeaderForPartition      |    6 | Pause               |
| RequestTimedOut            |    7 | Pause               |
| BrokerNotAvailable         |    8 | Pause               |
| ReplicaNotAvailable        |    9 | Pause               |
| MessageSizeTooLarge        |   10 | Discard             |
| StaleControllerEpochCode   |   11 | Discard             |
| OffsetMetadataTooLargeCode |   12 | Discard             |
| (all other values)         |    ? | Discard             |

Feedback from the Kafka community regarding these choices is welcomed.  If a
different response for a given error code would be more appropriate, changes
can easily be made.

Additionally, socket-related errors cause a response of type 4.  When a
response of type 4 occurs specifically due to an error ACK, a failed delivery
attempt count is incremented for each message in the corresponding message set.
Once a message's failed delivery attempt count exceeds a certain configurable
threshold, the message is discarded.

### Message Batching

Batching is configurable on a per-topic basis.  Specifically, topics may be
configured with individual batching thresholds that consist of any combination
of the following limits:

- Maximum batching delay, specified in milliseconds.  This threshold is
triggered when the age of the oldest message
[timestamp](sending_messages.md#message-formats) in the batch is at least the
specified value.
- Maximum combined message data size, specified in bytes.  This includes only
the sizes of the [keys and values](sending_messages.md#message-formats).
- Maximum message count.

Batching may also be configured so that topics without individual batching
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
When a batch for a topic is complete, Bruce chooses a destination broker as
follows.  For each topic, Bruce maintains an array of available partitions.  An
index in this array is chosen in a round-robin manner, and the broker that
hosts the lead replica for that partition is chosen as the destination.
However, at this point only the destination broker is determined, and the batch
may ultimately be sent to a different partition hosted by the same broker.
Final partition selection is done by the send thread for the destination broker
as a produce request is being built.  In this manner, batches for a given topic
are sent to brokers proportionally according to how many partitions for the
topic each broker hosts.  For instance, suppose topic T has a total of 10
partitions.  If 3 of the partitions are hosted by broker B1 and 7 are hosted by
broker B2, then 30% of the batches for topic T will be sent to B1 and 70% will
be sent to B2.

Combined topics batching (in which a single batch contains multiple topics) may
be configured for topics that do not have per-topic batching configurations.
For AnyPartition messages with these topics, a broker is chosen in the same
manner as described above for per-topic batches.  However, this type of message
is batched at the broker level after the router thread has chosen a destination
broker and transferred the message to the dispatcher.

#### Batching of PartitionKey Messages

For PartitionKey messages, the chosen partition is determined by the partition
key which the client provides along with the message, as documented
[here](sending_messages.md#message-types).  Since the partition determines the
destination broker, per-topic batching of PartitionKey messages is done at the
broker level after the router thread has transferred a message to the
dispatcher.  All other aspects of batching for PartitionKey messages operate in
the same manner as for AnyPartition messages.  In the case of combined topics
batching, a single batch may contain a mixture of AnyPartition and PartitionKey
messages.

#### Produce Request Creation and Final Partition Selection

As mentioned above, the send thread for a broker may combine the contents of
multiple batches (possibly a mixture of per-topic batches of AnyPartition
messages, per-topic batches of PartitionKey messages, and combined topics
batches which may contain both types of messages) into a single produce
request.  To prevent creation of arbitrarily large produce requests, a
configurable upper bound on the combined size of all message content in a
produce request is implemented.  Enforcement of this limit may result in a
subset of the contents of a particular batch being included in a produce
request, with the remaining batch contents left for inclusion in the next
produce request.

Once the send thread has determined which messages to include in a produce
request, it must assign partitions to AnyPartition messages and group the
messages together by topic and partition.  First the messages are grouped by
topic, so that if a produce request contains messages from multiple batches,
all messages for a given topic are grouped together regardless of which batch
they came from.  Then partitions are assigned to AnyPartition messages, and
messages within a topic are grouped by partition into message sets.

For a given topic within a produce request, all AnyPartition messages are
assigned to the same partition.  To facilitate choosing a partition, the send
thread for a given broker has access to an array of available partitions for
the topic such that the lead replica for each partition is located at the
broker.  The send thread chooses a partition by cycling through this vector in
a round-robin manner.  For instance, suppose that topic T has available
partitions { P1, P2, P3, P4, P5 }.  Suppose that the lead replica for each of
{ P1, P3, P4 } is hosted by broker B, while the lead replica for each of the
other partitions is hosted on some other broker.  Then for a given produce
request, the send thread for B will choose one of { P1, P3, P4 } as the
assigned partition for all AnyPartition messages with topic T.  Let's assume
that partition P3 is chosen.  Then P4 will be chosen for the next produce
request containing AnyPartition messages for T, P1 will be chosen next, and the
send thread will continue cycling through the array in a round-robin manner.
For a given topic within a produce request, once a partition is chosen for all
AnyPartition messages, the messages are grouped into a single message set along
with all PartitionKey messages that map to the chosen partition.  PartitionKey
messages for T that map to other partitions are grouped into additional message
sets.

### Message Compression

Kafka supports compression of individual message sets.  To send a compressed
message set, a producer such as Bruce is expected to encapsulate the compressed
data inside a single message with a header field set to indicate the type of
compression.  Bruce allows compression to be configured on a per-topic basis.
Although Bruce was designed to support multiple compression types, only Snappy
compression is currently implemented.  Support for new compression types can
easily be added with minimal changes to Bruce's core implementation.

Bruce may be configured to skip compression of message sets whose uncompressed
sizes are below a certain limit, since compression of small amounts of data may
not be worthwhile.  Bruce may also be configured to compute the ratio of
compressed to uncompressed message set size, and send an individual message set
uncompressed if this ratio is above a certain limit.  This prevents the brokers
from wasting CPU cycles dealing with message sets that compress poorly.  The
intended use case for this behavior is situations where most messages compress
well enough for compression to be worthwhile, but there are occasional message
sets that compress poorly.  It is best to disable compression for topics that
consistently compress poorly, so Bruce avoids wasting CPU cycles on useless
compression attempts.  Future work is to add compression statistics reporting
to Bruce's web interface, so topics that compress poorly can easily be
identified.  An additional possibility is to make Bruce smart enough to learn
through experience which topics compress well, although this may be more effort
than it is worth.

Kafka places an upper bound on the size of a single message.  To prevent this
limit from being exceeded by a message that encapsulates a large compressed
message set, Bruce limits the size of a produce request so that the
uncompressed size of each individual message set it contains does not exceed
the limit.  Although it's possible that the compressed size of a message set is
within the limit while the uncompressed size exceeds it, basing enforcement of
the limit on uncompressed size is simple and avoids wasting CPU cycles on
message sets that are found to still exceed the limit after compression.

### Message Rate Limiting

Bruce provides optional message rate limiting on a per-topic basis.  The
motivation is to deal with situations in which buggy client code sends an
unreasonably large volume of messages to some topic T. Without rate limiting,
this might stress the Kafka cluster to the point where it can no longer keep up
with the message volume. The result is likely to be slowness in message
processing that affects many topics, causing Bruce to discard messages across
many topics.  The goal of rate limiting is to contain the damage by discarding
excess messages for topic T, preventing the Kafka cluster from becoming
overwhelmed and forcing Bruce to discard messages for other topics.  To specify
a rate limiting configuration, you provide an interval length in milliseconds
and a maximum number of messages for a given topic that should be allowed
within an interval of that length.  Bruce implements rate limiting by assigning
its own internal timestamps to messages as they are created using a clock that
increases monotonically, and is guaranteed to be unaffected by changes made to
the system wall clock.  Therefore there is no danger of messages being
erroneously discarded by the rate limiting mechanism if the system clock is set
back.  As with all other types of discards, messages discarded by the rate
limiting mechanism will be included in Bruce's discard reports.

Next: [detailed configuration](../README.md#detailed-configuration).

-----

design.md: Copyright 2014 if(we), Inc.

design.md is licensed under a Creative Commons Attribution-ShareAlike 4.0
International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
