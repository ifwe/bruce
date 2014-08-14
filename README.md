(Note: This documentation is a work in progress.  More will be added soon.)

# Bruce

Bruce is a producer daemon for [Apache Kafka](http://kafka.apache.org), a
high throughput publish/subscribe messaging system implemented as a distributed
commit log.  Bruce simplifies clients that send messages to Kafka, freeing them
from the complexity of direct interaction with the Kafka cluster.
Specifically, it handles the details of:

* Routing messages to the proper brokers, and spreading the load evenly across
  multiple partitions for a given topic
* Waiting for acknowledgements, and resending messages as necessary due to
  communication failures or Kafka-reported errors
* Buffering messages to handle transient load spikes and Kafka-related problems
* Tracking message discards when serious problems occur; Providing web-based
  discard reporting and status monitoring interfaces
* Batching and compressing messages in a configurable manner for improved
  performance

Bruce runs on each individual host that communicates with Kafka, receiving
messages from local clients over a UNIX domain datagram socket.  Clients write
messages to Bruce's socket in a simple binary format.  Once a client has
written a message, no further interaction with Bruce is required.  From that
point onward, Bruce takes full responsibility for reliable message delivery.
Bruce serves as a single intake point for a Kafka cluster, receiving messages
from diverse clients regardless of what programming language a client is
written in.  Client code for writing to Bruce's socket will soon be available
for Java, Python, PHP, and C.  Client code is currently available in C++,
Bruce's implementation language.

(more documentation will appear shortly...)
