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
Bruce's implementation language.  Bruce currently runs only on Linux.  Also, it
does not support Kafka versions prior to 0.8.

## Setting Up a Build Environment

The first step in getting Bruce working is to set up a build environment.  A
good starting point for this is to
[set up the Google Test Framework](https://github.com/tagged/bruce/blob/master/doc/gtest.md), which Bruce uses for its unit tests.  The remaining steps differ
depending on which Linux distribution you are using.  Currently, instructions
are available for
[CentOS 7](https://github.com/tagged/bruce/blob/master/doc/centos_7_env.md),
[CentOS 6.5](https://github.com/tagged/bruce/blob/master/doc/centos_6_5_env.md),
and [Ubuntu (14.04.1 LTS and 13.10)](https://github.com/tagged/bruce/blob/master/doc/ubuntu_14_and_13_env.md).

## Building and Installing Bruce

Once your build environment is set up, the next step is to
[build and install](https://github.com/tagged/bruce/blob/master/doc/build_install.md)
Bruce.

## Running Bruce with Basic Configuration

Simple instructions for running Bruce with a basic configuration can be found
[here](https://github.com/tagged/bruce/blob/master/doc/basic_config.md).

## Sending Messages

Information on how to send messages to Bruce can be found
[here](https://github.com/tagged/bruce/blob/master/doc/sending_messages.md).

## Status Monitoring

Information on status monitoring can be found
[here](https://github.com/tagged/bruce/blob/master/doc/status_monitoring.md).

## Design Overview

Before going into more details on Bruce's configuration options, it is helpful
to have an understanding of Bruce's design, which is described
[here](https://github.com/tagged/bruce/blob/master/doc/design.md).

## Detailed Configuration

Full details of Bruce's configuration options are provided
[here](https://github.com/tagged/bruce/blob/master/doc/detailed_config.md).

## Troubleshooting

Information that may help with troubleshooting is provided
[here](https://github.com/tagged/bruce/blob/master/doc/troubleshooting.md).

## Modifying Bruce's Implementation

Information for developers interested in making custom modifications or
contributing code to Bruce is provided
[here](https://github.com/tagged/bruce/blob/master/doc/dev_info.md).

## Getting Help

Information on getting help with Bruce is provided
[here](https://github.com/tagged/bruce/blob/master/doc/getting_help.md).

-----

README.md: Copyright 2014 Tagged, Inc.

README.md is licensed under a Creative Commons Attribution-ShareAlike 4.0
International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
