## Basic Configuration

A simple Bruce configuration can be found in the
[config](../config) directory of Bruce's Git repository, and instructions for
deploying it can be found [here](build_install.md#installing-bruce).  Before
using it, you need to edit Bruce's config file (`/etc/bruce/bruce_conf.xml` in
the example configuration) as follows.  Look for the `<initialBrokers>` XML
element near the bottom of the file.  You will find a list of Kafka brokers to
try contacting for Bruce's initial metadata request.  This list needs to be
edited to specify the brokers in your Kafka cluster.  Specifying a single
broker is ok, since Bruce will learn about other brokers from the metadata
response it receives.  However, specifying multiple brokers is preferable to
guard against a situation where the specified broker is down.  If you wish to
start Bruce using its init script after following the steps given
[here](build_install.md#installing-bruce), that can be done as follows:

```
chkconfig bruce on
service bruce start
```
Otherwise, Bruce can be started manually using the example configuration as
follows:

```
bruce --daemon --msg_buffer_max 65536 \
        --receive_socket_name /var/run/bruce/bruce.socket \
        --config_path /etc/bruce/bruce_conf.xml
```

The above command line arguments have the following effects:
* `--daemon` tells Bruce to run as a daemon.
* `--msg_buffer_max 65536` tells Bruce to reserve 65536 kbytes (or 64 * 1024 *
1024 bytes) of memory for buffering message data to be sent to Kafka.
* `--receive_socket_name /var/run/bruce/bruce.socket` specifies the location of
the UNIX domain datagram socket Bruce creates for receiving messages from
clients.
* `--config_path /etc/bruce/bruce_conf.xml` specifies the location of Bruce's
config file.

A few additional options that you may find useful are the following:
* `--status_port N` specifies the port to use for Bruce's status monitoring web
interface.  The default value is 9090.
* `--client_id CLIENT_ID_STRING` allows a Client ID string to be specified when
sending produce requests to Kafka.  If unspecified, the client ID will be
empty.
* `--required_acks N` specifies the *required ACKs* value to be sent in produce
requests, as described
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceRequest).
If unspecified, a default value of -1 is used.
* `--replication_timeout N` specifies the time in milliseconds the broker will
wait for successful replication to occur, as described
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceRequest),
before returning an error.  If unspecified, a default value of 10000 is used.

Bruce's config file (`/etc/bruce/bruce_conf.xml` in the above example) is an
XML document that specifies batching and configuration options, as well as the
above-described list of initial brokers.  The example configuration specifies
a uniform batching latency of 1000 ms, with a 256 kbyte upper bound on the
total message data size in a single batch.  The maximum message data size for a
single produce request is limited to 1024 kbytes.  These values are somewhat
arbitrary, and may require tuning.  Snappy message compression is also
configured for all topics.

Full details of Bruce's configuration options are provided
[here](detailed_config.md).

You can shut down Bruce using its init script as follows:

```
service bruce stop
```

Alternatively, you can shut down Bruce directly by sending it a SIGTERM or
SIGINT.  For instance:

```
kill -TERM PROCESS_ID_OF_BRUCE
```

or

```
kill -INT PROCESS_ID_OF_BRUCE
```

Once Bruce has been set up with a basic configuration, you can
[send messages](../README.md#sending-messages).

-----

basic_config.md: Copyright 2014 if(we), Inc.

basic_config.md is licensed under a Creative Commons Attribution-ShareAlike 4.0
International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
