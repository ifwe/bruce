## Detailed Configuration

Before reading the full details of Bruce's configuration options, you will
probably want an overview of Bruce's design, which is avaliable
[here](design.md).

### Config File

Bruce's config file is an XML document that specifies settings for batching,
compression, and per-topic message rate limiting.  It also specifies a list of
initial brokers to try contacting for metadata when Bruce is starting.  Below
is an example config file.  It is well commented, and should be self-
explanatory once the reader is familiar with the information provided in the
above-mentioned design section.

```XML
<?xml version="1.0" encoding="US-ASCII"?>
<!-- example Bruce configuration -->
<bruceConfig>
    <batching>
        <namedConfigs>
            <config name="low_latency">
                <!-- The "value" attribute of any of the 3 elements below can
                     be set to "disable".  However, at least one of them must
                     have its value set to something other than "disable". -->

                <!-- Set 500 milliseconds max batching delay.  Here, you must
                     specify integer values directly rather than using syntax
                     such as "10k". -->
                <time value="500" />

                <!-- No limit on message count.  You can specify a value here
                     such as "10" or "10k".  A value of "10k" is interpreted as
                     (10 * 1024) messages. -->
                <messages value="disable" />

                <!-- Somewhat arbitrary upper bound on batch data size.  As
                     above, "256k" is interpreted as (256 * 1024) bytes.  A
                     simple integer value such as "10000" can also be
                     specified.  This value applies to the actual message
                     content (keys and values).  The total size of a batch will
                     be a bit larger due to header overhead. -->
                <bytes value="256k" />
            </config>

            <config name="default_latency">
                <time value="10000" />
                <messages value="disable" />
                <bytes value="256k" />
            </config>
        </namedConfigs>

        <!-- Somewhat arbitrary upper bound on produce request size.  As above,
             a value such as "100k" is interpreted as (100 * 1024) bytes.  You
             can also specify simple integer valuse such as "100000" directly.
             Here, you can not specify "disable".  A nonnegative integer value
             must be specified.  This value applies to the actual message
             content (keys and values).  The total size of a produce request
             will be a bit larger due to header overhead. -->
        <produceRequestDataLimit value="1024k" />

        <!-- This value should be exactly the same as the message.max.bytes
             value in the Kafka broker configuration.  A larger value will
             cause Kafka to send MessageSizeTooLarge error ACKs to Bruce for
             large compressed message sets, which will cause Bruce to discard
             them.  A smaller value will not cause data loss, but will
             unnecessarily restrict the size of a compressed message set.  As
             above, you can supply a value such as "1024k".  Specifying
             "disable" here is not permitted. -->
        <messageMaxBytes value="1000000" />

        <!-- This specifies the configuration for combined topics batching
             (where a single batch may contain multiple topics).  Setting
             "enable" to false is strongly discouraged due to performance
             considerations.
          -->
        <combinedTopics enable="true" config="default_latency" />

        <!-- This specifies how batching is handled for topics not specified in
             "topicConfigs" below.  Allowed values for the "action" attribute
             are as follows:

                 "perTopic": This setting will cause each topic not listed in
                     "topicConfigs" to be batched individually on a per-topic
                     basis, with the "config" attribute specifying a batching
                     configuration from "namedConfigs" above.

                 "combinedTopics": This setting will cause all topics not
                     listed in "topicConfigs" to be batched together in mixed
                     topic batches.  In this case, "config" must be either
                     missing or set to the empty string, and the batching
                     configuration is determined by the "combinedTopics"
                     element above.  This is the setting that most people will
                     want.

                 "disable": This setting will cause batching to be disabled for
                     all topics not listed in "topicConfigs".  In this case,
                     the "config" attribute is optional and ignored.  This
                     setting is strongly discouraged due to performance
                     considerations.
          -->
        <defaultTopic action="combinedTopics" config="" />

        <topicConfigs>
            <!-- Uncomment and customize the settings in here if you wish to
                 have batching configurations that differ on a per-topic basis.

                 As above, allowed settings for the "action" attribute are
                 "perTopic", "combinedTopics", and "disable".  The "disable"
                 setting is strongly discouraged due to performance
                 considerations.

            <topic name="low_latency_topic_1" action="perTopic"
                   config="low_latency" />
            <topic name="low_latency_topic_2" action="perTopic"
                   config="low_latency" />
              -->
        </topicConfigs>
    </batching>

    <compression>
        <namedConfigs>
            <!-- Don't bother to compress a message set whose total size is
                 less than minSize bytes.  As above, a value such as "1k" is
                 interpreted as (1 * 1024) bytes.  Here, a value of "disable"
                 is not recognized, but you can specify "0".  The value of 128
                 below is somewhat arbitrary, and not based on experimental
                 data.  Currently the only allowed values for "type" are
                 "snappy" and "none".
              -->
            <config name="snappy_config" type="snappy" minSize="128" />

            <!-- "minSize" is ignored (and optional) if type in "none". -->
            <config name="no_compression" type="none" />
        </namedConfigs>

        <!-- This must be an integer value at least 0 and at most 100.  If the
             compressed size of a message set is greater than this percentage
             of the uncompressed size, then Bruce sends the data uncompressed,
             so the Kafka brokers don't waste CPU cycles dealing with the
             compression.  The value below is somewhat arbitrary, and not based
             on experimental data. -->
        <sizeThresholdPercent value="75" />

        <!-- This specifies the compression configuration for all topics not
             listed in "topicConfigs" below.
          -->
        <defaultTopic config="snappy_config" />

        <topicConfigs>
            <!-- Uncomment and customize the settings in here if you wish to
                 configure compression on a per-topic basis.

            <topic name="no_compression_topic_1" config="no_compression" />
            <topic name="no_compression_topic_2" config="no_compression" />
              -->
        </topicConfigs>
    </compression>

    <topicRateLimiting>
        <namedConfigs>
            <!-- This configuration specifies that all messages should be
                 discarded. -->
            <config name="zero" interval="1" maxCount="0" />

            <!-- This configuration specifies no rate limit (i.e. don't discard
                 any messages regardless of their arrival rate). -->
            <config name="infinity" interval="1" maxCount="unlimited" />

            <!-- This configuration specifies a limit of at most 1000 messages
                 every 10000 milliseconds.  Messages that would exceed this
                 limit are discarded. -->
            <config name="config1" interval="10000" maxCount="1000" />

            <!-- This configuration specifies a limit of at most (4 * 1024)
                 messages every 15000 milliseconds.  Messages that would exceed
                 this limit are discarded. -->
            <config name="config2" interval="15000" maxCount="4k" />
        </namedConfigs>

        <!-- This specifies a default configuration for topics not listed in
             <topicConfigs> below.  Each such topic is rate-limited
             individually.  In other words, with this configuration, topic
             "topic_a" would be allowed 1000 messages every 10000 milliseconds,
             and "topic_b" would also be allowed 1000 messages every 10000
             milliseconds. -->
        <defaultTopic config="config1" />

        <topicConfigs>
            <!-- Rate limit configurations for individual topics go here. -->
            <topic name="topic1" config="zero" />
            <topic name="topic2" config="infinity" />
            <topic name="topic3" config="config1" />
            <topic name="topic4" config="config2" />
        </topicConfigs>
    </topicRateLimiting>

    <initialBrokers>
        <!-- When Bruce starts, it chooses a broker in this list to contact for
             metadata.  If Bruce cannot get metadata from the host it chooses,
             it tries other hosts until it succeeds.  Once Bruce successfully
             gets metadata, the broker list in the metadata determines which
             brokers Bruce will connect to for message transmission and future
             metadata requests.  Specifying a single host is ok, but multiple
             hosts are recommended to guard against the case where a single
             specified host is down.
          -->
        <broker host="broker_host_1" port="9092" />
        <broker host="broker_host_2" port="9092" />
    </initialBrokers>
</bruceConfig>
```

### Command Line Arguments

Bruce's required command line arguments are summarized below:

* `--config_path PATH`: This specifies the location of the config file.
* `--msg_buffer_max MAX_KB`: This specifies the amount of memory in kbytes
Bruce reserves for message data.  If this buffer space is exhausted, Bruce
starts discarding messages.
* `--receive_socket_name PATH`: This specifies the pathname of Bruce's UNIX
domain datagram socket that clients write messages to.

Bruce's optional command line arguments are summarized below:

* `-h --help`: Display help message.
* `--version`: Display version information and exit.
* `--log_level LEVEL`: This specifies the maximum enabled log level for syslog
messages.  Allowed values are { LOG_ERR, LOG_WARNING, LOG_NOTICE, LOG_INFO,
LOG_DEBUG }.  The default value is LOG_NOTICE.
* `--log_echo`: Echo syslog messages to standard error.
* `--protocol_version VERSION`: This specifies the protocol version to use when
communicating with Kafka, as specified
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol).
Currently 0 is the only allowed value.  Note: There should actually be two
separate options: a metadata protocol version and a producer protocol version,
since Kafka versions these parts of the protocol separately.  Some minor
refactoring needs to be done to make Bruce behave in this manner.
* `--status_port PORT`: This specifies the port Bruce uses for its web
interface.  The default value is 9090.
* `--max_input_msg_size N`: This specifies the maximum input message size in
bytes expected from clients.  Messages larger than this value will be
discarded.  Here, "size" means the size of the entire datagram.  The default
value is 65536.
* `--allow_large_unix_datagrams`: Allow large enough values for
max_input_msg_size that a client sending a UNIX domain datagram of the maximum
allowed size will need to increase its SO_SNDBUF socket option above the
default value.
* `--max_failed_delivery_attempts N`: Each time Bruce receives an error ACK
causing it to initiate a "pause without discard" action as documented
[here](design.md#dispatcher), Bruce increments the failed delivery attempt
account for each message in the message set that the ACK applies to.  Once a
message's failed delivery attempt count exceeds this value, the message is
discarded.  The default vaule is 5.
* `--daemon`: Causes Bruce to run as a daemon.
* `--client_id ID`: This specifies the client ID string to send in produce
requests, as documented
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol).
If unspecified, the client ID will be empty.
* `--required_acks N`: This specifies the requires ACKs value to send in
produce requests, as documented
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol).
The default value -s -1.
* `--replication_timeout N`: specifies the time in milliseconds the broker will
wait for successful replication to occur, as described
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceRequest),
before returning an error.  The default value is 10000.
* `--shutdown_max_delay N`: This specifies the maximum time in milliseconds
Bruce will spend trying to send queued messages and receive ACKs before
shutting down once it receives a shutdown signal.  If the time limit expires
and Bruce still has queued messages, they will be discarded.  The recommended
way to shut down Bruce is to stop all clients and let Bruce empty its queues
*before* sending Bruce a shutdown signal.  The default value is 30000.
* `--dispatcher_restart_max_delay N`: This specifies the maximum allowed delay
in milliseconds for dispatcher shutdown during a pause or metadata update
event.  During this time period, each send thread will attempt to finish
sending any message currently being sent and then shut down.  Each receive
thread will attempt to drain its queue of sent produce requests waiting for
responses from Kafka.  Any unsent messages in remaining in a send thread's
queue after it shuts down will be sent after the dispatcher is restarted with
new metadata.  The messages contained in any remaining sent produce requests in
a receive thread's "ACK waiting" queue on dispatcher shutdown will be resent
after the dispatcher is restarted with new metadata.  These messages will be
reported as possible duplicates in Bruce's discard reporting interface.  The
default value is 5000.
* `--metadata_refresh_interval N`: This specifies Bruce's metadata refresh
interval in minutes.  The actual interval will vary somewhat due to added
randomness.  This will spread out the metadata requests of multiple Bruce
instances to prevent them from all requesting metadata at the same time.  The
default value is 15.
* `--kafka_socket_timeout N`: This specifies the socket timeout in seconds that
Bruce uses when communicating with the Kafka brokers.  The default value is 60.
* `--min_pause_delay N`: This specifies a lower bound on the initial time
period in milliseconds Bruce will wait before sending a metadata request in
response to a pause event or retrying a failed metadata request.  The default
value is 5000.  See also --pause_rate_limit_initial and
--pause_rate_limit_max_double below.
* `--pause_rate_limit_initial N`: This specifies an initial delay in
milliseconds that Bruce will wait before sending a metadata request in response
to a pause event or retrying a failed metadata request.  The --min_pause_delay
option described above places a lower bound on the actual value, which has a
bit of randomness added to it.  The default value is 5000.
* `--pause_rate_limit_max_double N`: On repeated failed metadata requests or
errors that cause pause events, the delay before requesting metadata (as
specified by --pause_rate_limit_initial) is doubled each time up to a maximum
number of times specified here.  The actual delay has some randomness added to
it.  The default value is 4.
* `--discard_report_interval N`: This specifies the discard report interval in
seconds.  The default value is 600.
* `--no_log_discard`: This prevents Bruce from writing syslog messages when
discards occur.  Discards will still be reported through Bruce's web interface.
* `--debug_dir DIR`: This specifies a directory for debug instrumentation
files, as described
[here](troubleshooting.md).  If unspecified, the debug instrumentation file
option is disabled.
* `--msg_debug_time_limit N`: This specifies a message debugging time limit in
seconds, as described [here](troubleshooting.md).
The default value is 3600.
* `--msg_debug_byte_limit N`: This specifies a message debugging byte limit, as
described [here](troubleshooting.md).
The default value is (2 * 1024 8 1024 * 1024).
* `--skip_compare_metadata_on_refresh`: On metadata refresh, don't compare new
metadata to old metadata.  Always replace the metadata even if it is unchanged.
This should be disabled for normal operation, but enabling it may be useful for
testing.
* `--discard_log_path PATH`: Absolute pathname of local file where discards
will be logged.  This is intended for debugging.  If unspecified, logging of
discards to a file will be disabled.
* `--discard_log_bad_msg_prefix_size N`: Maximum bad message prefix size in
bytes to write to discard logfile when discarding.  The default value is 256.
* `--discard_log_max_file_size N`: Maximum size (in Kb) of discard logfile.
When the next log entry e would exceed the maximum, the logfile (with name f)
is renamed to f.N wnere N is the current time in milliseconds since the epoch.
Then a new file f is opened, and e is written to f.  The default value is 1024.
See also --discard_log_max_archive_size.
* `--discard_log_max_archive_size N`: See description of
--discard_log_max_file_size.  Once a discard logfile is renamed from f to f.N
due to the size restriction imposed by discard_log_max_file_size, the directory
containing f.N is scanned for all old discard logfiles.  If their combined size
exceeds discard_log_max_archive_size (specified in Kb), then old logfiles are
deleted, starting with the oldest, until their combined size no longer exceeds
the maximum.  The default value is 8192.
* `--discard_report_bad_msg_prefix_size N`: Maximum bad message prefix size in
bytes to write to discard report available from Bruce's web interface.  The
default value is 256.
* `--topic_autocreate`: Enable automatic topic creation.  For this to work, the
brokers must be configured with `auto.create.topics.enable=true`.
* `--omit_timestamp`: Do not use this option, since it will soon be removed.
Its purpose is to provide compatibility with legacy infrastructure at if(we).
* `--use_old_input_format`: Do not use this option, since it will soon be
removed.  Its purpose is to provide compatibility with legacy infrastructure at
if(we).
* `--use_old_output_format`: Do not use this option, since it will soon be
removed.  Its purpose is to provide compatibility with legacy infrastructure at
if(we).

Now that you are familiar with all of Bruce's configuration options, you may
find information on [troubleshooting](../README.md#troubleshooting) helpful.

-----

detailed_config.md: Copyright 2014 if(we), Inc.

detailed_config.md is licensed under a Creative Commons Attribution-ShareAlike
4.0 International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
