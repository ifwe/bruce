## Troubleshooting

### Counters

As described
[here](https://github.com/tagged/bruce/blob/master/doc/status_monitoring.md#counter-reporting),
Bruce's web interface provides counter reports that can be used for
troubleshooting.  Some of Bruce's more interesting counters are as follows:

* `MsgCreate`: This is incremented each time Bruce creates a message.
* `MsgDestroy`: This is incremented each time Bruce destroys a message.  The
value `MsgCreate - MsgDestroy` indicates the total number of messages Bruce is
currently holding internally.  If the value gets too large, it likely indicates
that Kafka isn't keeping up with the volume of messages directed at it.
* `MsgUnprocessedDestroy`: This indicates destruction of a message before Bruce
has marked it as processed.  If this occurs due to any reason other than Bruce
exiting on a fatal error, then it indicates a bug in Bruce.  If this type of
event occurs, Bruce will write a syslog message containing a stack trace, which
will help track down the problem.
* `AckErrorXxx`: `AckErrorNone` indicates a successful ACK from Kafka.  All
other counters of this type indicate various types of error ACKs received from
Kafka, as documented
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes).
`AckErrorNone` is incremented each time Bruce receives an ACK indicating
successful storage and replication of a message set.
* `InputThreadDiscardXxx` and `DiscardXxx`: These counters are incremented when
Bruce discards messages for various reasons.
* `BugXxx`: A nonzero value for any counter starting with the prefix `Bug`
indicates a bug in Bruce.
* `MongooseXxx`: These counters indicate events related to Bruce's web
interface.
* `NoDiscardQuery`: This is incremented when Bruce stops getting asked for
discard reports at frequent enough intervals.  To avoid losing discard
information, it is recommended to query Bruce's discard reporting interface at
intervals slightly shorter that Bruce's discard reporting interval.

It is also useful to look in `src/bruce/scripts/bruce_counters.py` to see
which counters the script monitors and how it responds to incremented values.

### Discard File Logging

For debugging and troubleshooting purposes, it may be helpful to configure
Bruce to log discards to local files.  To do this, see the
`--discard_log_path PATH`, `--discard_log_bad_msg_prefix_size N`,
`--discard_log_max_file_size N`, and `--discard_log_max_archive_size N` command
line options, which are documented
[here](https://github.com/tagged/bruce/blob/master/doc/detailed_config.md#command-line-arguments).
As documented
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets),
a Kafka message consists of a key and a value, either of which may be empty.
Currently only the values are written to Bruce's discard logfiles.  Including
keys to the logfiles is future work.  It should also be mentioned that the
values are base64 encoded when written to the logfiles.

### Debug Logfiles

As documented
[here](https://github.com/tagged/bruce/blob/master/doc/detailed_config.md#command-line-arguments),
the `--debug_dir DIR`, `--msg_debug_time_limit N` and
`--msg_debug_byte_limit N` command line options configure Bruce's debug logfile
mechanism, which causes Bruce to maintain 3 separate logfiles: One for messages
received from its input socket, one for messages sent to Kafka, and one for
messages that Bruce received a successful ACK for.  The `--debug_dir DIR`
option specifies the directory in which these files are placed.  To start
and stop the logging mechanism, you must send HTTP requests to Bruce's web
interface as follows:

* To start logging for a specific topic, send an HTTP GET to
`http://bruce_host:9090/msg_debug/add_topic/name_of_topic`.
* To stop logging for a specific topic, send an HTTP GET to
`http://bruce_host:9090/msg_debug/del_topic/name_of_topic`.
* To start logging for all topics, send an HTTP GET to
`http://bruce_host:9090/msg_debug/add_all_topics`.
* To stop logging for all topics, send an HTTP GET to
`http://bruce_host:9090/msg_debug/del_all_topics`.
* To see which topics are currently being logged, send an HTTP GET to
`http://bruce_host:9090/msg_debug/get_topics`.
* To stop logging and make the debug logfiles empty, send an HTTP GET to
`http://bruce_host:9090/msg_debug/truncate_files`.

Once debug logging is started, it will automatically stop when either the
time limit specified by `--msg_debug_time_limit N` expires or the debug logfile
size limit specified by `--msg_debug_byte_limit N` is reached.

### Other Tools

Bruce's queue status interface described
[here](https://github.com/tagged/bruce/blob/master/doc/status_monitoring.md#queued-message-information)
provides per-topic information on messages being processed by Bruce.  In cases
where Kafka isn't keeping up with the message volume, this may be helpful in
identifying specific topics that are placing heavy load on the system.  Useful
information can also be obtained from Bruce's syslog messages.  As documented
[here](https://github.com/tagged/bruce/blob/master/doc/detailed_config.md#command-line-arguments),
the `--log_level LEVEL` command line option configures Bruce's logging
verbosity.

If you are interested in making custom modifications or contributing to Bruce,
information is provided
[here](https://github.com/tagged/bruce#modifying-bruces-implementation).
