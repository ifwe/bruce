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

(more content will appear here soon)

If you are interested in making custom modifications or contributing to Bruce,
information is provided
[here](https://github.com/tagged/bruce#modifying-bruces-implementation).
