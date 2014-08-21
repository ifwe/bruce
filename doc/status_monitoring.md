## Status Monitoring

Bruce provides a web-based management interface.  The default port number is
9090, but can be changed as described
[here](https://github.com/tagged/bruce/blob/master/doc/detailed_config.md).
Assuming that Bruce is running on a system named `example`, your web browser
will show the following when directed at `http://example:9090`:

![Bruce management interface](https://github.com/tagged/bruce/blob/master/doc/web_interface.jpg?raw=true)

### Counter Reporting

If you click on `get counter values`, you will see a page that looks something
like this:

```
now=1408656417 Thu Aug 21 14:26:57 2014
since=1408585285 Wed Aug 20 18:41:25 2014
pid=14246

[bruce/web_interface.cc, 43].MongooseUrlDecodeError=0
[bruce/web_interface.cc, 42].MongooseUnknownException=0
[bruce/web_interface.cc, 41].MongooseStdException=0
[bruce/web_interface.cc, 40].MongooseHttpRequest=1582
[bruce/web_interface.cc, 39].MongooseGetMsgStatsRequest=0
[bruce/web_interface.cc, 38].MongooseGetMetadataFetchTimeRequest=0
[bruce/web_interface.cc, 37].MongooseGetDiscardsRequest=790
[bruce/web_interface.cc, 36].MongooseGetCountersRequest=792
[bruce/web_interface.cc, 35].MongooseEventLog=0
[bruce/msg.cc, 25].MsgUnprocessedDestroy=0
[bruce/msg.cc, 24].MsgDestroy=26934272
[bruce/msg.cc, 23].MsgCreate=26934440
[bruce/util/pause_button.cc, 14].PauseStarted=0
[bruce/msg_dispatch/sender.cc, 53].SendProduceRequestOk=1015702
(remaining output omitted)
```

These counter values track various events inside Bruce, and can be used for
health monitoring and troubleshooting.  For example,
`src/bruce/scripts/bruce_counters.py` is a Nagios script that monitors Bruce's
counters and reports problems.  Details on the meanings of some of the more
interesting counters are provided
[here](https://github.com/tagged/bruce/blob/master/doc/troubleshooting.md).
Also, you can look in Bruce's source code to see what a counter indicates.  For
instance, near the top of `src/bruce/msg.cc` you will see the following
definitions:

```
SERVER_COUNTER(MsgCreate);
SERVER_COUNTER(MsgDestroy);
SERVER_COUNTER(MsgUnprocessedDestroy);
```

These create the counters of the same names shown above.  Then you can look for
places in the code where the counters are incremented.  For example, counters
`MsgDestroy` and `MsgUnprocessedDestroy` are incremented inside the destructor
for class `TMsg`, which represents a single message:

```C++
TMsg::~TMsg() noexcept {
  assert(this);
  MsgDestroy.Increment();

  if (State != TState::Processed) {
    MsgUnprocessedDestroy.Increment();
    static TLogRateLimiter lim(std::chrono::seconds(5));

    if (lim.Test()) {
      syslog(LOG_ERR, "Possible bug: destroying unprocessed message with "
             "topic [%s] and timestamp %llu. This is expected behavior if "
             "the server is exiting due to a fatal error.", Topic.c_str(),
             static_cast<unsigned long long>(Timestamp));
      Server::BacktraceToLog();
    }
  }
}
```

### Discard Reporting

(content will be added here soon)

### Queued Message Information

(content will be added here soon)

### Metadata Fetch Time

(content will be added here soon)

### Metadata Updates

(content will be added here soon)

At this point it is helpful to have some information on
[Bruce's design](https://github.com/tagged/bruce#design-overview).
