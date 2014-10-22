/* <bruce/anomaly_tracker.h>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 if(we)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
   ----------------------------------------------------------------------------

   Class used by bruce daemon to track message discard and possible duplicate
   events.
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <time.h>

#include <base/no_copy_semantics.h>
#include <bruce/discard_file_logger.h>
#include <bruce/msg.h>
#include <bruce/util/time_util.h>

namespace Bruce {

  /* Class used by bruce daemon to track message discard and possible duplicate
     events. */
  class TAnomalyTracker final {
    NO_COPY_SEMANTICS(TAnomalyTracker);

    public:
    /* The maximum number of bad topics to store info for.  Don't make this
       value too large since a simple linear list search is done each time a
       bad topic is received. */
    const size_t MAX_BAD_TOPICS = 25;

    /* The maximum number of malformed messages to store info for.  Don't make
       this value too large since a simple linear list search is done each time
       a malformed message is received. */
    const size_t MAX_MALFORMED_MSGS = 25;

    /* The maximum number of too long messages to store info for.  Don't make
       this value too large since a simple linear list search is done each time
       a too long message is received. */
    const size_t MAX_LONG_MSGS = 25;

    /* For a given topic, this represents the earliest and latest timestamps
       of some type of anomaly over a time period. */
    struct TInterval {
      /* Timestamp of earliest anomaly. */
      TMsg::TTimestamp First;

      /* Timestamp of latest anomaly. */
      TMsg::TTimestamp Last;

      /* Construct an interval representing the first anomaly for a topic,
         whose timestamp is given by parameter 'initial'.  Since this is the
         first anomaly, the timestamp defines both endpoints. */
      explicit TInterval(TMsg::TTimestamp initial)
          : First(initial),
            Last(initial) {
      }
    };  // TInterval

    /* For a given topic, this stores anomaly information. */
    struct TTopicInfo {
      /* This is the interval defined by the timestamps of the earliest and
         latest anomalies. */
      TInterval Interval;

      /* This is the total number of anomalies. */
      size_t Count;

      /* Parameter 'initial' specifies the timestamp of the first anomaly for
         a topic.  Both endpoints of the interval are initialized to this value
         and the count is initialized to 1. */
      explicit TTopicInfo(TMsg::TTimestamp initial)
          : Interval(initial),
            Count(1) {
      }
    };  // TTopicInfo

    /* This stores anomaly information for all valid topics for which at least
       one anomaly has occurred.  The keys are the topics.  A topic is in the
       map if and only if at least one anomaly has occurred for that topic. */
    using TMap = std::map<std::string, TTopicInfo>;

    /* This stores per-topic counts of messages discarded due to the message
       rate limiting mechanism.  Discards due to rate limiting are tracked both
       here and using the above TMap mechanism. */
    using TRateLimitMap = std::map<std::string, size_t>;

    /* Contains all info tracked by the anomaly tracker. */
    struct TInfo {
      private:
      /* A unique ID number for this TInfo. */
      size_t ReportId;

      /* Start time of reporting interval in seconds since the epoch. */
      uint64_t StartTime;

      public:
      size_t GetReportId() const {
        assert(this);
        return ReportId;
      }

      uint64_t GetStartTime() const {
        assert(this);
        return StartTime;
      }

      /* Info for discarded messages with valid topics. */
      TMap DiscardTopicMap;

      /* Info for possibly duplicated messages. */
      TMap DuplicateTopicMap;

      /* Per-topic counts of messages discarded due to the rate limiting
         mechanism.  Each such discard is tracked both here and in
         'DiscardTopicMap' above. */
      TRateLimitMap RateLimitDiscardMap;

      /* List of most recently discarded malformed messages.  The most recently
         seen message is at the front of the list.  For messages exceeding a
         certain length, only a prefix of the message is stored. */
      std::list<std::string> MalformedMsgs;

      /* Messages received with an unsupported protocol version.  The keys are
         protocol versions and the values are counts. */
      std::map<int, size_t> UnsupportedVersionMsgs;

      /* List of most recent messages with valid topics that were discarded
         because they were too long.  The most recently seen message is at the
         front of the list.  Due to their excessive lengths, only prefixes of
         the messages are stored. */
      std::list<std::string> LongMsgs;

      /* List of most recent bad topics that caused discards.  The most
         recently seen bad topic is at the front of the list. */
      std::list<std::string> BadTopics;

      /* Count of discarded malformed messages. */
      size_t MalformedMsgCount;

      /* Count of messages discarded due to unsupported API key. */
      size_t UnsupportedApiKeyMsgCount;

      /* Count of messages discarded due to unsupported message version. */
      size_t UnsupportedVersionMsgCount;

      /* Count of messages discarded due to bad topic. */
      size_t BadTopicMsgCount;

      TInfo()
          : ReportId(0),
            StartTime(0),
            MalformedMsgCount(0),
            UnsupportedApiKeyMsgCount(0),
            UnsupportedVersionMsgCount(0),
            BadTopicMsgCount(0) {
      }

      TInfo(size_t report_id, uint64_t start_time)
          : ReportId(report_id),
            StartTime(start_time),
            MalformedMsgCount(0),
            UnsupportedApiKeyMsgCount(0),
            UnsupportedVersionMsgCount(0),
            BadTopicMsgCount(0) {
      }
    };  // TInfo

    using TClockFn = std::function<uint64_t()>;
    using TDiscardReason = TDiscardFileLogger::TDiscardReason;

    /* Used by unit tests. */
    static size_t GetNoDiscardQueryCount();

    /* A value of 0 for 'report_interval' disables interval reporting (useful
       for unit tests). */
    explicit TAnomalyTracker(TDiscardFileLogger &discard_file_logger,
        size_t report_interval, size_t max_msg_prefix_len,
        TClockFn clock_fn = &Util::GetEpochSeconds)
        : DiscardFileLogger(discard_file_logger),
          ReportInterval(report_interval),
          ClockFn(clock_fn),
          MaxMsgPrefixLen(max_msg_prefix_len),
          LastGetInfoTime(clock_fn()),
          FillingReport(new TInfo(0, LastGetInfoTime.load())) {
    }

    /* Parameter 'msg' is a message that is about to be discarded.  Update the
       map with information for the given discard. */
    void TrackDiscard(TMsg &msg, TDiscardReason reason);

    /* Same as above, except the message is passed by smart pointer. */
    void TrackDiscard(const TMsg::TPtr &msg_ptr, TDiscardReason reason) {
      assert(this);
      TrackDiscard(*msg_ptr, reason);
    }

    /* Parameter 'msg' is a message that was sent twice due to a Kafka failure,
       and therefore may be duplicated.  Update the map with information for
       the given possible duplicate. */
    void TrackDuplicate(const TMsg &msg);

    /* Same as above, except the message is passed by smart pointer. */
    void TrackDuplicate(const TMsg::TPtr &msg_ptr) {
      assert(this);
      TrackDuplicate(*msg_ptr);
    }

    /* Track a discard due to lack of buffer space.  'topic_begin' points to
       the first character of the topic of a discarded message.  'topic_end'
       points one position past the last character of the topic.  Likewise,
       'body_begin' and 'body_end' specify the message body. */
    void TrackNoMemDiscard(TMsg::TTimestamp timestamp, const char *topic_begin,
        const char *topic_end, const void *key_begin, const void *key_end,
        const void *value_begin, const void *value_end);

    /* Track a discard due to a message whose topic could not be identified.
       We save prefixes of recently seen invalid message strings to facilitate
       troubleshooting. */
    void TrackMalformedMsgDiscard(const void *prefix_begin,
                                  const void *prefix_end);

    void TrackUnsupportedApiKeyDiscard(const void *prefix_begin,
        const void *prefix_end, int api_key);

    /* Track a discard due to a message whose topic could not be identified.
       We save prefixes of recently seen invalid message strings to facilitate
       troubleshooting. */
    void TrackUnsupportedMsgVersionDiscard(const void *prefix_begin,
        const void *prefix_end, int version);

    /* 'topic_begin' points to the first character of an unknown topic, and
       'topic_end' points one position past the last character of the topic.
       Likewise, 'body_begin' and 'body_end' specify the message body.  If the
       unknown topic name exceeds the maximum supported topic length N, then
       this will specify the first N characters of the unknown topic.  We keep
       track of up to MAX_BAD_TOPICS topics.
     */
    void TrackBadTopicDiscard(TMsg::TTimestamp timestamp,
        const char *topic_begin, const char *topic_end, const void *key_begin,
        const void *key_end, const void *value_begin, const void *value_end);

    /* Same as above, but takes TMsg as parameter. */
    void TrackBadTopicDiscard(TMsg &msg);

    /* Same as above, but takes TMsg::TPtr as parameter. */
    void TrackBadTopicDiscard(const TMsg::TPtr &msg_ptr) {
      assert(this);
      TrackBadTopicDiscard(*msg_ptr);
    }

    /* Track a discard due to a message whose topic is valid, but whose size is
       too large. */
    void TrackLongMsgDiscard(TMsg &msg);

    /* Same as above, except the message is passed by smart pointer. */
    void TrackLongMsgDiscard(const TMsg::TPtr &msg_ptr) {
      assert(this);
      TrackLongMsgDiscard(*msg_ptr);
    }

    /* This method copies the partially completed report from the current
       reporting period into 'filling_report_copy' and returns a shared_ptr to
       the previous report, which is the most recently completed one.  In the
       case where we are still in the first reporting interval, the returned
       shared_ptr will be empty. */
    std::shared_ptr<const TInfo> GetInfo(TInfo &filling_report_copy) const;

    /* A monitoring script is supposed to periodically query bruce for discard
       info.  Call this function periodically to make sure we are getting
       queried frequently enough to avoid losing discard info, and report an
       error if we are not. */
    void CheckGetInfoRate() const;

    /* Return the reporting interval length in seconds. */
    size_t GetReportInterval() const {
      assert(this);
      return ReportInterval;
    }

    private:
    const uint8_t *EnforceMaxPrefixLen(const void *msg_begin,
        const void *msg_end);

    /* Caller must hold 'Mutex'.  Parameter 'now' is the current time in
       seconds since the epoch. */
    void AdvanceReportPeriod(uint64_t now) const;

    /* Caller must hold 'Mutex'. */
    void UpdateTopicMap(TMap &topic_map, std::string &&topic,
                        TMsg::TTimestamp timestamp);

    TDiscardFileLogger &DiscardFileLogger;

    /* Value is in seconds. */
    const size_t ReportInterval;

    const TClockFn ClockFn;

    const size_t MaxMsgPrefixLen;

    /* Value is in seconds since the epoch. */
    mutable std::atomic<uint64_t> LastGetInfoTime;

    /* Protects 'LastFullReport' and 'FillingReport'. */
    mutable std::mutex Mutex;

    mutable std::shared_ptr<const TInfo> LastFullReport;

    mutable std::unique_ptr<TInfo> FillingReport;
  };  // TAnomalyTracker

}  // Bruce
