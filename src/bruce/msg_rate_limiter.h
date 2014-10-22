/* <bruce/msg_rate_limiter.h>

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

   Class for per-topic message rate limiting.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>

#include <base/no_copy_semantics.h>
#include <bruce/conf/topic_rate_conf.h>

namespace Bruce {

  /* This class is used for rate limiting of messages on a per-topic basis.
     The motivation is to deal with situations in which buggy client code sends
     an unreasonably large volume of messages to some topic T.  Without rate
     limiting, this might stress the Kafka cluster to the point where it can no
     longer keep up with the message volume.  The result is likely to be
     slowness in message processing that affects many topics, causing Bruce to
     discard messages across many topics.  The goal of rate limiting is to
     contain the damage by discarding excess messages for topic T, preventing
     the Kafka cluster from becoming overwhelmed and forcing Bruce to discard
     messages for other topics. */
  class TMsgRateLimiter final {
    NO_COPY_SEMANTICS(TMsgRateLimiter);

    public:
    explicit TMsgRateLimiter(const Conf::TTopicRateConf &conf)
        : Conf(conf),
          IsEnabled(RateLimitingIsEnabled(conf)) {
    }

    /* Return true if forwarding a message with the given topic would cause the
       rate limit for the topic to be exceeded.  Otherwise return false.  The
       message's rate limiting timestamp is given by 'timestamp'. */
    bool WouldExceedLimit(const std::string &topic, uint64_t timestamp);

    private:
    /* Rate limiting state for a single topic. */
    struct TTopicState {
        /* true indicates that rate limiting for this topic is enabled. */
        bool Enable;

        /* Interval length in milliseconds from config file. */
        size_t Interval;

        /* Max # of allowed messages within 'Interval', from config file. */
        size_t MaxCount;

        /* Start time of current interval in milliseconds since the epoch. */
        uint64_t IntervalStart;

        /* # of messages currently received during interval starting on
           'IntervalStart' */
        size_t Count;

        TTopicState()
            : Enable(false),
              Interval(1),
              MaxCount(0),
              IntervalStart(0),
              Count(0) {
        }
    };  // TTopicState

    static bool RateLimitingIsEnabled(const Conf::TTopicRateConf &conf);

    TTopicState &GetTopicState(const std::string &topic, uint64_t timestamp);

    /* Rate limiting config from config file. */
    const Conf::TTopicRateConf &Conf;

    /* true is rate limiting is enabled for at least one topic.  false
       otherwise. */
    const bool IsEnabled;

    using TTopicStateMap = std::unordered_map<std::string, TTopicState>;

    /* Key is topic and value is rate limiting state for topic.  When we see
       the very first message for a given topic, we add an entry for that topic
       to the map. */
    TTopicStateMap TopicStateMap;
  };  // TMsgRateLimiter

}  // Bruce
