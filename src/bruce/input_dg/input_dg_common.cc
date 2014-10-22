/* <bruce/input_dg/input_dg_common.cc>

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

   Implements <bruce/input_dg/input_dg_common.h>.
 */

#include <bruce/input_dg/input_dg_common.h>

#include <cerrno>
#include <string>
#include <system_error>

#include <syslog.h>

#include <bruce/msg_creator.h>
#include <bruce/util/time_util.h>
#include <capped/memory_cap_reached.h>
#include <server/counter.h>

using namespace Bruce;
using namespace Bruce::InputDg;
using namespace Bruce::Util;
using namespace Capped;

SERVER_COUNTER(InputThreadDiscardMsgMalformed);
SERVER_COUNTER(InputThreadDiscardMsgNoMem);

void Bruce::InputDg::DiscardMalformedMsg(const uint8_t *msg_begin,
    size_t msg_size, TAnomalyTracker &anomaly_tracker, bool no_log_discard) {
  if (!no_log_discard) {
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Discarding malformed message");
    }
  }

  anomaly_tracker.TrackMalformedMsgDiscard(msg_begin, msg_begin + msg_size);
  InputThreadDiscardMsgMalformed.Increment();
}

void Bruce::InputDg::DiscardMsgNoMem(TMsg::TTimestamp timestamp,
    const char *topic_begin, const char *topic_end, const void *key_begin,
    const void *key_end, const void *value_begin, const void *value_end,
    TAnomalyTracker &anomaly_tracker, bool no_log_discard) {
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(key_begin || (key_end == key_begin));
  assert(key_end >= key_begin);
  assert(value_begin || (value_end == value_begin));
  assert(value_end >= value_begin);
  anomaly_tracker.TrackNoMemDiscard(timestamp, topic_begin, topic_end,
      key_begin, key_end, value_begin, value_end);
  InputThreadDiscardMsgNoMem.Increment();

  if (!no_log_discard) {
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      /* Make the topic into a C string for logging. */
      std::string topic(topic_begin, topic_end);

      syslog(LOG_ERR,
             "Discarding message due to buffer space cap (topic: [%s])",
             topic.c_str());
    }
  }
}

TMsg::TPtr Bruce::InputDg::TryCreateAnyPartitionMsg(int64_t timestamp,
    const char *topic_begin, const char *topic_end, const void *key_begin,
    size_t key_size, const void *value_begin, size_t value_size,
    Capped::TPool &pool, TAnomalyTracker &anomaly_tracker,
    TMsgStateTracker &msg_state_tracker, bool no_log_discard) {
  assert(topic_begin);
  assert(topic_end > topic_begin);
  assert(key_begin);
  assert(value_begin);
  TMsg::TPtr msg;

  try {
    msg = TMsgCreator::CreateAnyPartitionMsg(timestamp, topic_begin, topic_end,
        key_begin, key_size, value_begin, value_size, false, pool,
        msg_state_tracker);
  } catch (const TMemoryCapReached &) {
    /* Memory cap prevented message creation.  Report discard below. */
  }

  if (!msg) {
    DiscardMsgNoMem(timestamp, topic_begin, topic_end, key_begin,
        reinterpret_cast<const uint8_t *>(key_begin) + key_size, value_begin,
        reinterpret_cast<const uint8_t *>(value_begin) + value_size,
        anomaly_tracker, no_log_discard);
  }

  return std::move(msg);
}

TMsg::TPtr Bruce::InputDg::TryCreatePartitionKeyMsg(int32_t partition_key,
    int64_t timestamp, const char *topic_begin, const char *topic_end,
    const void *key_begin, size_t key_size, const void *value_begin,
    size_t value_size, Capped::TPool &pool, TAnomalyTracker &anomaly_tracker,
    TMsgStateTracker &msg_state_tracker, bool no_log_discard) {
  assert(topic_begin);
  assert(topic_end > topic_begin);
  assert(key_begin);
  assert(value_begin);
  TMsg::TPtr msg;

  try {
    msg = TMsgCreator::CreatePartitionKeyMsg(partition_key, timestamp,
        topic_begin, topic_end, key_begin, key_size, value_begin, value_size,
        false, pool, msg_state_tracker);
  } catch (const TMemoryCapReached &) {
    /* Memory cap prevented message creation.  Report discard below. */
  }

  if (!msg) {
    DiscardMsgNoMem(timestamp, topic_begin, topic_end, key_begin,
        reinterpret_cast<const uint8_t *>(key_begin) + key_size, value_begin,
        reinterpret_cast<const uint8_t *>(value_begin) + value_size,
        anomaly_tracker, no_log_discard);
  }

  return std::move(msg);
}
