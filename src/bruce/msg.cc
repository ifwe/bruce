/* <bruce/msg.cc>

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

   Implements <bruce/msg.h>.
 */

#include <bruce/msg.h>

#include <algorithm>

#include <syslog.h>

#include <bruce/util/time_util.h>
#include <capped/writer.h>
#include <server/counter.h>
#include <server/daemonize.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Util;
using namespace Capped;

SERVER_COUNTER(MsgCreate);
SERVER_COUNTER(MsgDestroy);
SERVER_COUNTER(MsgUnprocessedDestroy);

/* Create a key and value for a message.  Used by constructor. */
static TBlob MakeKeyAndValue(const void *key, size_t key_size,
    const void *value, size_t value_size, Capped::TPool &pool) {
  TWriter writer(&pool);
  writer.Write(key, key_size);
  writer.Write(value, value_size);
  return writer.DraftBlob();
}

TMsg::TPtr TMsg::CreateAnyPartitionMsg(TTimestamp timestamp,
    const void *topic_begin, const void *topic_end, const void *key,
    size_t key_size, const void *value, size_t value_size, bool body_truncated,
    Capped::TPool &pool) {
  return TPtr(new TMsg(TRoutingType::AnyPartition, 0, timestamp, topic_begin,
      topic_end, key, key_size, value, value_size, body_truncated, pool));
}

TMsg::TPtr TMsg::CreatePartitionKeyMsg(int32_t partition_key,
    TTimestamp timestamp, const void *topic_begin, const void *topic_end,
    const void *key, size_t key_size, const void *value, size_t value_size,
    bool body_truncated, Capped::TPool &pool) {
  return TPtr(new TMsg(TRoutingType::PartitionKey, partition_key, timestamp,
      topic_begin, topic_end, key, key_size, value, value_size, body_truncated,
      pool));
}

TMsg::~TMsg() noexcept {
  assert(this);
  MsgDestroy.Increment();

  if (State != TState::Processed) {
    MsgUnprocessedDestroy.Increment();
    static TLogRateLimiter lim(std::chrono::seconds(5));

    if (lim.Test()) {
      syslog(LOG_ERR, "Possible bug: destroying unprocessed message with "
             "topic [%s] and timestamp %llu.  This is expected behavior if "
             "the server is exiting due to a fatal error.", Topic.c_str(),
             static_cast<unsigned long long>(Timestamp));
      Server::BacktraceToLog();
    }
  }
}

TMsg::TMsg(TRoutingType routing_type, int32_t partition_key,
    TTimestamp timestamp, const void *topic_begin, const void *topic_end,
    const void *key, size_t key_size, const void *value,
    size_t value_size, bool body_truncated, Capped::TPool &pool)
    : RoutingType(routing_type),
      PartitionKey(partition_key),
      Timestamp(timestamp),
      CreationTimestamp(GetMonotonicRawMilliseconds()),
      State(TState::New),
      FailedDeliveryAttemptCount(0),
      Topic(reinterpret_cast<const char *>(topic_begin),
            reinterpret_cast<const char *>(topic_end)),
      Partition(0),
      ErrorAckReceived(false),
      KeyAndValue(MakeKeyAndValue(key, key_size, value, value_size, pool)),
      KeySize(key_size),
      BodyTruncated(body_truncated) {
  assert(topic_begin);
  assert(topic_end >= topic_end);
  assert(key || (key_size == 0));
  assert(value || (value_size == 0));
  assert(KeyAndValue.Size() == (key_size + value_size));
  MsgCreate.Increment();
}
