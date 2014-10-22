/* <bruce/input_dg/input_dg_util.cc>

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

   Implements <bruce/input_dg/input_dg_util.h>.
 */

#include <bruce/input_dg/input_dg_util.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <system_error>

#include <syslog.h>

#include <base/field_access.h>
#include <bruce/input_dg/any_partition/any_partition_util.h>
#include <bruce/input_dg/input_dg_common.h>
#include <bruce/input_dg/input_dg_constants.h>
#include <bruce/input_dg/old_v0_input_dg_reader.h>
#include <bruce/input_dg/partition_key/partition_key_util.h>
#include <bruce/msg_creator.h>
#include <bruce/util/time_util.h>
#include <capped/memory_cap_reached.h>
#include <server/counter.h>

using namespace Bruce;
using namespace Bruce::InputDg;
using namespace Bruce::InputDg::AnyPartition;
using namespace Bruce::InputDg::PartitionKey;
using namespace Bruce::Util;
using namespace Capped;

SERVER_COUNTER(InputThreadDiscardMsgUnsupportedApiKey);
SERVER_COUNTER(InputThreadDiscardMsgUnsupportedVersion);
SERVER_COUNTER(InputThreadDiscardOldOldFormatMsgMalformed);
SERVER_COUNTER(InputThreadDiscardOldOldFormatMsgNoMem);
SERVER_COUNTER(InputThreadProcessOldFormatMsg);
SERVER_COUNTER(InputThreadProcessOldOldFormatMsg);

static void DiscardMsgWithNoTopic(const char *msg_begin, size_t msg_size,
    TAnomalyTracker &anomaly_tracker, bool no_log_discard) {
  if (!no_log_discard) {
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Discarding message with invalid format");
    }
  }

  anomaly_tracker.TrackMalformedMsgDiscard(
      reinterpret_cast<const uint8_t *>(msg_begin),
      reinterpret_cast<const uint8_t *>(msg_begin) + msg_size);
  InputThreadDiscardOldOldFormatMsgMalformed.Increment();
}

static void DiscardOldOldFormatMsgNoMem(const char *msg_begin, char *topic_end,
    const char *body_begin, const char *body_end,
    TAnomalyTracker &anomaly_tracker, bool no_log_discard) {
  assert(topic_end >= msg_begin);
  assert(body_begin > topic_end);
  assert(body_end >= body_begin);

  if (!no_log_discard) {
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      /* Make the topic into a C string for logging. */
      assert(topic_end < body_begin);
      *topic_end = '\0';

      syslog(LOG_ERR,
             "Discarding message due to buffer space cap (topic: [%s])",
             msg_begin);
    }
  }

  anomaly_tracker.TrackNoMemDiscard(GetEpochMilliseconds(), msg_begin,
      topic_end, nullptr, nullptr, body_begin, body_end);
  InputThreadDiscardOldOldFormatMsgNoMem.Increment();
}

static TMsg::TPtr BuildMsgFromOldOldFormatDg(const char *msg_start,
    size_t msg_size, const TConfig &config, Capped::TPool &pool,
    TAnomalyTracker &anomaly_tracker, TMsgStateTracker &msg_state_tracker) {
  assert(msg_start);
  InputThreadProcessOldOldFormatMsg.Increment();

  /* Dirty hack: This function is going away soon, so I'm being sloppy. */
  char *msg_begin = const_cast<char *>(msg_start);

  bool truncate = (msg_size > config.MaxInputMsgSize);

  if (truncate) {
    /* A Message truncated because it is too large is labeled as such, and
       passed to the router thread.  If the router thread determines that the
       message has a valid topic, it will process the message as a discard due
       to excessive size.  Otherwise it will process the message as a bad topic
       discard. */
    msg_size = config.MaxInputMsgSize;
  }

  char * const msg_end = msg_begin + msg_size;

  /* Find the first space character (ASCII 0x20) in the message.  A message
     consists of a Kafka topic, followed by a single space character, followed
     by the message body. */
  char * const topic_end = std::find(msg_begin, msg_end, ' ');

  if (topic_end == msg_end) {
    DiscardMsgWithNoTopic(msg_begin, msg_size, anomaly_tracker,
                          config.NoLogDiscard);
    return TMsg::TPtr();
  }

  char * const body_begin = topic_end + 1;
  size_t body_size = msg_size - (body_begin - msg_begin);

  TMsg::TPtr msg;

  try {
    msg = TMsgCreator::CreateAnyPartitionMsg(GetEpochMilliseconds(), msg_begin,
        topic_end, nullptr, 0, body_begin, body_size, truncate, pool,
        msg_state_tracker);
  } catch (const TMemoryCapReached &) {
    /* Memory cap prevented message creation.  Report discard below. */
  }

  if (!msg) {
    DiscardOldOldFormatMsgNoMem(msg_begin, topic_end, body_begin,
        body_begin + body_size, anomaly_tracker, config.NoLogDiscard);
  }

  return std::move(msg);
}

static TMsg::TPtr BuildMsgFromOldFormatDg(const void *dg, size_t dg_size,
    Capped::TPool &pool, TAnomalyTracker &anomaly_tracker,
    TMsgStateTracker &msg_state_tracker, bool no_log_discard) {
  assert(dg);
  InputThreadProcessOldFormatMsg.Increment();
  const uint8_t *dg_bytes = reinterpret_cast<const uint8_t *>(dg);
  size_t fixed_part_size = INPUT_DG_SZ_FIELD_SIZE +
      INPUT_DG_OLD_VER_FIELD_SIZE;
  int8_t ver = dg_bytes[INPUT_DG_SZ_FIELD_SIZE];
  const uint8_t *versioned_part_begin = &dg_bytes[fixed_part_size];
  const uint8_t *versioned_part_end = versioned_part_begin +
      (dg_size - fixed_part_size);

  switch (ver) {
    case 0: {
      return TOldV0InputDgReader(dg_bytes, versioned_part_begin,
          versioned_part_end, pool, anomaly_tracker,
          msg_state_tracker, no_log_discard).BuildMsg();
    }
    default: {
      break;
    }
  }

  if (!no_log_discard) {
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Discarding message with unsupported version: %d",
             static_cast<int>(ver));
    }
  }

  anomaly_tracker.TrackUnsupportedMsgVersionDiscard(dg_bytes,
      dg_bytes + dg_size, ver);
  InputThreadDiscardMsgUnsupportedVersion.Increment();
  return TMsg::TPtr();
}

TMsg::TPtr Bruce::InputDg::BuildMsgFromDg(const void *dg, size_t dg_size,
    const TConfig &config, Capped::TPool &pool,
    TAnomalyTracker &anomaly_tracker, TMsgStateTracker &msg_state_tracker) {
  assert(dg);

  if (config.UseOldInputFormat) {
    return BuildMsgFromOldOldFormatDg(reinterpret_cast<const char *>(dg),
        dg_size, config, pool, anomaly_tracker, msg_state_tracker);
  }

  const uint8_t *dg_bytes = reinterpret_cast<const uint8_t *>(dg);
  size_t fixed_part_size = INPUT_DG_SZ_FIELD_SIZE +
      INPUT_DG_API_KEY_FIELD_SIZE + INPUT_DG_API_VERSION_FIELD_SIZE;

  if (dg_size < fixed_part_size) {
    DiscardMalformedMsg(dg_bytes, dg_size, anomaly_tracker,
                        config.NoLogDiscard);
    return TMsg::TPtr();
  }

  int32_t sz = ReadInt32FromHeader(dg_bytes);

  if ((sz < 0) || (static_cast<size_t>(sz) != dg_size)) {
    DiscardMalformedMsg(dg_bytes, dg_size, anomaly_tracker,
                        config.NoLogDiscard);
    return TMsg::TPtr();
  }

  int16_t api_key = ReadInt16FromHeader(dg_bytes + INPUT_DG_SZ_FIELD_SIZE);

  /* TODO: remove this temporary hack when it is no longer needed */
  if ((static_cast<uint16_t>(api_key) & 0xff00) != (1 << 8)) {
    return BuildMsgFromOldFormatDg(dg, dg_size, pool, anomaly_tracker,
        msg_state_tracker, config.NoLogDiscard);
  }

  size_t key_part_size = INPUT_DG_SZ_FIELD_SIZE + INPUT_DG_API_KEY_FIELD_SIZE;
  int16_t api_version = ReadInt16FromHeader(dg_bytes + key_part_size);
  const uint8_t *versioned_part_begin = &dg_bytes[fixed_part_size];
  const uint8_t *versioned_part_end = versioned_part_begin +
      (dg_size - fixed_part_size);

  switch (api_key) {
    case ((1 << 8) + 0): {
      return BuildAnyPartitionMsgFromDg(dg_bytes, dg_size, api_version,
          versioned_part_begin, versioned_part_end, pool, anomaly_tracker,
          msg_state_tracker, config.NoLogDiscard);
    }
    case ((1 << 8) + 1): {
      return BuildPartitionKeyMsgFromDg(dg_bytes, dg_size, api_version,
          versioned_part_begin, versioned_part_end, pool, anomaly_tracker,
          msg_state_tracker, config.NoLogDiscard);
    }
    default: {
      break;
    }
  }

  if (!config.NoLogDiscard) {
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Discarding message with unsupported API key: %d",
             static_cast<int>(api_key));
    }
  }

  anomaly_tracker.TrackUnsupportedApiKeyDiscard(dg_bytes, dg_bytes + dg_size,
      api_key);
  InputThreadDiscardMsgUnsupportedApiKey.Increment();
  return TMsg::TPtr();
}
