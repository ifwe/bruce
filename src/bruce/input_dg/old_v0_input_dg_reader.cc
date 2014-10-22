/* <bruce/input_dg/old_v0_input_dg_reader.cc>

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

   Implements <bruce/input_dg/old_v0_input_dg_reader.h>.
 */

#include <bruce/input_dg/old_v0_input_dg_reader.h>

#include <algorithm>

#include <base/field_access.h>
#include <bruce/input_dg/input_dg_common.h>
#include <bruce/input_dg/old_v0_input_dg_common.h>
#include <bruce/msg_creator.h>
#include <capped/memory_cap_reached.h>

using namespace Bruce;
using namespace Bruce::InputDg;
using namespace Capped;

TMsg::TPtr TOldV0InputDgReader::BuildMsg() {
  assert(this);
  const uint8_t *pos = DataBegin;

  if (DataSize < (OLD_V0_TS_FIELD_SIZE + OLD_V0_TOPIC_SZ_FIELD_SIZE)) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  TMsg::TTimestamp timestamp = ReadInt64FromHeader(pos);
  pos += OLD_V0_TS_FIELD_SIZE;
  int8_t topic_size = *pos;
  pos += OLD_V0_TOPIC_SZ_FIELD_SIZE;
  size_t bytes_left = DataSize - OLD_V0_TS_FIELD_SIZE -
                      OLD_V0_TOPIC_SZ_FIELD_SIZE;

  if (topic_size < 0) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  if (static_cast<size_t>(topic_size + OLD_V0_BODY_SZ_FIELD_SIZE) >
      bytes_left) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  const char *topic_begin = reinterpret_cast<const char *>(pos);
  pos += topic_size;
  const char *topic_end = reinterpret_cast<const char *>(pos);
  bytes_left -= topic_size;
  int32_t body_size = ReadInt32FromHeader(pos);
  pos += OLD_V0_BODY_SZ_FIELD_SIZE;
  bytes_left -= OLD_V0_BODY_SZ_FIELD_SIZE;

  if ((body_size < 0) || (static_cast<size_t>(body_size) != bytes_left)) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  const uint8_t *body_begin = pos;
  TMsg::TPtr msg;

  try {
    msg = TMsgCreator::CreateAnyPartitionMsg(timestamp, topic_begin, topic_end,
        nullptr, 0, body_begin, body_size, false, Pool, MsgStateTracker);
  } catch (const TMemoryCapReached &) {
    /* Memory cap prevented message creation.  Report discard below. */
  }

  if (!msg) {
    DiscardMsgNoMem(timestamp, topic_begin, topic_end, nullptr, nullptr,
        body_begin, body_begin + body_size, AnomalyTracker, NoLogDiscard);
  }

  return std::move(msg);
}
