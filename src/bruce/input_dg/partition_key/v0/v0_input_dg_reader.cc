/* <bruce/input_dg/partition_key/v0/v0_input_dg_reader.cc>

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

   Implements <bruce/input_dg/partition_key/v0/v0_input_dg_reader.h>.
 */

#include <bruce/input_dg/partition_key/v0/v0_input_dg_reader.h>

#include <base/field_access.h>
#include <bruce/input_dg/input_dg_common.h>
#include <bruce/msg_creator.h>

using namespace Bruce;
using namespace Bruce::InputDg;
using namespace Bruce::InputDg::PartitionKey;
using namespace Bruce::InputDg::PartitionKey::V0;

TMsg::TPtr TV0InputDgReader::BuildMsg() {
  assert(this);
  const uint8_t *pos = DataBegin;

  if ((DataEnd - pos) < INPUT_DG_P_KEY_V0_FLAGS_FIELD_SIZE) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  int16_t flags = ReadInt16FromHeader(pos);

  if (flags) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  pos += INPUT_DG_P_KEY_V0_FLAGS_FIELD_SIZE;

  if ((DataEnd - pos) < INPUT_DG_P_KEY_V0_PARTITION_KEY_FIELD_SIZE) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  int32_t partition_key = ReadInt32FromHeader(pos);

  pos += INPUT_DG_P_KEY_V0_PARTITION_KEY_FIELD_SIZE;

  if ((DataEnd - pos) < INPUT_DG_P_KEY_V0_TOPIC_SZ_FIELD_SIZE) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  int16_t topic_sz = ReadInt16FromHeader(pos);

  if (topic_sz <= 0) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  pos += INPUT_DG_P_KEY_V0_TOPIC_SZ_FIELD_SIZE;

  if ((DataEnd - pos) < topic_sz) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  const char *topic_begin = reinterpret_cast<const char *>(pos);
  const char *topic_end = topic_begin + topic_sz;
  pos = reinterpret_cast<const uint8_t *>(topic_end);

  if ((DataEnd - pos) < INPUT_DG_P_KEY_V0_TS_FIELD_SIZE) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  int64_t ts = ReadInt64FromHeader(pos);
  pos += INPUT_DG_P_KEY_V0_TS_FIELD_SIZE;

  if ((DataEnd - pos) < INPUT_DG_P_KEY_V0_KEY_SZ_FIELD_SIZE) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  int32_t key_sz = ReadInt32FromHeader(pos);

  if (key_sz < 0) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  pos += INPUT_DG_P_KEY_V0_KEY_SZ_FIELD_SIZE;

  if ((DataEnd - pos) < key_sz) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  const uint8_t *key_begin = pos;

  pos += key_sz;

  if ((DataEnd - pos) < INPUT_DG_P_KEY_V0_VALUE_SZ_FIELD_SIZE) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  int32_t value_sz = ReadInt32FromHeader(pos);

  if (value_sz < 0) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  pos += INPUT_DG_P_KEY_V0_VALUE_SZ_FIELD_SIZE;

  if ((DataEnd - pos) != value_sz) {
    DiscardMalformedMsg(DgBegin, DgSize, AnomalyTracker, NoLogDiscard);
    return TMsg::TPtr();
  }

  const uint8_t *value_begin = pos;
  return TryCreatePartitionKeyMsg(partition_key, ts, topic_begin, topic_end,
      key_begin, key_sz, value_begin, value_sz, Pool, AnomalyTracker,
      MsgStateTracker, NoLogDiscard);
}
