/* <bruce/kafka_proto/v0/msg_set_writer.cc>

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

   Implements <bruce/kafka_proto/v0/msg_set_writer.h>.
 */

#include <bruce/kafka_proto/v0/msg_set_writer.h>

#include <limits>

#include <base/crc.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

TMsgSetWriter::TMsgSetWriter() {
  Reset();
}

void TMsgSetWriter::Reset() {
  assert(this);
  Buf = nullptr;
  State = TState::Idle;
  AtOffset = 0;
  MsgSetSize = 0;
  FirstMsgSetItemOffset = 0;
  CurrentMsgSetItemOffset = 0;
  MsgSetItemCount = 0;
  CurrentMsgCrcOffset = 0;
  CurrentMsgKeyOffset = 0;
  CurrentMsgValueOffset = 0;
  CurrentMsgKeySize = 0;
  CurrentMsgValueSize = 0;
}

void TMsgSetWriter::OpenMsgSet(std::vector<uint8_t> &result_buf, bool append) {
  assert(this);

  /* Make sure we start in a sane state.  This guards against cases where an
     exception previously thrown by this object leaves it in a bad state and we
     later reuse it for another produce request. */
  Reset();

  assert(State == TState::Idle);
  assert(&result_buf);

  if (!append) {
    result_buf.clear();
  }

  Buf = &result_buf;
  AtOffset = Buf->size();
  FirstMsgSetItemOffset = AtOffset;
  State = TState::InMsgSet;
}

void TMsgSetWriter::OpenMsg(int8_t attributes, size_t key_size,
    size_t value_size) {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  assert(key_size <= std::numeric_limits<int32_t>::max());
  assert(value_size <= std::numeric_limits<int32_t>::max());
  size_t msg_minus_value_size = ComputeMsgMinusValueSize(key_size);
  size_t msg_size = msg_minus_value_size + value_size;
  size_t msg_set_item_size = ComputeMsgSetItemSize(msg_size);
  assert(AtOffset == Buf->size());
  Buf->resize(Buf->size() + msg_set_item_size);
  CurrentMsgSetItemOffset = AtOffset;
  WriteInt64AtOffset(0);  // message offset (fill in any value)
  AtOffset += PRC::MSG_SIZE_SIZE;  // skip message size field
  CurrentMsgCrcOffset = AtOffset;
  AtOffset += PRC::CRC_SIZE;  // skip CRC field
  WriteInt8AtOffset(0);  // magic byte
  WriteInt8AtOffset(attributes);  // attributes

  /* Here, -1 indicates a length of 0. */
  WriteInt32AtOffset(key_size ? key_size : -1);  // key length

  CurrentMsgKeyOffset = AtOffset;  // key goes here
  AtOffset += key_size;  // skip space for key
  AtOffset += PRC::VALUE_LEN_SIZE;  // skip value size field
  CurrentMsgValueOffset = AtOffset;  // value goes here
  CurrentMsgKeySize = key_size;
  CurrentMsgValueSize = value_size;
  State = TState::InMsg;
}

size_t TMsgSetWriter::GetCurrentMsgKeyOffset() const {
  assert(this);
  assert(State == TState::InMsg);
  assert(Buf);
  assert(CurrentMsgKeyOffset > CurrentMsgSetItemOffset);
  return CurrentMsgKeyOffset;
}

size_t TMsgSetWriter::GetCurrentMsgValueOffset() const {
  assert(this);
  assert(State == TState::InMsg);
  assert(Buf);
  assert(CurrentMsgValueOffset > CurrentMsgSetItemOffset);
  return CurrentMsgValueOffset;
}

void TMsgSetWriter::AdjustValueSize(size_t new_size) {
  assert(this);
  assert(State == TState::InMsg);
  assert(Buf->size() > CurrentMsgValueSize);
  size_t size_of_buf_minus_value = Buf->size() - CurrentMsgValueSize;
  Buf->resize(size_of_buf_minus_value + new_size);
  CurrentMsgValueSize = new_size;
}

void TMsgSetWriter::RollbackOpenMsg() {
  assert(this);
  assert(State == TState::InMsg);
  assert(Buf);
  assert(CurrentMsgCrcOffset > CurrentMsgSetItemOffset);
  assert(CurrentMsgKeyOffset > CurrentMsgSetItemOffset);
  assert(CurrentMsgValueOffset > CurrentMsgSetItemOffset);
  Buf->resize(CurrentMsgSetItemOffset);
  AtOffset = CurrentMsgSetItemOffset;
  CurrentMsgSetItemOffset = 0;
  CurrentMsgCrcOffset = 0;
  CurrentMsgKeyOffset = 0;
  CurrentMsgValueOffset = 0;
  CurrentMsgKeySize = 0;
  CurrentMsgValueSize = 0;
  State = TState::InMsgSet;
}

void TMsgSetWriter::CloseMsg() {
  assert(this);
  assert(State == TState::InMsg);
  assert(Buf);
  assert(CurrentMsgCrcOffset > CurrentMsgSetItemOffset);
  assert(CurrentMsgKeyOffset > CurrentMsgSetItemOffset);
  assert(CurrentMsgValueOffset > CurrentMsgSetItemOffset);
  size_t msg_size = ComputeMsgMinusValueSize(CurrentMsgKeySize) +
                    CurrentMsgValueSize;
  assert(Buf->size() >= CurrentMsgValueOffset);
  assert((Buf->size() - CurrentMsgValueOffset) == CurrentMsgValueSize);
  WriteInt32(CurrentMsgSetItemOffset + PRC::MSG_OFFSET_SIZE, msg_size);

  /* Here, -1 indicates a length of 0. */
  WriteInt32(CurrentMsgKeyOffset + CurrentMsgKeySize,
             CurrentMsgValueSize ? CurrentMsgValueSize : -1);  // value length

  AtOffset += CurrentMsgValueSize;  // skip past value
  MsgSetSize += ComputeMsgSetItemSize(msg_size);
  assert(msg_size > PRC::CRC_SIZE);
  size_t crc_area_size = msg_size - PRC::CRC_SIZE;
  uint32_t crc = ComputeCrc32(&(*Buf)[CurrentMsgCrcOffset + PRC::CRC_SIZE],
                              crc_area_size);
  WriteInt32(CurrentMsgCrcOffset, static_cast<int32_t>(crc));
  CurrentMsgSetItemOffset = 0;
  CurrentMsgCrcOffset = 0;
  CurrentMsgKeyOffset = 0;
  CurrentMsgValueOffset = 0;
  CurrentMsgKeySize = 0;
  CurrentMsgValueSize = 0;
  ++MsgSetItemCount;
  State = TState::InMsgSet;
}

void TMsgSetWriter::AddMsg(int8_t attributes, const uint8_t *key_begin,
    const uint8_t *key_end, const uint8_t *value_begin,
    const uint8_t *value_end) {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  assert(key_begin || (!key_begin && !key_end));
  assert(key_end >= key_begin);
  size_t key_size = key_end - key_begin;
  assert(key_size <= std::numeric_limits<int32_t>::max());
  assert(value_begin || (!value_begin && !value_end));
  assert(value_end >= value_begin);
  size_t value_size = value_end - value_begin;
  assert(value_size <= std::numeric_limits<int32_t>::max());
  OpenMsg(attributes, key_size, value_size);

  if (key_size) {
    std::memcpy(&(*Buf)[GetCurrentMsgKeyOffset()], key_begin, key_size);
  }

  if (value_size) {
    std::memcpy(&(*Buf)[GetCurrentMsgValueOffset()], value_begin, value_size);
  }

  CloseMsg();
}

size_t TMsgSetWriter::CloseMsgSet() {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  assert(AtOffset >= FirstMsgSetItemOffset);
  assert(MsgSetSize == (AtOffset - FirstMsgSetItemOffset));
  State = TState::Idle;
  assert(MsgSetSize <= std::numeric_limits<int32_t>::max());
  return MsgSetSize;
}
