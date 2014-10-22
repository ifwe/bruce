/* <bruce/kafka_proto/v0/msg_set_reader.cc>

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

   Implements <bruce/kafka_proto/v0/msg_set_reader.h>.
 */

#include <bruce/kafka_proto/v0/msg_set_reader.h>

#include <cassert>

#include <base/crc.h>
#include <base/field_access.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

TMsgSetReader::TMsgSetReader() {
  Clear();
}

void TMsgSetReader::Clear() {
  assert(this);
  Begin = nullptr;
  End = nullptr;
  Size = 0;
  CurrentMsg = nullptr;
  CurrentMsgSize = 0;
  CurrentMsgCrcOk = false;
  CurrentMsgKeyBegin = nullptr;
  CurrentMsgKeyEnd = nullptr;
  CurrentMsgValueBegin = nullptr;
  CurrentMsgValueEnd = nullptr;
}

void TMsgSetReader::SetMsgSet(const void *msg_set, size_t msg_set_size) {
  assert(this);
  Clear();
  Begin = reinterpret_cast<const uint8_t *>(msg_set);
  End = Begin + msg_set_size;
  Size = End - Begin;
}

bool TMsgSetReader::FirstMsg() {
  assert(this);
  assert(Begin);
  assert(End >= Begin);
  CurrentMsg = Begin;

  if (CurrentMsg < End) {
    InitCurrentMsg();
    return true;
  }

  return false;
}

bool TMsgSetReader::NextMsg() {
  assert(this);
  assert(Begin);
  assert(End >= Begin);

  if (CurrentMsg == nullptr) {
    return FirstMsg();
  }

  assert(CurrentMsg >= Begin);

  if (CurrentMsg >= End) {
    throw std::range_error(
        "Invalid message location while iterating over Kafka message set");
  }

  CurrentMsg += PRC::MSG_OFFSET_SIZE + PRC::MSG_SIZE_SIZE + CurrentMsgSize;

  if (CurrentMsg > End) {
    THROW_ERROR(TMsgSetTruncated);
  }

  if (CurrentMsg < End) {
    InitCurrentMsg();
    return true;
  }

  CurrentMsgSize = 0;
  CurrentMsgCrcOk = false;
  CurrentMsgKeyBegin = nullptr;
  CurrentMsgKeyEnd = nullptr;
  CurrentMsgValueBegin = nullptr;
  CurrentMsgValueEnd = nullptr;
  return false;
}

bool TMsgSetReader::CurrentMsgCrcIsOk() const {
  assert(this);
  assert((CurrentMsg >= Begin) && (CurrentMsg < End));
  return CurrentMsgCrcOk;
}

uint8_t TMsgSetReader::GetCurrentMsgAttributes() const {
  assert(this);
  assert((CurrentMsg >= Begin) && (CurrentMsg < End));
  return *(CurrentMsg + PRC::MSG_OFFSET_SIZE +
           PRC::MSG_SIZE_SIZE + PRC::CRC_SIZE + PRC::MAGIC_BYTE_SIZE);
}

const uint8_t *TMsgSetReader::GetCurrentMsgKeyBegin() const {
  assert(this);
  assert((CurrentMsg >= Begin) && (CurrentMsg < End));
  return CurrentMsgKeyBegin;
}

const uint8_t *TMsgSetReader::GetCurrentMsgKeyEnd() const {
  assert(this);
  assert((CurrentMsg >= Begin) && (CurrentMsg < End));
  return CurrentMsgKeyEnd;
}

const uint8_t *TMsgSetReader::GetCurrentMsgValueBegin() const {
  assert(this);
  assert((CurrentMsg >= Begin) && (CurrentMsg < End));
  return CurrentMsgValueBegin;
}

const uint8_t *TMsgSetReader::GetCurrentMsgValueEnd() const {
  assert(this);
  assert((CurrentMsg >= Begin) && (CurrentMsg < End));
  return CurrentMsgValueEnd;
}

void TMsgSetReader::InitCurrentMsg() {
  assert(this);
  assert(Begin);
  assert(End > Begin);
  assert(CurrentMsg >= Begin);
  const uint8_t *msg_size_field =
      CurrentMsg + PRC::MSG_OFFSET_SIZE;
  const uint8_t *msg_start = msg_size_field + PRC::MSG_SIZE_SIZE;

  if (msg_start > End) {
    THROW_ERROR(TMsgSetTruncated);
  }

  CurrentMsgSize = ReadInt32FromHeader(msg_size_field);

  if (CurrentMsgSize < PRC::MIN_MSG_SIZE) {
    THROW_ERROR(TBadMsgSize);
  }

  if ((msg_start + CurrentMsgSize) > End) {
    THROW_ERROR(TMsgSetTruncated);
  }

  uint32_t crc = ComputeCrc32(msg_start + PRC::CRC_SIZE,
                              CurrentMsgSize - PRC::CRC_SIZE);
  uint32_t expected_crc = ReadUint32FromHeader(msg_start);
  CurrentMsgCrcOk = (crc == expected_crc);

  if (CurrentMsgCrcOk) {
    int32_t key_size = ReadInt32FromHeader(msg_start + PRC::CRC_SIZE +
        PRC::MAGIC_BYTE_SIZE + PRC::ATTRIBUTES_SIZE);

    /* A value of -1 indicates a length of 0. */
    if (key_size == -1) {
      key_size = 0;
    }

    if ((key_size < 0) || ((PRC::MIN_MSG_SIZE + key_size) > CurrentMsgSize)) {
      THROW_ERROR(TBadMsgKeySize);
    }

    CurrentMsgKeyBegin = msg_start + PRC::CRC_SIZE + PRC::MAGIC_BYTE_SIZE +
                         PRC::ATTRIBUTES_SIZE + PRC::KEY_LEN_SIZE;
    CurrentMsgKeyEnd = CurrentMsgKeyBegin + key_size;
    int32_t value_size = ReadInt32FromHeader(CurrentMsgKeyEnd);

    /* A value of -1 indicates a length of 0. */
    if (value_size == -1) {
      value_size = 0;
    }

    if ((value_size < 0) ||
        ((PRC::MIN_MSG_SIZE + key_size + value_size) != CurrentMsgSize)) {
      THROW_ERROR(TBadMsgValueSize);
    }

    CurrentMsgValueBegin = CurrentMsgKeyEnd + PRC::VALUE_LEN_SIZE;
    CurrentMsgValueEnd = CurrentMsgValueBegin + value_size;
  } else {
    CurrentMsgKeyBegin = nullptr;
    CurrentMsgKeyEnd = nullptr;
    CurrentMsgValueBegin = nullptr;
    CurrentMsgValueEnd = nullptr;
  }
}
