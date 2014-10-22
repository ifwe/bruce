/* <bruce/kafka_proto/v0/produce_request_reader.cc>

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

   Implements <bruce/kafka_proto/v0/produce_request_reader.h>.
 */

#include <bruce/kafka_proto/v0/produce_request_reader.h>

#include <cassert>

#include <base/field_access.h>

using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

TProduceRequestReader::TProduceRequestReader() {
  Clear();
}

void TProduceRequestReader::Clear() {
  assert(this);
  Begin = nullptr;
  End = nullptr;
  Size = 0;
  ClientIdLen = 0;
  RequiredAcksOffset = 0;
  NumTopics = 0;
  CurrentTopicIndex = -1;
  CurrentTopicBegin = nullptr;
  CurrentTopicNameEnd = nullptr;
  NumPartitionsInTopic = 0;
  CurrentPartitionIndexInTopic = -1;
  CurrentPartitionBegin = nullptr;
  PartitionMsgSetBegin = nullptr;
  PartitionMsgSetEnd = nullptr;
  MsgSetReader.Clear();
}

void TProduceRequestReader::SetRequest(const void *request,
    size_t request_size) {
  assert(this);
  Clear();
  Begin = reinterpret_cast<const uint8_t *>(request);
  End = Begin + GetRequestOrResponseSize(Begin);
  Size = End - Begin;

  if (Size < MinSize()) {
    THROW_ERROR(TBadRequestSize);
  }

  if ((Begin + request_size) < End) {
    THROW_ERROR(TRequestTruncated);
  }

  if (ReadInt16FromHeader(Begin + REQUEST_OR_RESPONSE_SIZE_SIZE)) {
    THROW_ERROR(TBadApiKey);
  }

  if (ReadInt16FromHeader(Begin + REQUEST_OR_RESPONSE_SIZE_SIZE +
                          PRC::API_KEY_SIZE)) {
    THROW_ERROR(TBadApiVersion);
  }

  size_t client_id_len_offset = REQUEST_OR_RESPONSE_SIZE_SIZE +
      PRC::API_KEY_SIZE + PRC::API_VERSION_SIZE + PRC::CORRELATION_ID_SIZE;

  ClientIdLen = ReadInt16FromHeader(Begin + client_id_len_offset);

  /* A value of -1 indicates a length of 0. */
  if (ClientIdLen == -1) {
    ClientIdLen = 0;
  }

  if (ClientIdLen < 0) {
    THROW_ERROR(TBadClientIdLen);
  }

  if (Size < (MinSize() + ClientIdLen)) {
    THROW_ERROR(TBadRequestSize);
  }

  RequiredAcksOffset = client_id_len_offset + PRC::CLIENT_ID_LEN_SIZE +
                       ClientIdLen;
  NumTopics = ReadInt32FromHeader(Begin + RequiredAcksOffset +
      PRC::REQUIRED_ACKS_SIZE + PRC::REPLICATION_TIMEOUT_SIZE);

  if (NumTopics < 0) {
    THROW_ERROR(TBadTopicCount);
  }
}

int32_t TProduceRequestReader::GetCorrelationId() const {
  assert(this);
  return ReadInt32FromHeader(Begin + REQUEST_OR_RESPONSE_SIZE_SIZE +
      PRC::API_KEY_SIZE + PRC::API_VERSION_SIZE);
}

const char *TProduceRequestReader::GetClientIdBegin() const {
  assert(this);
  return reinterpret_cast<const char *>(Begin) +
      REQUEST_OR_RESPONSE_SIZE_SIZE + PRC::API_KEY_SIZE +
      PRC::API_VERSION_SIZE + PRC::CORRELATION_ID_SIZE +
      PRC::CLIENT_ID_LEN_SIZE;
}

const char *TProduceRequestReader::GetClientIdEnd() const {
  assert(this);
  return GetClientIdBegin() + ClientIdLen;
}

int16_t TProduceRequestReader::GetRequiredAcks() const {
  assert(this);
  return ReadInt16FromHeader(GetClientIdEnd());
}

int32_t TProduceRequestReader::GetReplicationTimeout() const {
  assert(this);
  return ReadInt32FromHeader(GetClientIdEnd() + PRC::REQUIRED_ACKS_SIZE);
}

size_t TProduceRequestReader::GetNumTopics() const {
  assert(this);
  return NumTopics;
}

bool TProduceRequestReader::FirstTopic() {
  assert(this);
  assert(Begin);
  assert(End > Begin);
  assert(NumTopics >= 0);
  CurrentTopicIndex = 0;
  CurrentTopicBegin = reinterpret_cast<const uint8_t *>(GetClientIdEnd()) +
                      PRC::REQUIRED_ACKS_SIZE + PRC::REPLICATION_TIMEOUT_SIZE +
                      PRC::TOPIC_COUNT_SIZE;

  if (NumTopics > 0) {
    InitCurrentTopic();
    return true;
  }

  return false;
}

bool TProduceRequestReader::NextTopic() {
  assert(this);
  assert(Begin);
  assert(End > Begin);
  assert(NumTopics >= 0);
  assert(CurrentTopicIndex >= -1);

  if (CurrentTopicIndex < 0) {
    return FirstTopic();
  }

  if (CurrentTopicIndex >= NumTopics) {
    throw std::range_error(
        "Invalid topic index while iterating over Kafka produce request");
  }

  assert(CurrentTopicBegin > Begin);
  assert(CurrentTopicNameEnd > CurrentTopicBegin);

  /* Skip past all remaining partitions in current topic. */

  bool not_at_end = (CurrentPartitionIndexInTopic == -1) ?
      FirstMsgSetInTopic() :
      (CurrentPartitionIndexInTopic < NumPartitionsInTopic);

  while (not_at_end) {
    not_at_end = NextMsgSetInTopic();
  }

  /* The start of the next topic is where the start of the next partition in
     this topic would be, if there was another partition. */
  CurrentTopicBegin = CurrentPartitionBegin;

  if (++CurrentTopicIndex < NumTopics) {
    InitCurrentTopic();
    return true;
  }

  return false;
}

const char *TProduceRequestReader::GetCurrentTopicNameBegin() const {
  assert(this);
  assert((CurrentTopicBegin > Begin) && (CurrentTopicBegin < End));
  return reinterpret_cast<const char *>(CurrentTopicBegin) +
         PRC::TOPIC_NAME_LEN_SIZE;
}

const char *TProduceRequestReader::GetCurrentTopicNameEnd() const {
  assert(this);
  assert((CurrentTopicNameEnd > Begin) && (CurrentTopicNameEnd < End));
  return reinterpret_cast<const char *>(CurrentTopicNameEnd);
}

size_t TProduceRequestReader::GetNumMsgSetsInCurrentTopic() const {
  assert(this);
  assert((CurrentTopicNameEnd > Begin) && (CurrentTopicNameEnd < End));
  return NumPartitionsInTopic;
}

bool TProduceRequestReader::FirstMsgSetInTopic() {
  assert(this);
  assert(Begin);
  assert(End > Begin);
  assert((CurrentTopicIndex >= 0) && (CurrentTopicIndex < NumTopics));
  assert(CurrentTopicBegin > Begin);
  assert(CurrentTopicNameEnd > CurrentTopicBegin);
  assert(NumPartitionsInTopic >= 0);
  CurrentPartitionIndexInTopic = 0;
  CurrentPartitionBegin = CurrentTopicNameEnd + PRC::PARTITION_COUNT_SIZE;

  if (NumPartitionsInTopic > 0) {
    InitCurrentPartition();
    return true;
  }

  return false;
}

bool TProduceRequestReader::NextMsgSetInTopic() {
  assert(this);
  assert(Begin);
  assert(End > Begin);
  assert(CurrentTopicBegin > Begin);
  assert(CurrentTopicNameEnd > CurrentTopicBegin);
  assert(NumPartitionsInTopic >= 0);

  if (CurrentPartitionIndexInTopic < 0) {
    return FirstMsgSetInTopic();
  }

  if (CurrentPartitionIndexInTopic >= NumPartitionsInTopic) {
    throw std::range_error(
        "Invalid partition index while iterating over Kafka produce request");
  }

  assert(CurrentPartitionBegin > CurrentTopicNameEnd);

  /* The start of the next partition (and associated message set) is the end of
     the message set in the current partition. */
  CurrentPartitionBegin = PartitionMsgSetEnd;

  if (++CurrentPartitionIndexInTopic < NumPartitionsInTopic) {
    InitCurrentPartition();
    return true;
  }

  MsgSetReader.Clear();
  return false;
}

int32_t TProduceRequestReader::GetPartitionOfCurrentMsgSet() const {
  assert(this);
  assert((CurrentPartitionBegin > Begin) && (CurrentPartitionBegin < End));
  return ReadInt32FromHeader(CurrentPartitionBegin);
}

bool TProduceRequestReader::FirstMsgInMsgSet() {
  assert(this);
  assert(Begin);
  assert(End > Begin);
  assert((CurrentTopicIndex >= 0) && (CurrentTopicIndex < NumTopics));
  assert(CurrentTopicBegin > Begin);
  assert(CurrentTopicNameEnd > CurrentTopicBegin);
  assert(NumPartitionsInTopic >= 0);
  assert((CurrentPartitionIndexInTopic >= 0) &&
         (CurrentPartitionIndexInTopic < NumPartitionsInTopic));
  assert(CurrentPartitionBegin > CurrentTopicNameEnd);
  assert(PartitionMsgSetBegin > CurrentPartitionBegin);
  assert(PartitionMsgSetEnd >= PartitionMsgSetBegin);
  return MsgSetReader.FirstMsg();
}

bool TProduceRequestReader::NextMsgInMsgSet() {
  assert(this);
  assert(Begin);
  assert(End > Begin);
  assert((CurrentTopicIndex >= 0) && (CurrentTopicIndex < NumTopics));
  assert(CurrentTopicBegin > Begin);
  assert(CurrentTopicNameEnd > CurrentTopicBegin);
  assert(NumPartitionsInTopic >= 0);
  assert((CurrentPartitionIndexInTopic >= 0) &&
         (CurrentPartitionIndexInTopic < NumPartitionsInTopic));
  assert(CurrentPartitionBegin > CurrentTopicNameEnd);
  assert(PartitionMsgSetBegin > CurrentPartitionBegin);
  assert(PartitionMsgSetEnd >= PartitionMsgSetBegin);
  return MsgSetReader.NextMsg();
}

bool TProduceRequestReader::CurrentMsgCrcIsOk() const {
  assert(this);
  return MsgSetReader.CurrentMsgCrcIsOk();
}

uint8_t TProduceRequestReader::GetCurrentMsgAttributes() const {
  assert(this);
  return MsgSetReader.GetCurrentMsgAttributes();
}

const uint8_t *TProduceRequestReader::GetCurrentMsgKeyBegin() const {
  assert(this);
  return MsgSetReader.GetCurrentMsgKeyBegin();
}

const uint8_t *TProduceRequestReader::GetCurrentMsgKeyEnd() const {
  assert(this);
  return MsgSetReader.GetCurrentMsgKeyEnd();
}

const uint8_t *TProduceRequestReader::GetCurrentMsgValueBegin() const {
  assert(this);
  return MsgSetReader.GetCurrentMsgValueBegin();
}

const uint8_t *TProduceRequestReader::GetCurrentMsgValueEnd() const {
  assert(this);
  return MsgSetReader.GetCurrentMsgValueEnd();
}

void TProduceRequestReader::InitCurrentTopic() {
  assert(this);
  assert(Begin);
  assert(End > Begin);
  assert(CurrentTopicBegin > Begin);

  if ((CurrentTopicBegin + PRC::TOPIC_NAME_LEN_SIZE) > End) {
    THROW_ERROR(TRequestTruncated);
  }

  int16_t topic_name_len = ReadInt16FromHeader(CurrentTopicBegin);

  /* A value of -1 indicates a length of 0. */
  if (topic_name_len == -1) {
    topic_name_len = 0;
  }

  if (topic_name_len < 0) {
    THROW_ERROR(TBadTopicNameLen);
  }

  CurrentTopicNameEnd = CurrentTopicBegin + PRC::TOPIC_NAME_LEN_SIZE +
                        topic_name_len;

  if ((CurrentTopicNameEnd + PRC::PARTITION_COUNT_SIZE) > End) {
    THROW_ERROR(TRequestTruncated);
  }

  NumPartitionsInTopic = ReadInt32FromHeader(CurrentTopicNameEnd);

  if (NumPartitionsInTopic < 0) {
    THROW_ERROR(TBadPartitionCount);
  }

  CurrentPartitionIndexInTopic = -1;
  CurrentPartitionBegin = nullptr;
  PartitionMsgSetBegin = nullptr;
  PartitionMsgSetEnd = nullptr;
}

void TProduceRequestReader::InitCurrentPartition() {
  assert(this);
  assert(Begin);
  assert(End > Begin);
  assert(CurrentPartitionBegin > Begin);
  PartitionMsgSetBegin = CurrentPartitionBegin + PRC::PARTITION_SIZE +
                         PRC::MSG_SET_SIZE_SIZE;

  if (PartitionMsgSetBegin > End) {
    THROW_ERROR(TRequestTruncated);
  }

  int32_t msg_set_size =
      ReadInt32FromHeader(CurrentPartitionBegin + PRC::PARTITION_SIZE);
  PartitionMsgSetEnd = PartitionMsgSetBegin + msg_set_size;

  if (PartitionMsgSetEnd > End) {
    THROW_ERROR(TRequestTruncated);
  }

  MsgSetReader.SetMsgSet(PartitionMsgSetBegin, msg_set_size);
}
