/* <bruce/kafka_proto/v0/produce_request_writer.cc>

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

   Implements <bruce/kafka_proto/v0/produce_request_writer.h>.
 */

#include <bruce/kafka_proto/v0/produce_request_writer.h>

#include <limits>

#include <bruce/kafka_proto/v0/protocol_util.h>

using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

TProduceRequestWriter::TProduceRequestWriter() {
  Reset();
}

void TProduceRequestWriter::Reset() {
  assert(this);
  Buf = nullptr;
  State = TState::Idle;
  AtOffset = 0;
  TopicCountOffset = 0;
  FirstTopicOffset = 0;
  CurrentTopicOffset = 0;
  CurrentTopicPartitionCountOffset = 0;
  TopicCount = 0;
  FirstPartitionOffset = 0;
  CurrentPartitionOffset = 0;
  PartitionCount = 0;
  MsgSetWriter.Reset();
}

void TProduceRequestWriter::OpenRequest(std::vector<uint8_t> &result_buf,
    int32_t corr_id, const char *client_id_begin, const char *client_id_end,
    int16_t required_acks, int32_t replication_timeout) {
  assert(this);

  /* Make sure we start in a sane state. */
  Reset();

  assert(State == TState::Idle);
  assert(&result_buf);
  assert(client_id_begin || (!client_id_begin && !client_id_end));
  assert(client_id_end >= client_id_begin);
  size_t client_id_len = client_id_end - client_id_begin;
  assert(client_id_len <= std::numeric_limits<int16_t>::max());
  Buf = &result_buf;
  assert(Buf);
  Buf->resize(REQUEST_OR_RESPONSE_SIZE_SIZE + PRC::API_KEY_SIZE +
              PRC::API_VERSION_SIZE + PRC::CORRELATION_ID_SIZE +
              PRC::CLIENT_ID_LEN_SIZE + client_id_len +
              PRC::REQUIRED_ACKS_SIZE + PRC::REPLICATION_TIMEOUT_SIZE +
              PRC::TOPIC_COUNT_SIZE);
  AtOffset = REQUEST_OR_RESPONSE_SIZE_SIZE;  // skip produce request size field
  WriteInt16AtOffset(0);  // API key
  WriteInt16AtOffset(0);  // API version
  WriteInt32AtOffset(corr_id);  // correlation ID

  /* Here, -1 indicates a length of 0. */
  WriteInt16AtOffset(client_id_len ? client_id_len : -1);  // client ID length

  WriteDataAtOffset(client_id_begin, client_id_len);  // client ID
  WriteInt16AtOffset(required_acks);  // required ACKs
  WriteInt32AtOffset(replication_timeout);  // replication timeout
  TopicCountOffset = AtOffset;
  AtOffset += PRC::TOPIC_COUNT_SIZE;  // skip topic count field
  State = TState::InRequest;
}

void TProduceRequestWriter::OpenTopic(const char *topic_name_begin,
    const char *topic_name_end) {
  assert(this);
  assert(State == TState::InRequest);
  assert(topic_name_begin);
  assert(topic_name_end > topic_name_begin);
  size_t topic_name_len = topic_name_end - topic_name_begin;
  assert(Buf);
  FirstPartitionOffset = 0;
  CurrentPartitionOffset = 0;
  PartitionCount = 0;
  Buf->resize(Buf->size() + PRC::TOPIC_NAME_LEN_SIZE + topic_name_len +
              PRC::PARTITION_COUNT_SIZE);  // size of partition count field
  CurrentTopicOffset = AtOffset;

  if (TopicCount == 0) {
    FirstTopicOffset = AtOffset;
  }

  /* Here, -1 indicates a length of 0. */
  WriteInt16AtOffset(topic_name_len ? topic_name_len : -1);

  WriteDataAtOffset(topic_name_begin, topic_name_len);
  CurrentTopicPartitionCountOffset = AtOffset;
  AtOffset += PRC::PARTITION_COUNT_SIZE;  // skip partition count field;
  State = TState::InTopic;
}

void TProduceRequestWriter::OpenMsgSet(int32_t partition) {
  assert(this);
  assert(State == TState::InTopic);
  assert(Buf);
  Buf->resize(Buf->size() + PRC::PARTITION_SIZE + PRC::MSG_SET_SIZE_SIZE);
  CurrentPartitionOffset = AtOffset;

  if (PartitionCount == 0) {
    FirstPartitionOffset = AtOffset;
  }

  WriteInt32AtOffset(partition);
  AtOffset += PRC::MSG_SET_SIZE_SIZE;  // skip message set size field
  MsgSetWriter.OpenMsgSet(*Buf, true);
  State = TState::InMsgSet;
}

void TProduceRequestWriter::OpenMsg(int8_t attributes, size_t key_size,
    size_t value_size) {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  assert(key_size <= std::numeric_limits<int32_t>::max());
  assert(value_size <= std::numeric_limits<int32_t>::max());
  MsgSetWriter.OpenMsg(attributes, key_size, value_size);
}

size_t TProduceRequestWriter::GetCurrentMsgKeyOffset() const {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  return MsgSetWriter.GetCurrentMsgKeyOffset();
}

size_t TProduceRequestWriter::GetCurrentMsgValueOffset() const {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  return MsgSetWriter.GetCurrentMsgValueOffset();
}

void TProduceRequestWriter::AdjustValueSize(size_t new_size) {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  MsgSetWriter.AdjustValueSize(new_size);
}

void TProduceRequestWriter::RollbackOpenMsg() {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  MsgSetWriter.RollbackOpenMsg();
}

void TProduceRequestWriter::CloseMsg() {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  MsgSetWriter.CloseMsg();
}

void TProduceRequestWriter::AddMsg(int8_t attributes, const uint8_t *key_begin,
    const uint8_t *key_end, const uint8_t *value_begin,
    const uint8_t *value_end) {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  MsgSetWriter.AddMsg(attributes, key_begin, key_end, value_begin, value_end);
}

void TProduceRequestWriter::CloseMsgSet() {
  assert(this);
  assert(State == TState::InMsgSet);
  assert(Buf);
  size_t msg_set_size = MsgSetWriter.CloseMsgSet();
  assert((AtOffset + msg_set_size) == Buf->size());
  AtOffset = Buf->size();
  WriteInt32(CurrentPartitionOffset + PRC::PARTITION_SIZE, msg_set_size);
  ++PartitionCount;
  State = TState::InTopic;
}

void TProduceRequestWriter::CloseTopic() {
  assert(this);
  assert(State == TState::InTopic);
  assert(Buf);
  WriteInt32(CurrentTopicPartitionCountOffset, PartitionCount);
  ++TopicCount;
  State = TState::InRequest;
}

void TProduceRequestWriter::CloseRequest() {
  assert(this);
  assert(State == TState::InRequest);
  assert(Buf);
  WriteInt32(TopicCountOffset, TopicCount);
  size_t total_request_size = Buf->size();
  assert(total_request_size > REQUEST_OR_RESPONSE_SIZE_SIZE);

  /* The request size field contains the size of the entire request minus the
     size of the request size field itself. */
  size_t request_size_field_value = total_request_size - 4;
  assert(request_size_field_value <= std::numeric_limits<int32_t>::max());

  WriteInt32(0, request_size_field_value);
  Buf = nullptr;
  State = TState::Idle;
}
