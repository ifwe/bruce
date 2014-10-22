/* <bruce/kafka_proto/v0/metadata_response_writer.cc>

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

   Implements <bruce/kafka_proto/v0/metadata_response_writer.h>.
 */

#include <bruce/kafka_proto/v0/metadata_response_writer.h>

#include <cstring>
#include <limits>

#include <base/field_access.h>
#include <bruce/kafka_proto/v0/protocol_util.h>

using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

TMetadataResponseWriter::TMetadataResponseWriter()
    : State(TState::Initial),
      Result(0),
      BrokerCount(0),
      BrokerOffset(0),
      TopicCountOffset(0),
      TopicCount(0),
      TopicOffset(0),
      PartitionCountOffset(0),
      PartitionCount(0),
      PartitionOffset(0),
      ReplicaCountOffset(0),
      ReplicaCount(0),
      CaughtUpReplicaCountOffset(0),
      CaughtUpReplicaCount(0) {
}

void TMetadataResponseWriter::OpenResponse(std::vector<uint8_t> &out,
    int32_t correlation_id) {
  assert(this);
  assert(State == TState::Initial);
  assert(Result == nullptr);
  State = TState::InResponse;
  ClearBookkeepingInfo();
  Result = &out;
  Result->resize(REQUEST_OR_RESPONSE_SIZE_SIZE + THdr::CORRELATION_ID_SIZE +
                 THdr::BROKER_COUNT_SIZE);
  WriteInt32ToHeader(Loc(THdr::CORRELATION_ID_OFFSET), correlation_id);
}

void TMetadataResponseWriter::OpenBrokerList() {
  assert(this);
  assert(State == TState::InResponse);
  State = TState::InBrokerList;
  BrokerCount = 0;
  BrokerOffset = THdr::BROKER_LIST_OFFSET;
  assert(Size() == static_cast<size_t>(BrokerOffset));
}

void TMetadataResponseWriter::AddBroker(int32_t node_id,
    const char *host_begin, const char *host_end, int32_t port) {
  assert(this);
  assert(host_begin);
  assert(host_end > host_begin);
  assert(State == TState::InBrokerList);
  size_t host_size = host_end - host_begin;
  size_t broker_size = THdr::BrokerListItemSize(host_size);
  GrowResult(broker_size);
  WriteInt32ToHeader(
      Loc(BrokerOffset + THdr::REL_BROKER_NODE_ID_OFFSET), node_id);
  SetStr(BrokerOffset + THdr::REL_BROKER_HOST_LENGTH_OFFSET, host_begin,
         host_end);
  size_t port_offset = BrokerOffset +
      THdr::RelBrokerPortOffset(host_end - host_begin);
  WriteInt32ToHeader(Loc(port_offset), port);
  ++BrokerCount;
  BrokerOffset += broker_size;
  assert(Size() == static_cast<size_t>(BrokerOffset));
}

void TMetadataResponseWriter::CloseBrokerList() {
  assert(this);
  assert(State == TState::InBrokerList);
  State = TState::InResponse;
  WriteInt32ToHeader(Loc(THdr::BROKER_COUNT_OFFSET), BrokerCount);
}

void TMetadataResponseWriter::OpenTopicList() {
  assert(this);
  assert(State == TState::InResponse);
  State = TState::InTopicList;
  GrowResult(THdr::TOPIC_COUNT_SIZE);
  TopicCountOffset = BrokerOffset;
  TopicCount = 0;
  TopicOffset = Size();
  assert(TopicOffset == static_cast<int32_t>(TopicCountOffset +
                                             THdr::TOPIC_COUNT_SIZE));
}

void TMetadataResponseWriter::OpenTopic(int16_t topic_error_code,
    const char *topic_name_begin, const char *topic_name_end) {
  assert(this);
  assert(State == TState::InTopicList);
  assert(topic_name_begin);
  assert(topic_name_end > topic_name_begin);
  State = TState::InTopic;
  size_t topic_name_size = topic_name_end - topic_name_begin;
  GrowResult(THdr::TOPIC_ERROR_CODE_SIZE + THdr::TOPIC_NAME_LENGTH_SIZE +
             topic_name_size + THdr::PARTITION_COUNT_SIZE);
  WriteInt16ToHeader(Loc(TopicOffset + THdr::REL_TOPIC_ERROR_CODE_OFFSET),
                     topic_error_code);
  SetStr(TopicOffset + THdr::REL_TOPIC_NAME_LENGTH_OFFSET, topic_name_begin,
         topic_name_end);
  PartitionCountOffset = TopicOffset + THdr::TOPIC_ERROR_CODE_SIZE +
      THdr::TOPIC_NAME_LENGTH_SIZE + topic_name_size;
  assert(Size() == (PartitionCountOffset + THdr::PARTITION_COUNT_SIZE));
}

void TMetadataResponseWriter::OpenPartitionList() {
  assert(this);
  assert(State == TState::InTopic);
  State = TState::InPartitionList;
  PartitionCount = 0;
  PartitionOffset = Size();
}

void TMetadataResponseWriter::OpenPartition(int16_t partition_error_code,
    int32_t partition_id, int32_t leader_node_id) {
  assert(this);
  assert(State == TState::InPartitionList);
  State = TState::InPartition;
  size_t replica_count_delta = THdr::PARTITION_ERROR_CODE_SIZE +
      THdr::PARTITION_ID_SIZE + THdr::LEADER_NODE_ID_SIZE;
  GrowResult(replica_count_delta + THdr::REPLICA_COUNT_SIZE);
  ReplicaCountOffset = PartitionOffset + replica_count_delta;
  WriteInt16ToHeader(
      Loc(PartitionOffset + THdr::REL_PARTITION_ERROR_CODE_OFFSET),
      partition_error_code);
  WriteInt32ToHeader(Loc(PartitionOffset + THdr::REL_PARTITION_ID_OFFSET),
                     partition_id);
  WriteInt32ToHeader(Loc(PartitionOffset + THdr::REL_LEADER_NODE_ID_OFFSET),
                     leader_node_id);
}

void TMetadataResponseWriter::OpenReplicaList() {
  assert(this);
  assert(State == TState::InPartition);
  State = TState::InReplicaList;
  ReplicaCount = 0;
}

void TMetadataResponseWriter::AddReplica(int32_t replica_node_id) {
  assert(this);
  assert(State == TState::InReplicaList);
  size_t offset = Size();
  GrowResult(THdr::REPLICA_NODE_ID_SIZE);
  WriteInt32ToHeader(Loc(offset), replica_node_id);
  ++ReplicaCount;
}

void TMetadataResponseWriter::CloseReplicaList() {
  assert(this);
  assert(State == TState::InReplicaList);
  State = TState::InPartition;
  CaughtUpReplicaCountOffset = Size();
  WriteInt32ToHeader(Loc(ReplicaCountOffset), ReplicaCount);
  GrowResult(THdr::CAUGHT_UP_REPLICA_COUNT_SIZE);
}

void TMetadataResponseWriter::OpenCaughtUpReplicaList() {
  assert(this);
  assert(State == TState::InPartition);
  State = TState::InCaughtUpReplicaList;
  CaughtUpReplicaCount = 0;
}

void TMetadataResponseWriter::AddCaughtUpReplica(int32_t replica_node_id) {
  assert(this);
  assert(State == TState::InCaughtUpReplicaList);
  size_t offset = Size();
  GrowResult(THdr::CAUGHT_UP_REPLICA_NODE_ID_SIZE);
  WriteInt32ToHeader(Loc(offset), replica_node_id);
  ++CaughtUpReplicaCount;
}

void TMetadataResponseWriter::CloseCaughtUpReplicaList() {
  assert(this);
  assert(State == TState::InCaughtUpReplicaList);
  State = TState::InPartition;
  WriteInt32ToHeader(Loc(CaughtUpReplicaCountOffset), CaughtUpReplicaCount);
}

void TMetadataResponseWriter::ClosePartition() {
  assert(this);
  assert(State == TState::InPartition);
  State = TState::InPartitionList;
  ++PartitionCount;
  PartitionOffset = Size();
}

void TMetadataResponseWriter::ClosePartitionList() {
  assert(this);
  assert(State == TState::InPartitionList);
  State = TState::InTopic;
  WriteInt32ToHeader(Loc(PartitionCountOffset), PartitionCount);
}

void TMetadataResponseWriter::CloseTopic() {
  assert(this);
  assert(State == TState::InTopic);
  State = TState::InTopicList;
  ++TopicCount;
  TopicOffset = Size();
}

void TMetadataResponseWriter::CloseTopicList() {
  assert(this);
  assert(State == TState::InTopicList);
  State = TState::InResponse;
  WriteInt32ToHeader(Loc(TopicCountOffset), TopicCount);
}

void TMetadataResponseWriter::CloseResponse() {
  assert(this);
  assert(State == TState::InResponse);
  State = TState::Initial;
  assert(Size() >= REQUEST_OR_RESPONSE_SIZE_SIZE);
  int32_t size_field_value = Size() - REQUEST_OR_RESPONSE_SIZE_SIZE;
  WriteInt32ToHeader(Loc(THdr::RESPONSE_SIZE_OFFSET), size_field_value);
  Result = nullptr;
  ClearBookkeepingInfo();
}

void TMetadataResponseWriter::ClearBookkeepingInfo() {
  assert(this);
  BrokerCount = 0;
  BrokerOffset = 0;
  TopicCountOffset = 0;
  TopicCount = 0;
  TopicOffset = 0;
  PartitionCountOffset = 0;
  PartitionCount = 0;
  PartitionOffset = 0;
  ReplicaCountOffset = 0;
  ReplicaCount = 0;
  CaughtUpReplicaCountOffset = 0;
  CaughtUpReplicaCount = 0;
}

void TMetadataResponseWriter::SetEmptyStr(size_t size_field_offset) {
  assert(this);
  WriteInt16ToHeader(Loc(size_field_offset), THdr::EMPTY_STRING_LENGTH);
}

void TMetadataResponseWriter::SetStr(size_t size_field_offset,
    const char *str_begin, const char *str_end) {
  assert(this);
  assert(str_begin || (str_end == str_begin));
  assert(str_end >= str_begin);

  if (str_end == str_begin) {
    SetEmptyStr(size_field_offset);
    return;
  }

  size_t size = str_end - str_begin;
  assert(size <= static_cast<size_t>(std::numeric_limits<int16_t>::max()));
  WriteInt16ToHeader(Loc(size_field_offset), size);

  if (size) {
    std::memcpy(Loc(size_field_offset + THdr::STRING_SIZE_FIELD_SIZE),
                    str_begin, size);
  }
}
