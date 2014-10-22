/* <bruce/kafka_proto/v0/metadata_response_reader.cc>

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

   Implements <bruce/kafka_proto/v0/metadata_response_reader.h>.
 */

#include <bruce/kafka_proto/v0/metadata_response_reader.h>

#include <limits>

#include <base/field_access.h>
#include <base/no_default_case.h>
#include <server/counter.h>

using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

SERVER_COUNTER(MetadataResponseBadBrokerHostLen);
SERVER_COUNTER(MetadataResponseBadBrokerPort);
SERVER_COUNTER(MetadataResponseBadTopicNameLen);
SERVER_COUNTER(MetadataResponseIncomplete1);
SERVER_COUNTER(MetadataResponseIncomplete2);
SERVER_COUNTER(MetadataResponseIncomplete3);
SERVER_COUNTER(MetadataResponseIncomplete4);
SERVER_COUNTER(MetadataResponseIncomplete5);
SERVER_COUNTER(MetadataResponseIncomplete6);
SERVER_COUNTER(MetadataResponseIncomplete7);
SERVER_COUNTER(MetadataResponseIncomplete8);
SERVER_COUNTER(MetadataResponseIncomplete9);
SERVER_COUNTER(MetadataResponseInvalidLeaderNodeId);
SERVER_COUNTER(MetadataResponseNegativeBrokerCount);
SERVER_COUNTER(MetadataResponseNegativeBrokerNodeId);
SERVER_COUNTER(MetadataResponseNegativeCaughtUpReplicaNodeId);
SERVER_COUNTER(MetadataResponseNegativePartitionCaughtUpReplicaCount);
SERVER_COUNTER(MetadataResponseNegativePartitionCount);
SERVER_COUNTER(MetadataResponseNegativePartitionId);
SERVER_COUNTER(MetadataResponseNegativePartitionReplicaCount);
SERVER_COUNTER(MetadataResponseNegativeReplicaNodeId);
SERVER_COUNTER(MetadataResponseNegativeTopicCount);

TMetadataResponseReader::TMetadataResponseReader(const void *buf,
    size_t buf_size)
    : Buf(reinterpret_cast<const uint8_t *>(buf)),
      BufSize(buf_size),
      State(TState::Initial),
      BrokersLeft(0),
      CurrentBrokerOffset(0),
      CurrentBrokerHostLength(0),
      TopicsLeft(0),
      CurrentTopicOffset(0),
      CurrentTopicNameLength(0),
      PartitionsLeftInTopic(0),
      CurrentPartitionOffset(0),
      ReplicasLeftInPartition(0),
      CurrentReplicaOffset(0),
      CaughtUpReplicasLeftInPartition(0),
      CurrentCaughtUpReplicaOffset(0) {
  assert(Buf);
  assert((Buf + BufSize) > Buf);
  assert(BufSize >= MinSize());
}

int32_t TMetadataResponseReader::GetCorrelationId() const {
  assert(this);
  return ReadInt32FromHeader(Buf + THdr::CORRELATION_ID_OFFSET);
}

size_t TMetadataResponseReader::GetBrokerCount() const {
  assert(this);
  int32_t count = ReadInt32FromHeader(Buf + THdr::BROKER_COUNT_OFFSET);

  if (count < 0) {
    MetadataResponseNegativeBrokerCount.Increment();
    THROW_ERROR(TNegativeBrokerCount);
  }

  return count;
}

bool TMetadataResponseReader::FirstBroker() {
  assert(this);
  State = TState::InBrokerList;
  BrokersLeft = GetBrokerCount();
  CurrentBrokerOffset = THdr::BROKER_LIST_OFFSET;
  ClearStateVariables();
  return InitBroker();
}

bool TMetadataResponseReader::NextBroker() {
  assert(this);

  if (State == TState::Initial) {
    return FirstBroker();
  }

  assert(State == TState::InBrokerList);
  assert(BrokersLeft);

  --BrokersLeft;
  CurrentBrokerOffset += BrokerSize(CurrentBrokerHostLength);
  return InitBroker();
}

void TMetadataResponseReader::SkipRemainingBrokers() {
  assert(this);
  assert(State <= TState::InBrokerList);

  if ((State == TState::Initial) && !FirstBroker()) {
    return;
  }

  assert(State == TState::InBrokerList);

  while (BrokersLeft) {
    NextBroker();
  }
}

int32_t TMetadataResponseReader::GetCurrentBrokerNodeId() const {
  assert(this);
  assert(State == TState::InBrokerList);
  assert(BrokersLeft);
  int32_t result = ReadInt32FromHeader(Buf + CurrentBrokerOffset +
                                       THdr::REL_BROKER_NODE_ID_OFFSET);

  /* It seems like a reasonable assumption that a broker node ID will never be
     negative. */
  if (result < 0) {
    MetadataResponseNegativeBrokerNodeId.Increment();
    THROW_ERROR(TNegativeBrokerNodeId);
  }

  return result;
}

const char *TMetadataResponseReader::GetCurrentBrokerHostBegin() const {
  assert(this);
  assert(State == TState::InBrokerList);
  assert(BrokersLeft);
  return reinterpret_cast<const char *>(Buf + CurrentBrokerOffset +
      THdr::REL_BROKER_HOST_OFFSET);
}

const char *TMetadataResponseReader::GetCurrentBrokerHostEnd() const {
  assert(this);
  assert(State == TState::InBrokerList);
  assert(BrokersLeft);
  return GetCurrentBrokerHostBegin() + CurrentBrokerHostLength;
}

int32_t TMetadataResponseReader::GetCurrentBrokerPort() const {
  assert(this);
  assert(State == TState::InBrokerList);
  assert(BrokersLeft);
  int32_t port = ReadInt32FromHeader(Buf + CurrentBrokerOffset +
      THdr::RelBrokerPortOffset(CurrentBrokerHostLength));

  /* TCP port numbers are 16-bit unsigned values. */
  if ((port < 0) || (port > std::numeric_limits<uint16_t>::max())) {
    MetadataResponseBadBrokerPort.Increment();
    THROW_ERROR(TBadBrokerPort);
  }

  return port;
}

size_t TMetadataResponseReader::GetTopicCount() const {
  assert(this);
  assert(State >= TState::InBrokerList);
  assert(BrokersLeft == 0);
  assert(BufSize >= CurrentBrokerOffset);

  if ((BufSize - CurrentBrokerOffset) < THdr::TOPIC_COUNT_SIZE) {
    MetadataResponseIncomplete1.Increment();
    THROW_ERROR(TIncompleteMetadataResponse);
  }

  int32_t count = ReadInt32FromHeader(Buf + CurrentBrokerOffset +
                                      THdr::REL_TOPIC_COUNT_OFFSET);

  if (count < 0) {
    MetadataResponseNegativeTopicCount.Increment();
    THROW_ERROR(TNegativeTopicCount);
  }

  return count;
}

bool TMetadataResponseReader::FirstTopic() {
  assert(this);

  if (State <= TState::InBrokerList) {
    SkipRemainingBrokers();
  }

  assert(State >= TState::InBrokerList);
  assert(BrokersLeft == 0);
  TopicsLeft = GetTopicCount();
  State = TState::InTopicList;
  CurrentTopicOffset = CurrentBrokerOffset + THdr::TOPIC_COUNT_SIZE;
  ClearStateVariables();
  return InitTopic();
}

bool TMetadataResponseReader::NextTopic() {
  assert(this);

  if (State <= TState::InBrokerList) {
    return FirstTopic();
  }

  assert(State >= TState::InTopicList);
  assert(TopicsLeft);
  SkipRemainingPartitions();
  assert(PartitionsLeftInTopic == 0);
  State = TState::InTopicList;
  --TopicsLeft;
  CurrentTopicOffset = CurrentPartitionOffset;
  return InitTopic();
}

void TMetadataResponseReader::SkipRemainingTopics() {
  assert(this);
  assert(State >= TState::InBrokerList);
  assert(BrokersLeft == 0);

  if ((State == TState::InBrokerList) && !FirstTopic()) {
    return;
  }

  assert(State >= TState::InTopicList);

  while (TopicsLeft) {
    NextTopic();
  }
}

int16_t TMetadataResponseReader::GetCurrentTopicErrorCode() const {
  assert(this);
  assert(State >= TState::InTopicList);
  assert(TopicsLeft);
  return ReadInt16FromHeader(Buf + CurrentTopicOffset +
                             THdr::REL_TOPIC_ERROR_CODE_OFFSET);
}

const char *TMetadataResponseReader::GetCurrentTopicNameBegin() const {
  assert(this);
  assert(State >= TState::InTopicList);
  assert(TopicsLeft);
  return reinterpret_cast<const char *>(Buf + CurrentTopicOffset +
      THdr::REL_TOPIC_NAME_OFFSET);
}

const char *TMetadataResponseReader::GetCurrentTopicNameEnd() const {
  assert(this);
  assert(State >= TState::InTopicList);
  assert(TopicsLeft);
  return GetCurrentTopicNameBegin() + CurrentTopicNameLength;
}

size_t TMetadataResponseReader::GetCurrentTopicPartitionCount() const {
  assert(this);
  assert(State >= TState::InTopicList);
  assert(TopicsLeft);
  int32_t count = ReadInt32FromHeader(Buf + CurrentTopicOffset +
      THdr::RelPartitionCountOffset(CurrentTopicNameLength));

  if (count < 0) {
    MetadataResponseNegativePartitionCount.Increment();
    THROW_ERROR(TNegativePartitionCount);
  }

  return count;
}

bool TMetadataResponseReader::FirstPartitionInTopic() {
  assert(this);
  assert(State >= TState::InTopicList);
  assert(TopicsLeft);
  PartitionsLeftInTopic = GetCurrentTopicPartitionCount();
  State = TState::InPartitionList;
  CurrentPartitionOffset = CurrentTopicOffset +
      THdr::RelPartitionCountOffset(CurrentTopicNameLength) +
      THdr::PARTITION_COUNT_SIZE;
  ClearStateVariables();
  return InitPartition();
}

bool TMetadataResponseReader::NextPartitionInTopic() {
  assert(this);
  assert(State >= TState::InTopicList);
  assert(TopicsLeft);

  if (State == TState::InTopicList) {
    return FirstPartitionInTopic();
  }

  assert(State >= TState::InPartitionList);
  assert(PartitionsLeftInTopic);

  switch (State) {
    case TState::InPartitionList: {
      /* FALLTHROUGH */
    }
    case TState::InReplicaList: {
      SkipRemainingReplicas();
      /* FALLTHROUGH */
    }
    case TState::InCaughtUpReplicaList: {
      SkipRemainingCaughtUpReplicas();
      break;
    }
    NO_DEFAULT_CASE;
  }

  assert(State == TState::InCaughtUpReplicaList);
  assert(CaughtUpReplicasLeftInPartition == 0);
  State = TState::InPartitionList;
  --PartitionsLeftInTopic;
  CurrentPartitionOffset = CurrentCaughtUpReplicaOffset;
  return InitPartition();
}

void TMetadataResponseReader::SkipRemainingPartitions() {
  assert(this);
  assert(State >= TState::InTopicList);
  assert(TopicsLeft);

  if ((State == TState::InTopicList) && !FirstPartitionInTopic()) {
    return;
  }

  assert(State >= TState::InPartitionList);

  while (PartitionsLeftInTopic) {
    NextPartitionInTopic();
  }
}

int16_t TMetadataResponseReader::GetCurrentPartitionErrorCode() const {
  assert(this);
  assert(State >= TState::InPartitionList);
  assert(PartitionsLeftInTopic);
  return ReadInt16FromHeader(Buf + CurrentPartitionOffset +
                             THdr::REL_PARTITION_ERROR_CODE_OFFSET);
}

int32_t TMetadataResponseReader::GetCurrentPartitionId() const {
  assert(this);
  assert(State >= TState::InPartitionList);
  assert(PartitionsLeftInTopic);
  int32_t id = ReadInt32FromHeader(Buf + CurrentPartitionOffset +
                                   THdr::REL_PARTITION_ID_OFFSET);

  /* It seems like a reasonable assumption that a partition ID will never be
     negative. */
  if (id < 0) {
    MetadataResponseNegativePartitionId.Increment();
    THROW_ERROR(TNegativePartitionId);
  }

  return id;
}

int32_t TMetadataResponseReader::GetCurrentPartitionLeaderId() const {
  assert(this);
  assert(State >= TState::InPartitionList);
  assert(PartitionsLeftInTopic);
  int32_t id = ReadInt32FromHeader(Buf + CurrentPartitionOffset +
                                   THdr::REL_LEADER_NODE_ID_OFFSET);

  /* A value of -1 indicates that no leader exists because a leadership
     election is in progress.  I assume that no other negative values are
     valid. */
  if (id < -1) {
    MetadataResponseInvalidLeaderNodeId.Increment();
    THROW_ERROR(TInvalidLeaderNodeId);
  }

  return id;
}

size_t TMetadataResponseReader::GetCurrentPartitionReplicaCount() const {
  assert(this);
  assert(State >= TState::InPartitionList);
  assert(PartitionsLeftInTopic);
  int32_t count = ReadInt32FromHeader(Buf + CurrentPartitionOffset +
      THdr::REL_REPLICA_COUNT_OFFSET);

  if (count < 0) {
    MetadataResponseNegativePartitionReplicaCount.Increment();
    THROW_ERROR(TNegativePartitionReplicaCount);
  }

  return count;
}

bool TMetadataResponseReader::FirstReplicaInPartition() {
  assert(this);
  assert(State >= TState::InPartitionList);
  assert(PartitionsLeftInTopic);
  ReplicasLeftInPartition = GetCurrentPartitionReplicaCount();
  State = TState::InReplicaList;
  CurrentReplicaOffset = CurrentPartitionOffset +
      THdr::REL_REPLICA_COUNT_OFFSET + THdr::REPLICA_COUNT_SIZE;
  ClearStateVariables();
  return InitReplica();
}

bool TMetadataResponseReader::NextReplicaInPartition() {
  assert(this);
  assert((State == TState::InPartitionList) ||
         (State == TState::InReplicaList));
  assert(PartitionsLeftInTopic);

  if (State == TState::InPartitionList) {
    return FirstReplicaInPartition();
  }

  assert(State == TState::InReplicaList);
  assert(ReplicasLeftInPartition);
  --ReplicasLeftInPartition;
  CurrentReplicaOffset += THdr::REPLICA_NODE_ID_SIZE;
  return InitReplica();
}

void TMetadataResponseReader::SkipRemainingReplicas() {
  assert(this);
  assert((State == TState::InPartitionList) ||
         (State == TState::InReplicaList));
  assert(PartitionsLeftInTopic);

  if ((State == TState::InPartitionList) && !FirstReplicaInPartition()) {
    return;
  }

  assert(State == TState::InReplicaList);

  while (ReplicasLeftInPartition) {
    NextReplicaInPartition();
  }
}

int32_t TMetadataResponseReader::GetCurrentReplicaNodeId() const {
  assert(this);
  assert(State == TState::InReplicaList);
  assert(ReplicasLeftInPartition);
  int32_t id = ReadInt32FromHeader(Buf + CurrentReplicaOffset);

  /* It seems like a reasonable assumption that a node ID will never be
     negative. */
  if (id < 0) {
    MetadataResponseNegativeReplicaNodeId.Increment();
    THROW_ERROR(TNegativeReplicaNodeId);
  }

  return id;
}

size_t
TMetadataResponseReader::GetCurrentPartitionCaughtUpReplicaCount() const {
  assert(this);
  assert(State >= TState::InReplicaList);
  assert(ReplicasLeftInPartition == 0);
  assert(BufSize >= CurrentReplicaOffset);

  if ((BufSize - CurrentReplicaOffset) < THdr::CAUGHT_UP_REPLICA_COUNT_SIZE) {
    MetadataResponseIncomplete2.Increment();
    THROW_ERROR(TIncompleteMetadataResponse);
  }

  int32_t count = ReadInt32FromHeader(Buf + CurrentReplicaOffset);

  if (count < 0) {
    MetadataResponseNegativePartitionCaughtUpReplicaCount.Increment();
    THROW_ERROR(TNegativePartitionCaughtUpReplicaCount);
  }

  return count;
}

bool TMetadataResponseReader::FirstCaughtUpReplicaInPartition() {
  assert(this);
  assert(State >= TState::InPartitionList);

  if (State <= TState::InReplicaList) {
    SkipRemainingReplicas();
  }

  assert(State >= TState::InReplicaList);
  assert(ReplicasLeftInPartition == 0);
  CaughtUpReplicasLeftInPartition = GetCurrentPartitionCaughtUpReplicaCount();
  State = TState::InCaughtUpReplicaList;
  CurrentCaughtUpReplicaOffset = CurrentReplicaOffset +
                                 THdr::CAUGHT_UP_REPLICA_COUNT_SIZE;
  ClearStateVariables();
  return InitCaughtUpReplica();
}

bool TMetadataResponseReader::NextCaughtUpReplicaInPartition() {
  assert(this);
  assert(State >= TState::InPartitionList);

  if (State < TState::InCaughtUpReplicaList) {
    return FirstCaughtUpReplicaInPartition();
  }

  assert(State == TState::InCaughtUpReplicaList);
  assert(CaughtUpReplicasLeftInPartition);
  --CaughtUpReplicasLeftInPartition;
  CurrentCaughtUpReplicaOffset += THdr::CAUGHT_UP_REPLICA_NODE_ID_SIZE;
  return InitCaughtUpReplica();
}

void TMetadataResponseReader::SkipRemainingCaughtUpReplicas() {
  assert(this);
  assert((State == TState::InReplicaList) ||
         (State == TState::InCaughtUpReplicaList));
  assert(ReplicasLeftInPartition == 0);

  if ((State == TState::InReplicaList) && !FirstCaughtUpReplicaInPartition()) {
    return;
  }

  assert(State == TState::InCaughtUpReplicaList);

  while (CaughtUpReplicasLeftInPartition) {
    NextCaughtUpReplicaInPartition();
  }
}

int32_t TMetadataResponseReader::GetCurrentCaughtUpReplicaNodeId() const {
  assert(this);
  assert(State == TState::InCaughtUpReplicaList);
  assert(CaughtUpReplicasLeftInPartition);
  int32_t id = ReadInt32FromHeader(Buf + CurrentCaughtUpReplicaOffset);

  /* It seems like a reasonable assumption that a node ID will never be
     negative. */
  if (id < 0) {
    MetadataResponseNegativeCaughtUpReplicaNodeId.Increment();
    THROW_ERROR(TNegativeCaughtUpReplicaNodeId);
  }

  return id;
}

void TMetadataResponseReader::ClearStateVariables() {
  assert(this);

  switch (State) {
    case TState::Initial: {
      BrokersLeft = 0;
      CurrentBrokerOffset = 0;
      CurrentBrokerHostLength = 0;
      /* FALLTHROUGH */
    }
    case TState::InBrokerList: {
      TopicsLeft = 0;
      CurrentTopicOffset = 0;
      CurrentTopicNameLength = 0;
      /* FALLTHROUGH */
    }
    case TState::InTopicList: {
      PartitionsLeftInTopic = 0;
      CurrentPartitionOffset = 0;
      /* FALLTHROUGH */
    }
    case TState::InPartitionList: {
      ReplicasLeftInPartition = 0;
      CurrentReplicaOffset = 0;
      /* FALLTHROUGH */
    }
    case TState::InReplicaList: {
      CaughtUpReplicasLeftInPartition = 0;
      CurrentCaughtUpReplicaOffset = 0;
      /* FALLTHROUGH */
    }
    case TState::InCaughtUpReplicaList: {
      break;
    }
    NO_DEFAULT_CASE;
  }
}

bool TMetadataResponseReader::InitBroker() {
  assert(this);
  CurrentBrokerHostLength = 0;

  if (BrokersLeft == 0) {
    return false;
  }

  assert(BufSize >= CurrentBrokerOffset);
  size_t broker_space = BufSize - CurrentBrokerOffset;

  if (broker_space < (THdr::REL_BROKER_HOST_LENGTH_OFFSET +
                      THdr::BROKER_HOST_LENGTH_SIZE)) {
    MetadataResponseIncomplete3.Increment();
    THROW_ERROR(TIncompleteMetadataResponse);
  }

  CurrentBrokerHostLength = ReadInt16FromHeader(
      Buf + CurrentBrokerOffset + THdr::REL_BROKER_HOST_LENGTH_OFFSET);

  if (CurrentBrokerHostLength < 1) {
    MetadataResponseBadBrokerHostLen.Increment();
    THROW_ERROR(TBadBrokerHostLen);
  }

  if (broker_space < BrokerSize(CurrentBrokerHostLength)) {
    MetadataResponseIncomplete4.Increment();
    THROW_ERROR(TIncompleteMetadataResponse);
  }

  return true;
}

bool TMetadataResponseReader::InitTopic() {
  assert(this);
  CurrentTopicNameLength = 0;

  if (TopicsLeft == 0) {
    return false;
  }

  assert(BufSize >= CurrentTopicOffset);
  size_t topic_space = BufSize - CurrentTopicOffset;

  if (topic_space <
      (THdr::REL_TOPIC_NAME_LENGTH_OFFSET + THdr::TOPIC_NAME_LENGTH_SIZE)) {
    MetadataResponseIncomplete5.Increment();
    THROW_ERROR(TIncompleteMetadataResponse);
  }

  CurrentTopicNameLength = ReadInt16FromHeader(Buf + CurrentTopicOffset +
      THdr::REL_TOPIC_NAME_LENGTH_OFFSET);

  if (CurrentTopicNameLength < 1) {
    MetadataResponseBadTopicNameLen.Increment();
    THROW_ERROR(TBadTopicNameLen);
  }

  if (topic_space < (THdr::RelPartitionCountOffset(CurrentTopicNameLength) +
                     THdr::PARTITION_COUNT_SIZE)) {
    MetadataResponseIncomplete6.Increment();
    THROW_ERROR(TIncompleteMetadataResponse);
  }

  return true;
}

bool TMetadataResponseReader::InitPartition() {
  assert(this);

  if (PartitionsLeftInTopic == 0) {
    return false;
  }

  assert(BufSize >= CurrentPartitionOffset);
  size_t partition_space = BufSize - CurrentPartitionOffset;

  if (partition_space < (THdr::REL_REPLICA_COUNT_OFFSET +
                         THdr::REPLICA_COUNT_SIZE)) {
    MetadataResponseIncomplete7.Increment();
    THROW_ERROR(TIncompleteMetadataResponse);
  }

  return true;
}

bool TMetadataResponseReader::InitReplica() {
  assert(this);

  if (ReplicasLeftInPartition == 0) {
    return false;
  }

  assert(BufSize >= CurrentReplicaOffset);
  size_t replica_space = BufSize - CurrentReplicaOffset;

  if (replica_space < THdr::REPLICA_NODE_ID_SIZE) {
    MetadataResponseIncomplete8.Increment();
    THROW_ERROR(TIncompleteMetadataResponse);
  }

  return true;
}

bool TMetadataResponseReader::InitCaughtUpReplica() {
  assert(this);

  if (CaughtUpReplicasLeftInPartition == 0) {
    return false;
  }

  assert(BufSize >= CurrentCaughtUpReplicaOffset);
  size_t replica_space = BufSize - CurrentCaughtUpReplicaOffset;

  if (replica_space < THdr::CAUGHT_UP_REPLICA_NODE_ID_SIZE) {
    MetadataResponseIncomplete9.Increment();
    THROW_ERROR(TIncompleteMetadataResponse);
  }

  return true;
}
