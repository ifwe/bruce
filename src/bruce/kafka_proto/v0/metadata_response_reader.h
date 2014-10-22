/* <bruce/kafka_proto/v0/metadata_response_reader.h>

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

   Helper class for reading a metadata response.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

#include <base/thrower.h>
#include <bruce/kafka_proto/v0/metadata_response_fields.h>
#include <bruce/kafka_proto/v0/protocol_util.h>
#include <bruce/kafka_proto/wire_protocol.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TMetadataResponseReader final {
        using THdr = TMetadataResponseFields;

        public:
        using TBadMetadataResponse = TWireProtocol::TBadMetadataResponse;

        DEFINE_ERROR(TNegativeBrokerCount, TBadMetadataResponse,
                     "Kafka metadata response has negative broker count");

        DEFINE_ERROR(TIncompleteMetadataResponse, TBadMetadataResponse,
                     "Kafka metadata response is incomplete");

        DEFINE_ERROR(TBadBrokerHostLen, TBadMetadataResponse,
                     "Invalid broker host length in Kafka metadata response");

        DEFINE_ERROR(TNegativeBrokerNodeId, TBadMetadataResponse,
                     "Kafka metadata response has negative broker node ID");

        DEFINE_ERROR(TBadBrokerPort, TBadMetadataResponse,
                     "Invalid broker port in Kafka metadata response");

        DEFINE_ERROR(TNegativeTopicCount, TBadMetadataResponse,
                     "Kafka metadata response has negative topic count");

        DEFINE_ERROR(TBadTopicNameLen, TBadMetadataResponse,
                     "Invalid topic name length in Kafka metadata response");

        DEFINE_ERROR(TNegativePartitionCount, TBadMetadataResponse,
                     "Kafka metadata response has negative partition count");

        DEFINE_ERROR(TNegativePartitionId, TBadMetadataResponse,
                     "Kafka metadata response has negative partition ID");

        DEFINE_ERROR(TInvalidLeaderNodeId, TBadMetadataResponse,
                     "Kafka metadata response has invalid leader node ID");

        DEFINE_ERROR(TNegativePartitionReplicaCount, TBadMetadataResponse,
                     "Kafka metadata response has negative partition replica "
                     "count");

        DEFINE_ERROR(TNegativeReplicaNodeId, TBadMetadataResponse,
                     "Kafka metadata response has negative replica node ID");

        DEFINE_ERROR(TNegativePartitionCaughtUpReplicaCount,
                     TBadMetadataResponse,
                     "Kafka metadata response has negative partition caught "
                     "up replica count");

        DEFINE_ERROR(TNegativeCaughtUpReplicaNodeId, TBadMetadataResponse,
                     "Kafka metadata response has negative caught up replica "
                     "node ID");

        static size_t MinSize() {
          return REQUEST_OR_RESPONSE_SIZE_SIZE + THdr::CORRELATION_ID_SIZE +
              THdr::BROKER_COUNT_SIZE + THdr::TOPIC_COUNT_SIZE;
        }

        TMetadataResponseReader(const void *buf, size_t buf_size);

        int32_t GetCorrelationId() const;

        size_t GetBrokerCount() const;

        bool FirstBroker();

        bool NextBroker();

        void SkipRemainingBrokers();

        int32_t GetCurrentBrokerNodeId() const;

        const char *GetCurrentBrokerHostBegin() const;

        const char *GetCurrentBrokerHostEnd() const;

        int32_t GetCurrentBrokerPort() const;

        size_t GetTopicCount() const;

        bool FirstTopic();

        bool NextTopic();

        void SkipRemainingTopics();

        int16_t GetCurrentTopicErrorCode() const;

        const char *GetCurrentTopicNameBegin() const;

        const char *GetCurrentTopicNameEnd() const;

        size_t GetCurrentTopicPartitionCount() const;

        bool FirstPartitionInTopic();

        bool NextPartitionInTopic();

        void SkipRemainingPartitions();

        int16_t GetCurrentPartitionErrorCode() const;

        int32_t GetCurrentPartitionId() const;

        int32_t GetCurrentPartitionLeaderId() const;

        size_t GetCurrentPartitionReplicaCount() const;

        bool FirstReplicaInPartition();

        bool NextReplicaInPartition();

        void SkipRemainingReplicas();

        int32_t GetCurrentReplicaNodeId() const;

        size_t GetCurrentPartitionCaughtUpReplicaCount() const;

        bool FirstCaughtUpReplicaInPartition();

        bool NextCaughtUpReplicaInPartition();

        void SkipRemainingCaughtUpReplicas();

        int32_t GetCurrentCaughtUpReplicaNodeId() const;

        private:
        enum class TState {
          Initial = 0,
          InBrokerList = 1,
          InTopicList = 2,
          InPartitionList = 3,
          InReplicaList = 4,
          InCaughtUpReplicaList = 5
        };

        static size_t BrokerSize(size_t host_length) {
          return THdr::RelBrokerPortOffset(host_length) +
                 THdr::BROKER_PORT_SIZE;
        }

        void ClearStateVariables();

        bool InitBroker();

        bool InitTopic();

        bool InitPartition();

        bool InitReplica();

        bool InitCaughtUpReplica();

        const uint8_t * const Buf;

        const size_t BufSize;

        TState State;

        size_t BrokersLeft;

        size_t CurrentBrokerOffset;

        size_t CurrentBrokerHostLength;

        size_t TopicsLeft;

        size_t CurrentTopicOffset;

        size_t CurrentTopicNameLength;

        size_t PartitionsLeftInTopic;

        size_t CurrentPartitionOffset;

        size_t ReplicasLeftInPartition;

        size_t CurrentReplicaOffset;

        size_t CaughtUpReplicasLeftInPartition;

        size_t CurrentCaughtUpReplicaOffset;
      };  // TMetadataResponseReader

    }  // V0

  }  // KafkaProto

}  // Bruce
