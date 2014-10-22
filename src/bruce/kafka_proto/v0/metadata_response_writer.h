/* <bruce/kafka_proto/v0/metadata_response_writer.h>

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

   Helper class for writing a metadata response (for testing).
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <base/no_copy_semantics.h>
#include <bruce/kafka_proto/v0/metadata_response_fields.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TMetadataResponseWriter final {
        NO_COPY_SEMANTICS(TMetadataResponseWriter);

        using THdr = TMetadataResponseFields;

        public:
        TMetadataResponseWriter();

        /* 'out' will contain the result, and will be resized to contain it. */
        void OpenResponse(std::vector<uint8_t> &out, int32_t correlation_id);

        /* Start writing the broker list.  Must be called even if the broker
           list is empty. */
        void OpenBrokerList();

        /* Add an item to the broker list. */
        void AddBroker(int32_t node_id, const char *host_begin,
                       const char *host_end, int32_t port);

        /* Finish writing the broker list. */
        void CloseBrokerList();

        /* Start writing the topic list.  Must be called even if the topic list
           is empty. */
        void OpenTopicList();

        /* Start writing the next topic within the topic list. */
        void OpenTopic(int16_t topic_error_code, const char *topic_name_begin,
            const char *topic_name_end);

        /* Start writing the partition list within the current topic.  Must be
           called even if the partition list is empty. */
        void OpenPartitionList();

        /* Start writing the next partition within a partition list. */
        void OpenPartition(int16_t partition_error_code, int32_t partition_id,
            int32_t leader_node_id);

        /* Start writing the replica list within a partition.  Must be called
           even if the replica list is empty. */
        void OpenReplicaList();

        /* Add a replica to a replica list. */
        void AddReplica(int32_t replica_node_id);

        /* Finish writing a replica list. */
        void CloseReplicaList();

        /* Start writing the caught up replica list within a partition.  Must
           be called even if the caught up replica list is empty. */
        void OpenCaughtUpReplicaList();

        /* Add a replica to a caught up replica list. */
        void AddCaughtUpReplica(int32_t replica_node_id);

        /* Finish writing a caught up replica list. */
        void CloseCaughtUpReplicaList();

        /* Finish writing a partition. */
        void ClosePartition();

        /* Finish writing a partition list. */
        void ClosePartitionList();

        /* Finish writing a topic. */
        void CloseTopic();

        /* Finish writing the topic list. */
        void CloseTopicList();

        /* Finish writing the request. */
        void CloseResponse();

        private:
        enum class TState {
          Initial,
          InResponse,
          InBrokerList,
          InTopicList,
          InTopic,
          InPartitionList,
          InPartition,
          InReplicaList,
          InCaughtUpReplicaList
        };

        void GrowResult(size_t num_bytes) {
          assert(this);
          assert(Result);
          Result->resize(Result->size() + num_bytes);
        }

        /* This is just to save me typing effort.  It's easier to type Loc(i)
           than &(*Result)[i]. */
        uint8_t *Loc(size_t i) {
          assert(this);
          assert(Result);
          return &(*Result)[i];
        }

        /* More stuff to save typing. */
        size_t Size() const {
          assert(this);
          assert(Result);
          return Result->size();
        }

        void ClearBookkeepingInfo();

        void SetEmptyStr(size_t size_field_offset);

        void SetStr(size_t size_field_offset, const char *str_begin,
                    const char *str_end);

        TState State;

        std::vector<uint8_t> *Result;

        int32_t BrokerCount;

        int32_t BrokerOffset;

        int32_t TopicCountOffset;

        int32_t TopicCount;

        int32_t TopicOffset;

        int32_t PartitionCountOffset;

        int32_t PartitionCount;

        int32_t PartitionOffset;

        int32_t ReplicaCountOffset;

        int32_t ReplicaCount;

        int32_t CaughtUpReplicaCountOffset;

        int32_t CaughtUpReplicaCount;
      };  // TMetadataResponseWriter

    }  // V0

  }  // KafkaProto

}  // Bruce
