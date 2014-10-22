/* <bruce/kafka_proto/v0/metadata_request_fields.h>

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

   Constants and functions related to sizes and offsets of fields in metadata
   responses.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include <bruce/kafka_proto/v0/protocol_util.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TMetadataResponseFields final {
        public:
        static const size_t STRING_SIZE_FIELD_SIZE = 2;

        static const size_t CORRELATION_ID_SIZE = 4;

        static const size_t BROKER_COUNT_SIZE = 4;

        static const size_t BROKER_NODE_ID_SIZE = 4;

        static const size_t BROKER_HOST_LENGTH_SIZE = STRING_SIZE_FIELD_SIZE;

        static const size_t BROKER_PORT_SIZE = 4;

        static size_t BrokerListItemSize(size_t broker_host_length) {
            return BROKER_NODE_ID_SIZE + BROKER_HOST_LENGTH_SIZE +
                   broker_host_length + BROKER_PORT_SIZE;
        }

        static const size_t TOPIC_COUNT_SIZE = 4;

        static const size_t TOPIC_ERROR_CODE_SIZE = 2;

        static const size_t TOPIC_NAME_LENGTH_SIZE = STRING_SIZE_FIELD_SIZE;

        static const size_t PARTITION_COUNT_SIZE = 4;

        static const size_t PARTITION_ERROR_CODE_SIZE = 2;

        static const size_t PARTITION_ID_SIZE = 4;

        static const size_t LEADER_NODE_ID_SIZE = 4;

        static const size_t REPLICA_COUNT_SIZE = 4;

        static const size_t REPLICA_NODE_ID_SIZE = 4;

        static const size_t CAUGHT_UP_REPLICA_COUNT_SIZE = 4;

        static const size_t CAUGHT_UP_REPLICA_NODE_ID_SIZE = 4;

        static const size_t RESPONSE_SIZE_OFFSET = 0;

        static const size_t CORRELATION_ID_OFFSET = RESPONSE_SIZE_OFFSET +
            REQUEST_OR_RESPONSE_SIZE_SIZE;

        static const size_t BROKER_COUNT_OFFSET = CORRELATION_ID_OFFSET +
            CORRELATION_ID_SIZE;

        static const size_t BROKER_LIST_OFFSET = BROKER_COUNT_OFFSET +
            BROKER_COUNT_SIZE;

        /* relative to start of broker list item */
        static const size_t REL_BROKER_NODE_ID_OFFSET = 0;

        /* relative to start of broker list item */
        static const size_t REL_BROKER_HOST_LENGTH_OFFSET =
            REL_BROKER_NODE_ID_OFFSET + BROKER_NODE_ID_SIZE;

        /* relative to start of broker list item */
        static const size_t REL_BROKER_HOST_OFFSET =
            REL_BROKER_HOST_LENGTH_OFFSET + BROKER_HOST_LENGTH_SIZE;

        /* Returns offset of broker port field relative to start of broker list
           item.  'broker_host_length' specifies broker host name length. */
        static size_t RelBrokerPortOffset(size_t broker_host_length) {
            return REL_BROKER_HOST_OFFSET + broker_host_length;
        }

        /* relative to start of topic metadata */
        static const size_t REL_TOPIC_COUNT_OFFSET = 0;

        /* relative to start of topic metadata */
        static const size_t REL_TOPIC_LIST_OFFSET = REL_TOPIC_COUNT_OFFSET +
            TOPIC_COUNT_SIZE;

        /* relative to start of topic list item */
        static const size_t REL_TOPIC_ERROR_CODE_OFFSET = 0;

        /* relative to start of topic list item */
        static const size_t REL_TOPIC_NAME_LENGTH_OFFSET =
            REL_TOPIC_ERROR_CODE_OFFSET + TOPIC_ERROR_CODE_SIZE;

        /* relative to start of topic list item */
        static const size_t REL_TOPIC_NAME_OFFSET =
            REL_TOPIC_NAME_LENGTH_OFFSET + TOPIC_NAME_LENGTH_SIZE;

        /* relative to start of topic list item */
        static size_t RelPartitionCountOffset(size_t topic_name_length) {
          return REL_TOPIC_NAME_OFFSET + topic_name_length;
        }

        /* relative to start of topic list item */
        static size_t RelPartitionListOffset(size_t topic_name_length) {
          return RelPartitionCountOffset(topic_name_length) +
                 PARTITION_COUNT_SIZE;
        }

        /* relative to start of partition list item */
        static const size_t REL_PARTITION_ERROR_CODE_OFFSET = 0;

        /* relative to start of partition list item */
        static const size_t REL_PARTITION_ID_OFFSET =
            REL_PARTITION_ERROR_CODE_OFFSET + PARTITION_ERROR_CODE_SIZE;

        /* relative to start of partition list item */
        static const size_t REL_LEADER_NODE_ID_OFFSET =
            REL_PARTITION_ID_OFFSET + PARTITION_ID_SIZE;

        /* relative to start of partition list item */
        static const size_t REL_REPLICA_COUNT_OFFSET =
            REL_LEADER_NODE_ID_OFFSET + LEADER_NODE_ID_SIZE;

        /* relative to start of partition list item */
        static const size_t REL_REPLICA_NODE_LIST_OFFSET =
            REL_REPLICA_COUNT_OFFSET + REPLICA_COUNT_SIZE;

        /* relative to start of partition list item */
        static size_t RelCaughtUpReplicaCountOffset(size_t replica_count) {
          return REL_REPLICA_NODE_LIST_OFFSET +
                 (replica_count * REPLICA_NODE_ID_SIZE);
        }

        /* relative to start of partition list item */
        static size_t RelCaughtUpReplicaListOffset(size_t replica_count) {
          return RelCaughtUpReplicaCountOffset(replica_count) +
                 CAUGHT_UP_REPLICA_COUNT_SIZE;
        }

        static const int16_t EMPTY_STRING_LENGTH = -1;
      };  // TMetadataResponseFields

    }  // V0

  }  // KafkaProto

}  // Bruce
