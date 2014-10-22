/* <bruce/kafka_proto/v0/produce_response_writer.h>

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

   Class for creating a Kafka produce response.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include <base/no_copy_semantics.h>
#include <bruce/kafka_proto/produce_response_writer_api.h>
#include <bruce/kafka_proto/v0/protocol_util.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TProduceResponseWriter final : public TProduceResponseWriterApi {
        NO_COPY_SEMANTICS(TProduceResponseWriter);

        public:
        TProduceResponseWriter();

        virtual void Reset() override;

        virtual void OpenResponse(std::vector<uint8_t> &out,
            int32_t correlation_id) override;

        virtual void OpenTopic(const char *topic_begin,
            const char *topic_end) override;

        virtual void AddPartition(int32_t partition, int16_t error_code,
            int64_t offset) override;

        virtual void CloseTopic() override;

        virtual void CloseResponse() override;

        private:
        static const size_t CORRELATION_ID_SIZE = 4;

        static const size_t TOPIC_COUNT_SIZE = 4;

        static const size_t TOPIC_NAME_LENGTH_SIZE = 2;

        static const size_t PARTITION_COUNT_SIZE = 4;

        static const size_t PARTITION_SIZE = 4;

        static const size_t ERROR_CODE_SIZE = 2;

        static const size_t OFFSET_SIZE = 8;

        static const size_t BYTES_PER_PARTITION = PARTITION_SIZE +
            ERROR_CODE_SIZE + OFFSET_SIZE;

        std::vector<uint8_t> *OutBuf;

        bool TopicStarted;

        size_t CurrentTopicOffset;

        size_t CurrentTopicIndex;

        size_t PartitionCountOffset;

        size_t CurrentPartitionOffset;

        size_t CurrentPartitionIndex;
      };  // TProduceResponseWriter

    }  // V0

  }  // KafkaProto

}  // Bruce
