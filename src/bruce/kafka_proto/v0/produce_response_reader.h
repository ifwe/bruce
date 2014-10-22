/* <bruce/kafka_proto/v0/produce_response_reader.h>

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

   Class for reading the contents of a produce response from a Kafka broker.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include <base/thrower.h>
#include <bruce/kafka_proto/produce_response_reader_api.h>
#include <bruce/kafka_proto/v0/produce_response_constants.h>
#include <bruce/kafka_proto/v0/protocol_util.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TProduceResponseReader final : public TProduceResponseReaderApi {
        public:
        DEFINE_ERROR(TShortResponse, TBadProduceResponse,
                     "Kafka produce response is too short");

        DEFINE_ERROR(TResponseTruncated, TBadProduceResponse,
                     "Kafka produce response is truncated");

        DEFINE_ERROR(TBadTopicCount, TBadProduceResponse,
                     "Invalid topic count in Kafka produce response");

        DEFINE_ERROR(TBadTopicNameLength, TBadProduceResponse,
                     "Bad topic name length in Kafka produce response");

        DEFINE_ERROR(TBadPartitionCount, TBadProduceResponse,
                     "Invalid partition count in Kafka produce response");

        static size_t MinSize() {
          return REQUEST_OR_RESPONSE_SIZE_SIZE + PRC::CORRELATION_ID_SIZE +
                 PRC::TOPIC_COUNT_SIZE;
        }

        TProduceResponseReader();

        virtual ~TProduceResponseReader() noexcept { }

        virtual void Clear() override;

        virtual void SetResponse(const void *response,
            size_t response_size) override;

        virtual int32_t GetCorrelationId() const override;

        virtual size_t GetNumTopics() const override;

        virtual bool FirstTopic() override;

        virtual bool NextTopic() override;

        virtual const char *GetCurrentTopicNameBegin() const override;

        virtual const char *GetCurrentTopicNameEnd() const override;

        virtual size_t GetNumPartitionsInCurrentTopic() const override;

        virtual bool FirstPartitionInTopic() override;

        virtual bool NextPartitionInTopic() override;

        virtual int32_t GetCurrentPartitionNumber() const override;

        virtual int16_t GetCurrentPartitionErrorCode() const override;

        virtual int64_t GetCurrentPartitionOffset() const override;

        private:
        using PRC = TProduceResponseConstants;

        const uint8_t *GetPartitionStart(size_t index) const;

        void InitCurrentTopic();

        void InitCurrentPartition();

        const uint8_t *Begin;

        const uint8_t *End;

        int32_t NumTopics;

        int32_t CurrentTopicIndex;

        const uint8_t *CurrentTopicBegin;

        const uint8_t *CurrentTopicNameEnd;

        int32_t NumPartitionsInTopic;

        int32_t CurrentPartitionIndexInTopic;
      };  // TProduceResponseReader

    }  // V0

  }  // KafkaProto

}  // Bruce
