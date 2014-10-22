/* <bruce/kafka_proto/v0/produce_request_reader.h>

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

   Class for reading the contents of a produce request.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include <base/thrower.h>
#include <bruce/kafka_proto/produce_request_reader_api.h>
#include <bruce/kafka_proto/v0/msg_set_reader.h>
#include <bruce/kafka_proto/v0/produce_request_constants.h>
#include <bruce/kafka_proto/v0/protocol_util.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TProduceRequestReader final : public TProduceRequestReaderApi {
        public:
        DEFINE_ERROR(TBadRequestSize, TBadProduceRequest,
                     "Produce request has bad size field");

        DEFINE_ERROR(TRequestTruncated, TBadProduceRequest,
                     "Produce request is truncated");

        DEFINE_ERROR(TBadApiKey, TBadProduceRequest,
                     "Produce request has bad API key");

        DEFINE_ERROR(TBadApiVersion, TBadProduceRequest,
                     "Produce request has bad API version");

        DEFINE_ERROR(TBadClientIdLen, TBadProduceRequest,
                     "Produce request has invalid client ID length");

        DEFINE_ERROR(TBadTopicCount, TBadProduceRequest,
                     "Produce request has invalid topic count");

        DEFINE_ERROR(TBadTopicNameLen, TBadProduceRequest,
                     "Produce request has invalid topic name length");

        DEFINE_ERROR(TBadPartitionCount, TBadProduceRequest,
                     "Produce request has invalid partition count");

        TProduceRequestReader();

        virtual ~TProduceRequestReader() noexcept { }

        virtual void Clear() override;

        virtual void SetRequest(const void *request,
            size_t request_size) override;

        virtual int32_t GetCorrelationId() const override;

        virtual const char *GetClientIdBegin() const override;

        virtual const char *GetClientIdEnd() const override;

        virtual int16_t GetRequiredAcks() const override;

        virtual int32_t GetReplicationTimeout() const override;

        virtual size_t GetNumTopics() const override;

        virtual bool FirstTopic() override;

        virtual bool NextTopic() override;

        virtual const char *GetCurrentTopicNameBegin() const override;

        virtual const char *GetCurrentTopicNameEnd() const override;

        virtual size_t GetNumMsgSetsInCurrentTopic() const override;

        virtual bool FirstMsgSetInTopic() override;

        virtual bool NextMsgSetInTopic() override;

        virtual int32_t GetPartitionOfCurrentMsgSet() const override;

        virtual bool FirstMsgInMsgSet() override;

        virtual bool NextMsgInMsgSet() override;

        virtual bool CurrentMsgCrcIsOk() const override;

        virtual uint8_t GetCurrentMsgAttributes() const override;

        virtual const uint8_t *GetCurrentMsgKeyBegin() const override;

        virtual const uint8_t *GetCurrentMsgKeyEnd() const override;

        virtual const uint8_t *GetCurrentMsgValueBegin() const override;

        virtual const uint8_t *GetCurrentMsgValueEnd() const override;

        private:
        using PRC = TProduceRequestConstants;

        static size_t MinSize() {
          return REQUEST_OR_RESPONSE_SIZE_SIZE + PRC::API_KEY_SIZE +
                 PRC::API_VERSION_SIZE + PRC::CORRELATION_ID_SIZE +
                 PRC::CLIENT_ID_LEN_SIZE + PRC::REQUIRED_ACKS_SIZE +
                 PRC::REPLICATION_TIMEOUT_SIZE + PRC::TOPIC_COUNT_SIZE;
        }

        void InitCurrentTopic();

        void InitCurrentPartition();

        const uint8_t *Begin;

        const uint8_t *End;

        size_t Size;

        int16_t ClientIdLen;

        size_t RequiredAcksOffset;

        int32_t NumTopics;

        int32_t CurrentTopicIndex;

        const uint8_t *CurrentTopicBegin;

        const uint8_t *CurrentTopicNameEnd;

        int32_t NumPartitionsInTopic;

        int32_t CurrentPartitionIndexInTopic;

        const uint8_t *CurrentPartitionBegin;

        const uint8_t *PartitionMsgSetBegin;

        const uint8_t *PartitionMsgSetEnd;

        TMsgSetReader MsgSetReader;
      };  // TProduceRequestReader

    }  // V0

  }  // KafkaProto

}  // Bruce
