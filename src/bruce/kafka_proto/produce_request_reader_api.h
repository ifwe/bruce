/* <bruce/kafka_proto/produce_request_reader_api.h>

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

   API definition for produce request reader abstraction.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>

#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace KafkaProto {

    class TProduceRequestReaderApi {
      NO_COPY_SEMANTICS(TProduceRequestReaderApi);

      public:
      class TBadProduceRequest : public std::runtime_error {
        public:
        virtual ~TBadProduceRequest() noexcept { }

        protected:
        explicit TBadProduceRequest(const char *msg)
            : std::runtime_error(msg) {
        }
      };  // TBadProduceRequest

      virtual ~TProduceRequestReaderApi() noexcept { }

      virtual void Clear() = 0;

      virtual void SetRequest(const void *request, size_t request_size) = 0;

      virtual int32_t GetCorrelationId() const = 0;

      virtual const char *GetClientIdBegin() const = 0;

      virtual const char *GetClientIdEnd() const = 0;

      virtual int16_t GetRequiredAcks() const = 0;

      virtual int32_t GetReplicationTimeout() const = 0;

      virtual size_t GetNumTopics() const = 0;

      virtual bool FirstTopic() = 0;

      virtual bool NextTopic() = 0;

      virtual const char *GetCurrentTopicNameBegin() const = 0;

      virtual const char *GetCurrentTopicNameEnd() const = 0;

      virtual size_t GetNumMsgSetsInCurrentTopic() const = 0;

      virtual bool FirstMsgSetInTopic() = 0;

      virtual bool NextMsgSetInTopic() = 0;

      virtual int32_t GetPartitionOfCurrentMsgSet() const = 0;

      virtual bool FirstMsgInMsgSet() = 0;

      virtual bool NextMsgInMsgSet() = 0;

      virtual bool CurrentMsgCrcIsOk() const = 0;

      virtual uint8_t GetCurrentMsgAttributes() const = 0;

      virtual const uint8_t *GetCurrentMsgKeyBegin() const = 0;

      virtual const uint8_t *GetCurrentMsgKeyEnd() const = 0;

      virtual const uint8_t *GetCurrentMsgValueBegin() const = 0;

      virtual const uint8_t *GetCurrentMsgValueEnd() const = 0;

      protected:
      TProduceRequestReaderApi() = default;
    };  // TProduceRequestReaderApi

  }  // KafkaProto

}  // Bruce
