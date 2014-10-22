/* <bruce/kafka_proto/produce_response_reader_api.h>

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

   API definition for produce response reader abstraction.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>

#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace KafkaProto {

    class TProduceResponseReaderApi {
      NO_COPY_SEMANTICS(TProduceResponseReaderApi);

      public:
      class TBadProduceResponse : public std::runtime_error {
        public:
        virtual ~TBadProduceResponse() noexcept { }

        protected:
        explicit TBadProduceResponse(const char *msg)
            : std::runtime_error(msg) {
        }
      };  // TBadProduceResponse

      virtual ~TProduceResponseReaderApi() noexcept { }

      virtual void Clear() = 0;

      virtual void SetResponse(const void *response, size_t response_size) = 0;

      virtual int32_t GetCorrelationId() const = 0;

      virtual size_t GetNumTopics() const = 0;

      virtual bool FirstTopic() = 0;

      virtual bool NextTopic() = 0;

      virtual const char *GetCurrentTopicNameBegin() const = 0;

      virtual const char *GetCurrentTopicNameEnd() const = 0;

      virtual size_t GetNumPartitionsInCurrentTopic() const = 0;

      virtual bool FirstPartitionInTopic() = 0;

      virtual bool NextPartitionInTopic() = 0;

      virtual int32_t GetCurrentPartitionNumber() const = 0;

      virtual int16_t GetCurrentPartitionErrorCode() const = 0;

      virtual int64_t GetCurrentPartitionOffset() const = 0;

      protected:
      TProduceResponseReaderApi() = default;
    };  // TProduceResponseReaderApi

  }  // KafkaProto

}  // Bruce
