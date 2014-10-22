/* <bruce/kafka_proto/produce_response_writer_api.h>

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

   API definition for produce response writer abstraction.
 */

#pragma once

#include <cstdint>
#include <vector>

#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace KafkaProto {

    class TProduceResponseWriterApi {
      NO_COPY_SEMANTICS(TProduceResponseWriterApi);

      public:
      virtual ~TProduceResponseWriterApi() noexcept { }

      virtual void Reset() = 0;

      virtual void OpenResponse(std::vector<uint8_t> &out,
          int32_t correlation_id) = 0;

      virtual void OpenTopic(const char *topic_begin,
          const char *topic_end) = 0;

      virtual void AddPartition(int32_t partition, int16_t error_code,
          int64_t offset) = 0;

      virtual void CloseTopic() = 0;

      virtual void CloseResponse() = 0;

      protected:
      TProduceResponseWriterApi() = default;
    };  // TProduceResponseWriterApi

  }  // KafkaProto

}  // Bruce
