/* <bruce/kafka_proto/produce_request_writer_api.h>

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

   API definition for produce request writer abstraction.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace KafkaProto {

    class TProduceRequestWriterApi {
      NO_COPY_SEMANTICS(TProduceRequestWriterApi);

      public:
      virtual ~TProduceRequestWriterApi() noexcept { }

      virtual void Reset() = 0;

      virtual void OpenRequest(std::vector<uint8_t> &result_buf,
          int32_t corr_id, const char *client_id_begin,
          const char *client_id_end, int16_t required_acks,
          int32_t replication_timeout) = 0;

      virtual void OpenTopic(const char *topic_name_begin,
          const char *topic_name_end) = 0;

      virtual void OpenMsgSet(int32_t partition) = 0;

      virtual void OpenMsg(int8_t attributes, size_t key_size,
          size_t value_size) = 0;

      virtual size_t GetCurrentMsgKeyOffset() const = 0;

      virtual size_t GetCurrentMsgValueOffset() const = 0;

      virtual void AdjustValueSize(size_t new_size) = 0;

      virtual void RollbackOpenMsg() = 0;

      virtual void CloseMsg() = 0;

      virtual void AddMsg(int8_t attributes, const uint8_t *key_begin,
          const uint8_t *key_end, const uint8_t *value_begin,
          const uint8_t *value_end) = 0;

      virtual void CloseMsgSet() = 0;

      virtual void CloseTopic() = 0;

      virtual void CloseRequest() = 0;

      protected:
      TProduceRequestWriterApi() = default;
    };  // TProduceRequestWriterApi

  }  // KafkaProto

}  // Bruce
