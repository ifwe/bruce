/* <bruce/kafka_proto/v0/msg_set_reader.h>

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

   Class for reading the contents of a message set.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include <base/thrower.h>
#include <bruce/kafka_proto/msg_set_reader_api.h>
#include <bruce/kafka_proto/v0/produce_request_constants.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TMsgSetReader final : public TMsgSetReaderApi {
        public:
        DEFINE_ERROR(TMsgSetTruncated, TBadMsgSet, "Message set is truncated");

        DEFINE_ERROR(TBadMsgSize, TBadMsgSet,
                     "Message set has message with invalid size");

        DEFINE_ERROR(TBadMsgKeySize, TBadMsgSet,
                     "Message set has message with invalid key size");

        DEFINE_ERROR(TBadMsgValueSize, TBadMsgSet,
                     "Message set has message with invalid value size");

        TMsgSetReader();

        virtual ~TMsgSetReader() noexcept { }

        virtual void Clear() override;

        virtual void SetMsgSet(const void *msg_set,
            size_t msg_set_size) override;

        virtual bool FirstMsg() override;

        virtual bool NextMsg() override;

        virtual bool CurrentMsgCrcIsOk() const override;

        virtual uint8_t GetCurrentMsgAttributes() const override;

        virtual const uint8_t *GetCurrentMsgKeyBegin() const override;

        virtual const uint8_t *GetCurrentMsgKeyEnd() const override;

        virtual const uint8_t *GetCurrentMsgValueBegin() const override;

        virtual const uint8_t *GetCurrentMsgValueEnd() const override;

        private:
        using PRC = TProduceRequestConstants;

        void InitCurrentMsg();

        const uint8_t *Begin;

        const uint8_t *End;

        size_t Size;

        const uint8_t *CurrentMsg;

        int32_t CurrentMsgSize;

        bool CurrentMsgCrcOk;

        const uint8_t *CurrentMsgKeyBegin;

        const uint8_t *CurrentMsgKeyEnd;

        const uint8_t *CurrentMsgValueBegin;

        const uint8_t *CurrentMsgValueEnd;
      };  // TMsgSetReader

    }  // V0

  }  // KafkaProto

}  // Bruce
