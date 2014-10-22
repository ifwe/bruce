/* <bruce/kafka_proto/v0/msg_set_writer.h>

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

   Class for writing a message set to a caller-supplied growable buffer of type
   std::vector<uint8_t>.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include <base/field_access.h>
#include <base/no_copy_semantics.h>
#include <bruce/kafka_proto/msg_set_writer_api.h>
#include <bruce/kafka_proto/v0/produce_request_constants.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TMsgSetWriter final : public TMsgSetWriterApi {
        NO_COPY_SEMANTICS(TMsgSetWriter);

        public:
        TMsgSetWriter();

        virtual ~TMsgSetWriter() noexcept { }

        virtual void Reset() override;

        virtual void OpenMsgSet(std::vector<uint8_t> &result_buf,
            bool append) override;

        virtual void OpenMsg(int8_t attributes, size_t key_size,
            size_t value_size) override;

        virtual size_t GetCurrentMsgKeyOffset() const override;

        virtual size_t GetCurrentMsgValueOffset() const override;

        virtual void AdjustValueSize(size_t new_size);

        virtual void RollbackOpenMsg() override;

        virtual void CloseMsg() override;

        virtual void AddMsg(int8_t attributes, const uint8_t *key_begin,
            const uint8_t *key_end, const uint8_t *value_begin,
            const uint8_t *value_end) override;

        virtual size_t CloseMsgSet() override;

        private:
        using PRC = TProduceRequestConstants;

        enum class TState {
          Idle,
          InMsgSet,
          InMsg
        };  // TState

        static size_t ComputeMsgMinusValueSize(size_t key_size) {
          return PRC::CRC_SIZE + PRC::MAGIC_BYTE_SIZE + PRC::ATTRIBUTES_SIZE +
              PRC::KEY_LEN_SIZE + key_size + PRC::VALUE_LEN_SIZE;
        }

        static size_t ComputeMsgSetItemSize(size_t msg_size) {
          return PRC::MSG_OFFSET_SIZE + PRC::MSG_SIZE_SIZE + msg_size;
        }

        void WriteInt8(size_t offset, int8_t value) {
          assert(this);
          assert(Buf);
          assert(Buf->size() > offset);
          (*Buf)[offset] = value;
        }

        void WriteInt8AtOffset(int8_t value) {
          assert(this);
          WriteInt8(AtOffset, value);
          ++AtOffset;
        }

        void WriteInt16(size_t offset, int16_t value) {
          assert(this);
          assert(Buf);
          assert(Buf->size() > (offset + 1));
          WriteInt16ToHeader(&(*Buf)[offset], value);
        }

        void WriteInt16AtOffset(int16_t value) {
          assert(this);
          WriteInt16(AtOffset, value);
          AtOffset += 2;
        }

        void WriteInt32(size_t offset, int32_t value) {
          assert(this);
          assert(Buf);
          assert(Buf->size() > (offset + 3));
          WriteInt32ToHeader(&(*Buf)[offset], value);
        }

        void WriteInt32AtOffset(int32_t value) {
          assert(this);
          WriteInt32(AtOffset, value);
          AtOffset += 4;
        }

        void WriteInt64(size_t offset, int64_t value) {
          assert(this);
          assert(Buf);
          assert(Buf->size() > (offset + 7));
          WriteInt64ToHeader(&(*Buf)[offset], value);
        }

        void WriteInt64AtOffset(int64_t value) {
          assert(this);
          WriteInt64(AtOffset, value);
          AtOffset += 8;
        }

        void WriteData(size_t offset, const void *data, size_t data_size) {
          assert(this);
          assert(Buf);
          assert(Buf->size() > (offset + data_size - 1));
          std::memcpy(&(*Buf)[offset], data, data_size);
        }

        void WriteDataAtOffset(const void *data, size_t data_size) {
          assert(this);
          WriteData(AtOffset, data, data_size);
          AtOffset += data_size;
        }

        std::vector<uint8_t> *Buf;

        TState State;

        size_t AtOffset;

        size_t MsgSetSize;

        size_t FirstMsgSetItemOffset;

        size_t CurrentMsgSetItemOffset;

        size_t MsgSetItemCount;

        size_t CurrentMsgCrcOffset;

        size_t CurrentMsgKeyOffset;

        size_t CurrentMsgValueOffset;

        size_t CurrentMsgKeySize;

        size_t CurrentMsgValueSize;
      };  // TMsgSetWriter

    }  // V0

  }  // KafkaProto

}  // Bruce
