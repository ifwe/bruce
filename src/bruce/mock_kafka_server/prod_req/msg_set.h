/* <bruce/mock_kafka_server/prod_req/msg_set.h>

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

   Message set representation for mock Kafka server.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <vector>

#include <bruce/conf/compression_type.h>
#include <bruce/mock_kafka_server/prod_req/msg.h>

namespace Bruce {

  namespace MockKafkaServer {

    namespace ProdReq {

      class TMsgSet final {
        public:
        explicit TMsgSet(int32_t partition)
            : Partition(partition),
              CompressionType(Conf::TCompressionType::None),
              MsgCrcsOk(true) {
        }

        TMsgSet(const TMsgSet &) = default;

        TMsgSet(TMsgSet &&) = default;

        TMsgSet &operator=(const TMsgSet &) = default;

        TMsgSet &operator=(TMsgSet &&) = default;

        void AddMsg(const TMsg &msg) {
          assert(this);
          MsgVec.push_back(msg);
          MsgCrcsOk = MsgCrcsOk && msg.GetCrcOk();
        }

        void AddMsg(TMsg &&msg) {
          assert(this);
          bool crc_ok = msg.GetCrcOk();
          MsgVec.push_back(std::move(msg));
          MsgCrcsOk = MsgCrcsOk && crc_ok;
        }

        int32_t GetPartition() const {
          assert(this);
          return Partition;
        }

        void SetCompressionType(Conf::TCompressionType compression_type) {
          assert(this);
          CompressionType = compression_type;
        }

        Conf::TCompressionType GetCompressionType() const {
          assert(this);
          return CompressionType;
        }

        bool GetMsgCrcsOk() const {
          assert(this);
          return MsgCrcsOk;
        }

        const std::vector<TMsg> &GetMsgVec() const {
          assert(this);
          return MsgVec;
        }

        private:
        int32_t Partition;

        Conf::TCompressionType CompressionType;

        bool MsgCrcsOk;

        std::vector<TMsg> MsgVec;
      };  // TMsgSet

    }  // ProdReq

  }  // MockKafkaServer

}  // Bruce
