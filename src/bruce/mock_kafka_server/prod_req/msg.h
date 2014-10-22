/* <bruce/mock_kafka_server/prod_req/msg.h>

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

   Message representation for mock Kafka server.
 */

#pragma once

#include <cassert>
#include <string>

namespace Bruce {

  namespace MockKafkaServer {

    namespace ProdReq {

      class TMsg final {
        public:
        TMsg()
            : CrcOk(true) {
        }

        TMsg(bool crc_ok, const void *key_begin, const void *key_end,
             const void *value_begin, const void *value_end)
            : CrcOk(crc_ok),
              Key(ToString(key_begin, key_end)),
              Value(ToString(value_begin, value_end)) {
        }

        TMsg(const TMsg &) = default;

        TMsg(TMsg &&) = default;

        TMsg &operator=(const TMsg &) = default;

        TMsg &operator=(TMsg &&) = default;

        bool GetCrcOk() const {
          assert(this);
          return CrcOk;
        }

        const std::string &GetKey() const {
          assert(this);
          return Key;
        }

        const std::string &GetValue() const {
          assert(this);
          return Value;
        }

        private:
        static std::string ToString(const void *begin, const void *end) {
          assert(begin || (!begin && !end));
          assert(end >= begin);

          if (!begin) {
            return std::string();
          }

          return std::string(reinterpret_cast<const char *>(begin),
                             reinterpret_cast<const char *>(end));
        }

        bool CrcOk;

        std::string Key;

        std::string Value;
      };  // TMsg

    }  // ProdReq

  }  // MockKafkaServer

}  // Bruce
