/* <bruce/mock_kafka_server/cmd.h>

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

   A command sent to the mock Kafka server (for error injection, etc.).
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <string>

#include <base/thrower.h>

namespace Bruce {

  namespace MockKafkaServer {

    struct TCmd final {
      enum class TType : uint8_t {
        SEND_PRODUCE_RESPONSE_ERROR = 0,
        DISCONNECT_ON_READ_PRODUCE_REQUEST = 1,
        DELAY_BEFORE_SEND_PRODUCE_RESPONSE = 2,
        SEND_METADATA_RESPONSE_ERROR = 3,
        DISCONNECT_ON_READ_METADATA_REQUEST = 4,
        DELAY_BEFORE_SEND_METADATA_RESPONSE = 5
      };  // TType

      /* This specifies the command type. */
      TType Type;

      /* A command can have 2 32-bit signed integer parameters: this is the
         first one. */
      int32_t Param1;

      /* A command can have 2 32-bit signed integer parameters: this is the
         second one. */
      int32_t Param2;

      /* For produce requests, this is a message string to match exactly.  For
         metadata requests, this is a topic string to match exactly. */
      std::string Str;

      /* A string representation of a client's IP address (for instance,
         "127.0.0.1").  If nonempty, the command is specific to the given
         client.  If empty, the command is for any client. */
      std::string ClientAddr;

      DEFINE_ERROR(TBadCmdType, std::runtime_error,
                   "Bad mock Kafka server command type");

      static TType ToType(uint8_t type);

      TCmd()
          : Type(TType::SEND_PRODUCE_RESPONSE_ERROR),
            Param1(0),
            Param2(0) {
      }

      TCmd(TType type, int32_t param1, int32_t param2,
           const std::string &str, const std::string &client_addr)
          : Type(type),
            Param1(param1),
            Param2(param2),
            Str(str),
            ClientAddr(client_addr) {
        /* The 255 byte size limits are imposed by the wire format. */
        assert(Str.size() <= 255);
        assert(ClientAddr.size() <= 255);
      }

      TCmd(TType type, int32_t param1, int32_t param2,
           std::string &&str, std::string &&client_addr)
          : Type(type),
            Param1(param1),
            Param2(param2),
            Str(std::move(str)),
            ClientAddr(std::move(client_addr)) {
        /* The 255 byte size limits are imposed by the wire format. */
        assert(Str.size() <= 255);
        assert(ClientAddr.size() <= 255);
      }

      TCmd(const TCmd &) = default;

      TCmd(TCmd &&) = default;

      TCmd &operator=(const TCmd &) = default;

      TCmd &operator=(TCmd &&) = default;

      void Swap(TCmd &that) {
        std::swap(Type, that.Type);
        std::swap(Param1, that.Param1);
        std::swap(Param2, that.Param2);
        Str.swap(that.Str);
        ClientAddr.swap(that.ClientAddr);
      }
    };  // TCmd

  }  // MockKafkaServer

}  // Bruce
