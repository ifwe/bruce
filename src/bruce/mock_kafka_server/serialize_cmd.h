/* <bruce/mock_kafka_server/serialize_cmd.h>

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

   Serialization code for mock Kafka server error injection commands.
 */

#pragma once

#include <bruce/mock_kafka_server/cmd.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <base/field_access.h>

namespace Bruce {

  namespace MockKafkaServer {

    void SerializeCmd(const TCmd &cmd, std::vector<uint8_t> &buf);

    inline size_t BytesNeededToGetCmdSize() {
      /* 2 bytes are needed to determine the size of the entire command, since
         the first 2 bytes contain a 16-bit length field. */
      return 2;
    }

    inline size_t GetCmdSize(const std::vector<uint8_t> &buf) {
      assert(buf.size() >= BytesNeededToGetCmdSize());
      return ReadUint16FromHeader(&buf[0]);
    }

    bool DeserializeCmd(const std::vector<uint8_t> &buf, TCmd &result);

  }  // MockKafkaServer

}  // Bruce
