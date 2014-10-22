/* <bruce/kafka_proto/v0/protocol_util.h>

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

   Utility functions for dealing with general aspects of Kafka wire protocol.
 */

#pragma once

#include <cstddef>
#include <stdexcept>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      enum { REQUEST_OR_RESPONSE_SIZE_SIZE = 4 };

      inline size_t BytesNeededToGetRequestOrResponseSize() {
        return REQUEST_OR_RESPONSE_SIZE_SIZE;
      }

      size_t GetRequestOrResponseSize(const void *response_begin);

    }  // V0

  }  // KafkaProto

}  // Bruce
