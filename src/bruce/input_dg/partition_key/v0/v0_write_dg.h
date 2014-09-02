/* <bruce/input_dg/partition_key/v0/v0_write_dg.h>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 Tagged

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

   Function for creating PartitionKey datagram for writing to Bruce's input
   socket.
 */

#pragma once

#include <cstdint>
#include <vector>

#include <bruce/client/status_codes.h>

namespace Bruce {

  namespace InputDg {

    namespace PartitionKey {

      namespace V0 {

        /* Write datagram into 'result_buf'.  'result buf' will be resized to
           the exact size of the written datagram.  Return BRUCE_OK on success.
           Return one of { BRUCE_TOPIC_TOO_LARGE, BRUCE_MSG_TOO_LARGE } on
           error.  See <bruce/client/status_codes.h> for status code
           definitions. */
        int WriteDg(std::vector<uint8_t> &result_buf, int64_t timestamp,
            int32_t partition_key, const void *topic_begin,
            const void *topic_end, const void *key_begin, const void *key_end,
            const void *value_begin, const void *value_end);

      }  // V0

    }  // PartitionKey

  }  // InputDg

}  // Bruce
