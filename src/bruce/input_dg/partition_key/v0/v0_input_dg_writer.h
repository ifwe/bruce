/* <bruce/input_dg/partition_key/v0/v0_input_dg_writer.h>

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

   Class for creating datagrams for writing to bruce's input socket.  Uses
   version 0 of input socket datagram format for PartitionKey messages.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace InputDg {

    namespace PartitionKey {

      namespace V0 {

        class TV0InputDgWriter final {
          NO_COPY_SEMANTICS(TV0InputDgWriter);

          public:
          TV0InputDgWriter() noexcept = default;

          enum class TDgSizeResult {
            Ok,  // size computed successfully
            TopicTooLarge,  // topic exceeds max allowed length
            MsgTooLarge  // datagram would exceed max possible size
          };  // TDgSizeResult

          static TDgSizeResult CheckDgSize(size_t topic_size, size_t key_size,
              size_t value_size) noexcept;

          /* If return value is TDgSizeResult::Ok then on return, 'result' will
             contain the number of bytes required for the entire datagram. */
          static TDgSizeResult ComputeDgSize(size_t &result, size_t topic_size,
              size_t key_size, size_t value_size) noexcept;

          /* Write datagram into 'result_buf'.  It is assumed that 'result_buf'
             has enough space for entire datagram (see ComputeDgSize()). */
          void WriteDg(void *result_buf, int64_t timestamp,
              int32_t partition_key, const void *topic_begin,
              const void *topic_end, const void *key_begin,
              const void *key_end, const void *value_begin,
              const void *value_end) noexcept {
            DoWriteDg(true, result_buf, timestamp, partition_key, topic_begin,
                      topic_end, key_begin, key_end, value_begin, value_end);
          }

          private:
          void DoWriteDg(bool check_size, void *result_buf, int64_t timestamp,
              int32_t partition_key, const void *topic_begin,
              const void *topic_end, const void *key_begin,
              const void *key_end, const void *value_begin,
              const void *value_end) noexcept;
        };  // class TV0InputDgWriter

      }  // V0

    }  // PartitionKey

  }  // InputDg

}  // Bruce
