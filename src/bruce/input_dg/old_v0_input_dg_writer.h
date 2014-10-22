/* <bruce/input_dg/old_v0_input_dg_writer.h>

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

   Class for creating datagrams for writing to bruce's input socket.  Uses
   version 0 of input socket datagram format.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace InputDg {

    class TOldV0InputDgWriter final {
      NO_COPY_SEMANTICS(TOldV0InputDgWriter);

      public:
      TOldV0InputDgWriter() = default;

      /* Maximum allowed topic size in bytes. */
      enum { MAX_TOPIC_SIZE = std::numeric_limits<int8_t>::max() };

      /* Maximum allowed body size in bytes. */
      enum { MAX_BODY_SIZE = std::numeric_limits<int32_t>::max() };

      /* Return number of bytes required for entire datagram, assuming that
         'topic_size' gives topic size in bytes and 'body_size' gives body size
         in bytes. */
      static size_t ComputeDgSize(size_t topic_size, size_t body_size);

      /* Write datagram into 'result_buf'.  It is assumed that 'result_buf' has
         enough space for entire datagram (see ComputeDgSize()). */
      void WriteDg(void *result_buf, int64_t timestamp,
          const void *topic_begin, const void *topic_end,
          const void *body_begin, const void *body_end);

      /* Write datagram into 'result_buf'.  If 'result_buf' does not have
         enough space for entire datagram, then resize it to exactly the
         required amount of space.  If 'result_buf' already has enough space,
         then leave its size unchanged.  Return the size in bytes of the
         written datagram. */
      size_t WriteDg(std::vector<uint8_t> &result_buf, int64_t timestamp,
          const void *topic_begin, const void *topic_end,
          const void *body_begin, const void *body_end);
    };  // class TOldV0InputDgWriter

  }  // InputDg

}  // Bruce
