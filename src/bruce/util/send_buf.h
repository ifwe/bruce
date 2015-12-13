/* <bruce/util/send_buf.h>

   ----------------------------------------------------------------------------
   Copyright 2015 Dave Peterson <dave@dspeterson.com>

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

   Buffer for sending data.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <utility>
#include <vector>

namespace Bruce {

  namespace Util {

    /* Buffer of items waiting to be sent, where T gives the type of items in
       the buffer.  T must be a POD type, and is typically expected to be a
       built-in integer type such as char. */
    template <typename T>
    class TSendBuf final {
      public:
      /* Construct initially empty buffer. */
      TSendBuf()
          : ItemsRemaining(0) {
      }

      /* Return pointer to start of buffer's data.  Must be called only when
         buffer is nonempty. */
      T *Data() noexcept {
        assert(this);
        assert(ItemsRemaining > 0);
        assert(ItemsRemaining <= Buf.size());
        return &Buf[0] + Buf.size() - ItemsRemaining;
      }

      /* Return number of items in buffer. */
      size_t DataSize() const noexcept {
        assert(this);
        return ItemsRemaining;
      }

      /* Return true if buffer is empty or false otherwise. */
      bool IsEmpty() const noexcept {
        assert(this);
        return (DataSize() == 0);
      }

      /* Mark leftmost 'size' items in buffer as consumed, causing buffer to
         shrink, and its starting memory location to shift to the right. */
      void MarkConsumed(size_t size) noexcept {
        assert(this);
        assert(size <= ItemsRemaining);
        ItemsRemaining -= size;
      }

      /* Remove and return buffer's internal storage, leaving buffer empty.
         Typically this is done once all data has been consumed.  This
         allows caller to refill storage with data and then call PutBuf() to
         reload buffer with new data.  Doing this allows easy refilling by
         routines that take a vector for filling with data as input.  It allows
         underlying memory allocated for storage to be reused. */
      typename std::vector<T> GetBuf() {
        assert(this);
        ItemsRemaining = 0;
        return std::move(Buf);
      }

      /* Move data contained in 'buf' into buffer, leaving 'buf' empty.  The
         entire initial constnts of 'buf' becomes the buffer's new contents. */
      void PutBuf(typename std::vector<T> &&buf) {
        assert(this);
        Buf = std::move(buf);
        ItemsRemaining = Buf.size();
      }

      private:
      /* Storage for buffer.  Items are consumed from the left.  Whenever
         buffer is nonempty, last item is always at Buf[Buf.size() - 1].  When
         buffer is full, first item is at Buf[0].  After 1 item has been
         consumed, first item is at Buf[1]. */
      typename std::vector<T> Buf;

      /* Count of items remaining.  Always <= Buf.size().  Value is 0 when
         buffer is empty. */
      size_t ItemsRemaining;
    };  // TSendBuf

  }  // Util

}  // Bruce
