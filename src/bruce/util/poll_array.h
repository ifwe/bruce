/* <bruce/util/poll_array.h>

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

   Utility class for poll() system call.
 */

#pragma once

#include <cassert>
#include <cstddef>

#include <poll.h>

#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace Util {

    template <typename TEnum, size_t SIZE>
    class TPollArray final {
      NO_COPY_SEMANTICS(TPollArray);

      public:
      TPollArray() {
        Clear();
      }

      size_t Size() const {
        assert(this);
        return SIZE;
      }

      const struct pollfd &operator[](TEnum index) const {
        assert(this);
        size_t n = static_cast<size_t>(index);
        assert(n < SIZE);
        return Items[n];
      }

      struct pollfd &operator[](TEnum index) {
        assert(this);
        size_t n = static_cast<size_t>(index);
        assert(n < SIZE);
        return Items[n];
      }

      operator const struct pollfd *() const {
        assert(this);
        return Items;
      }

      operator struct pollfd *() {
        assert(this);
        return Items;
      }

      void Clear() {
        assert(this);

        for (size_t i = 0; i < SIZE; ++i) {
          DoClear(i);
        }
      }

      void Clear(TEnum index) {
        assert(this);
        DoClear(static_cast<size_t>(index));
      }

      private:
      void DoClear(size_t index) {
        assert(this);
        assert(index < SIZE);
        struct pollfd &item = Items[index];
        item.fd = -1;
        item.events = 0;
        item.revents = 0;
      }

      struct pollfd Items[SIZE];
    };  // TPollArray

  }  // Util

}  // Bruce
