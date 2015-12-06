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

    template <typename T>
    class TSendBuf final {
      public:
      TSendBuf()
          : ItemsRemaining(0) {
      }

      T *Data() noexcept {
        assert(this);
        assert(ItemsRemaining <= Buf.size());
        return &Buf[0] + Buf.size() - ItemsRemaining;
      }

      size_t DataSize() const noexcept {
        assert(this);
        return ItemsRemaining;
      }

      bool IsEmpty() const noexcept {
        assert(this);
        return (DataSize() == 0);
      }

      void MarkConsumed(size_t size) noexcept {
        assert(this);
        assert(size <= ItemsRemaining);
        ItemsRemaining -= size;
      }

      typename std::vector<T> GetBuf() {
        assert(this);
        ItemsRemaining = 0;
        return std::move(Buf);
      }

      void PutBuf(typename std::vector<T> &&buf) {
        assert(this);
        Buf = std::move(buf);
        ItemsRemaining = Buf.size();
      }

      private:
      typename std::vector<T> Buf;

      size_t ItemsRemaining;
    };  // TSendBuf

  }  // Util

}  // Bruce
