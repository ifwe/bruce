/* <bruce/util/receive_buf.h>

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

   Buffer for receiving data.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <vector>

namespace Bruce {

  namespace Util {

    template <typename T>
    class TReceiveBuf final {
      public:
      TReceiveBuf()
          : DataOffset(0),
            ItemCount(0) {
      }

      T *Space() noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return &Buf[0] + DataOffset + ItemCount;
      }

      size_t SpaceSize() const noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return Buf.size() - DataOffset - ItemCount;
      }

      bool SpaceIsEmpty() const noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return (SpaceSize() == 0);
      }

      T *Data() noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return &Buf[0] + DataOffset;
      }

      size_t DataSize() const noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return ItemCount;
      }

      bool DataIsEmpty() const noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return (DataSize() == 0);
      }

      void MoveDataToFront() noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));

        if (DataOffset) {
          std::memmove(&Buf[0], &Buf[0] + DataOffset, ItemCount * sizeof(T));
          DataOffset = 0;
        }
      }

      void AddSpace(size_t min_to_add) {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        size_t added_by_move = DataOffset;
        MoveDataToFront();

        if (added_by_move < min_to_add) {
          Buf.resize(Buf.size() + min_to_add - added_by_move);
        }
      }

      void EnsureSpace(size_t min_size) {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        size_t actual = SpaceSize();

        if (actual < min_size) {
          AddSpace(min_size - actual);
        }
      }

      void EnsureDataPlusSpace(size_t min_size) {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        size_t actual = Buf.size() - DataOffset;

        if (actual < min_size) {
          AddSpace(min_size - actual);
        }
      }

      void MarkSpaceConsumed(size_t size) noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        assert(size <= SpaceSize());
        ItemCount += size;
      }

      void MarkDataConsumed(size_t size) noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        assert(size <= DataSize());
        DataOffset += size;
        ItemCount -= size;

        if (ItemCount == 0) {
          DataOffset = 0;
        }
      }

      private:
      typename std::vector<T> Buf;

      size_t DataOffset;

      size_t ItemCount;
    };  // TReceiveBuf

  }  // Util

}  // Bruce
