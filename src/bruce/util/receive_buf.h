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

    /* Buffer of items being received, where T gives the type of items in the
       buffer.  T must be a POD type, and is typically expected to be a
       built-in integer type such as char.  Received items are added to the
       right and consumed from the left.  Items may be moved to the beginning
       (left end) of the buffer to make more space for additional items while
       avoiding unnecessary memory allocation. */
    template <typename T>
    class TReceiveBuf final {
      public:
      /* Construct initially empty buffer. */
      TReceiveBuf()
          : DataOffset(0),
            ItemCount(0) {
      }

      /* Return pointer to start location where additional items are expected
         to be deposited.  Do not call unless SpaceSize() returns a value > 0.
       */
      T *Space() noexcept {
        assert(this);
        assert(DataOffset < Buf.size());
        assert(ItemCount < (Buf.size() - DataOffset));
        assert(SpaceSize() > 0);
        return &Buf[0] + DataOffset + ItemCount;
      }

      /* Return number of items the region returned by Space() can hold. */
      size_t SpaceSize() const noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return Buf.size() - DataOffset - ItemCount;
      }

      /* Return true if buffer currently has no space where additional items
         can be deposited, or false otherwise. */
      bool SpaceIsEmpty() const noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return (SpaceSize() == 0);
      }

      /* Return pointer to start location of received items that are ready to
         be consumed.  Do not call unless DataSize() returns a value > 0. */
      T *Data() noexcept {
        assert(this);
        assert(DataOffset < Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        assert(DataSize() > 0);
        return &Buf[0] + DataOffset;
      }

      /* Return number of items the region returned by Data() holds. */
      size_t DataSize() const noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return ItemCount;
      }

      /* Return true if buffer currently has no received items that are ready
         to be consumed, or false otherwise. */
      bool DataIsEmpty() const noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        return (DataSize() == 0);
      }

      /* Move items to front (left end) of buffer.  This makes additional space
         available without allocating memory.  This method is a no-op if the
         buffer is empty or its items are already at the front. */
      void MoveDataToFront() noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));

        if (DataOffset) {
          assert(ItemCount > 0);
          std::memmove(&Buf[0], &Buf[0] + DataOffset, ItemCount * sizeof(T));
          DataOffset = 0;
        }
      }

      /* Make room for at least 'min_to_add' additional items to be added
         (beyond any initially available space).  This is accomplished by first
         moving the items to the left end of the buffer, and then growing the
         storage by the minimal extra amount necessary to ensure that space is
         available for 'min_to_add' items.  Note that moving the items to the
         left may make more than 'min_to_add' extra space available. */
      void AddSpace(size_t min_to_add) {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));

        if (min_to_add) {
          DoAddSpace(min_to_add);
        }
      }

      /* Ensure that space for at least 'min_size' items is available.  If
         necessary, move items to left end of buffer and possibly allocate
         additional storage. */
      void EnsureSpace(size_t min_size) {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        size_t actual = SpaceSize();

        if (actual < min_size) {
          DoAddSpace(min_size - actual);
        }
      }

      /* Ensure that the total available item count plus the number of
         additional items the buffer can hold is at least 'min_size'.  If
         necessary, move items to left end of buffer and possibly allocate
         additional storage. */
      void EnsureDataPlusSpace(size_t min_size) {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        size_t actual = Buf.size() - DataOffset;

        if (actual < min_size) {
          DoAddSpace(min_size - actual);
        }
      }

      /* Record that 'size' items have been added to the buffer, starting at
         the location returned by Space().  On return, the items are ready to
         be consumed.  This increases the value returned by DataSize() and
         decreases the value returned by SpaceSize(). */
      void MarkSpaceConsumed(size_t size) noexcept {
        assert(this);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        assert(size <= SpaceSize());
        ItemCount += size;
      }

      /* Record that 'size' items have been consumed, starting at the location
         returned by Data().  This decreases the value returned by DataSize().
         It leaves the value returned by SpaceSize() unchanged, since the
         available space indicated by SpaceSize() is to the right of the
         unconsumed items, and consuming items makes space available at the
         left. */
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
      void DoAddSpace(size_t min_to_add) {
        assert(this);
        assert(min_to_add);
        assert(DataOffset <= Buf.size());
        assert(ItemCount <= (Buf.size() - DataOffset));
        size_t added_by_move = DataOffset;
        MoveDataToFront();

        if (added_by_move < min_to_add) {
          Buf.resize(Buf.size() + min_to_add - added_by_move);
        }
      }

      /* Storage for buffer.  Items are added to the right and consumed from
         the left.  Items may be moved to left end of buffer to make space for
         more to be added. */
      typename std::vector<T> Buf;

      /* Indicates position in 'Buf' of first item that is ready to be
         consumed. */
      size_t DataOffset;

      /* Indicates number of items in 'Buf' that are ready to be consumed.
         Any space in 'Buf' following items available for consumption can be
         filled with additional items. */
      size_t ItemCount;
    };  // TReceiveBuf

  }  // Util

}  // Bruce
