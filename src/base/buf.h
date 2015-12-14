/* <base/buf.h>

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

   General-purpose data buffer.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <utility>
#include <vector>

namespace Base {

  /* Buffer of items, where T gives the type of items in the buffer.  T must be
     a POD type, and is typically expected to be a built-in integer type such
     as char.  Items are added to the right and consumed from the left.  Items
     may be moved to the beginning (left end) of the buffer to make more space
     for additional items while avoiding unnecessary memory allocation.  The
     buffer's storage space may be removed and provided to the caller as a
     vector, leaving the buffer empty.  The caller may then fill the storage
     space with items and move the newly populated storage back into the
     buffer. */
  template <typename T>
  class TBuf final {
    public:
    using TStorage = typename std::vector<T>;

    /* Construct initially empty buffer. */
    TBuf()
        : ItemOffset(0),
          ItemCount(0) {
    }

    /* Construct buffer, populating it with 'items', whose contents are copied
       into the container. */
    explicit TBuf(const TStorage &items)
        : Storage(items),
          ItemOffset(0),
          ItemCount(Storage.size()) {
    }

    /* Construct buffer, populating it with 'items', whose contents are moved
       into the container. */
    TBuf(TStorage &&items)
        : Storage(std::move(items)),
          ItemOffset(0),
          ItemCount(Storage.size()) {
    }

    TBuf(const TBuf &) = default;

    /* Move constructor leaves 'that' empty. */
    TBuf(TBuf &&that)
        : Storage(std::move(that.Storage)),
          ItemOffset(that.ItemOffset),
          ItemCount(that.ItemCount) {
      that.ItemOffset = 0;
      that.ItemCount = 0;
    }

    TBuf &operator=(const TBuf &) = default;

    /* Move assignment operator leaves 'that' empty. */
    TBuf &operator=(TBuf &&that) {
      assert(this);

      if (&that != this) {
        Storage = std::move(that.Storage);
        ItemOffset = that.ItemOffset;
        ItemCount = that.ItemCount;
        that.ItemOffset = 0;
        that.ItemCount = 0;
      }

      return *this;
    }

    /* Move-assign 'items' into buffer, making 'items' its new contents.  May
       be used in combination with TakeStorage() to refill buffer. */
    TBuf &operator=(TStorage &&items) {
      assert(this);
      Storage = std::move(items);
      ItemOffset = 0;
      ItemCount = Storage.size();
      return *this;
    }

    /* Swap contents with contents of 'that'. */
    void Swap(TBuf &that) noexcept {
      assert(this);
      assert(&that);

      if (&that != this) {
        Storage.swap(that.Storage);
        std::swap(ItemOffset, that.ItemOffset);
        std::swap(ItemCount, that.ItemCount);
      }
    }

    /* Mark all data as consumed, leaving nothing but empty space. */
    void Clear() noexcept {
      assert(this);
      MarkDataConsumed(ItemCount);
    }

    /* Remove and return buffer's internal storage, leaving buffer empty.
       Typically this is done once all data has been consumed.  This allows
       caller to refill storage with data and then reload buffer by
       move-assigning data back into buffer.  Doing this allows easy refilling
       by routines that take as input a vector to fill with data.  It allows
       underlying memory allocated for storage to be reused. */
    TStorage TakeStorage() {
      assert(this);
      ItemOffset = 0;
      ItemCount = 0;
      return std::move(Storage);
    }

    /* Return pointer to start location where additional items are expected to
       be deposited.  Do not call unless SpaceSize() returns a value > 0. */
    T *Space() noexcept {
      assert(this);
      assert(ItemOffset < Storage.size());
      assert(ItemCount < (Storage.size() - ItemOffset));
      assert(SpaceSize() > 0);
      return &Storage[0] + ItemOffset + ItemCount;
    }

    /* Return number of items the region returned by Space() can hold. */
    size_t SpaceSize() const noexcept {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));
      return Storage.size() - ItemOffset - ItemCount;
    }

    /* Return true if buffer currently has no space where additional items can
       be deposited, or false otherwise. */
    bool SpaceIsEmpty() const noexcept {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));
      return (SpaceSize() == 0);
    }

    /* Return pointer to start location of received items that are ready to be
       consumed.  Do not call unless DataSize() returns a value > 0. */
    T *Data() noexcept {
      assert(this);
      assert(ItemOffset < Storage.size());
      assert(ItemCount <= (Storage.size() - ItemOffset));
      assert(DataSize() > 0);
      return &Storage[0] + ItemOffset;
    }

    /* Return const pointer to start location of received items that are ready
       to be consumed.  Do not call unless DataSize() returns a value > 0. */
    const T *Data() const noexcept {
      assert(this);
      assert(ItemOffset < Storage.size());
      assert(ItemCount <= (Storage.size() - ItemOffset));
      assert(DataSize() > 0);
      return &Storage[0] + ItemOffset;
    }

    /* Return number of items the region returned by Data() holds. */
    size_t DataSize() const noexcept {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));
      return ItemCount;
    }

    /* Return true if buffer currently has no received items that are ready to
       be consumed, or false otherwise. */
    bool DataIsEmpty() const noexcept {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));
      return (DataSize() == 0);
    }

    /* Move items to front (left end) of buffer.  This makes additional space
       available without allocating memory.  This method is a no-op if the
       buffer is empty or its items are already at the front. */
    void MoveDataToFront() noexcept {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));

      if (ItemOffset) {
        assert(ItemCount > 0);
        std::memmove(&Storage[0], &Storage[0] + ItemOffset,
            ItemCount * sizeof(T));
        ItemOffset = 0;
      }
    }

    /* Make room for at least 'min_to_add' additional items to be added (beyond
       any initially available space).  This is accomplished by first moving
       the items to the left end of the buffer, and then growing the storage by
       the minimal extra amount necessary to ensure that space is available for
       'min_to_add' items.  Note that moving the items to the left may make
       more than 'min_to_add' extra space available. */
    void AddSpace(size_t min_to_add) {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));

      if (min_to_add) {
        DoAddSpace(min_to_add);
      }
    }

    /* Ensure that space for at least 'min_size' items is available.  If
       necessary, move items to left end of buffer and possibly allocate
       additional storage. */
    void EnsureSpace(size_t min_size) {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));
      size_t actual = SpaceSize();

      if (actual < min_size) {
        DoAddSpace(min_size - actual);
      }
    }

    /* Ensure that the total available item count plus the number of additional
       items the buffer can hold is at least 'min_size'.  If necessary, move
       items to left end of buffer and possibly allocate additional storage. */
    void EnsureDataPlusSpace(size_t min_size) {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));
      size_t actual = Storage.size() - ItemOffset;

      if (actual < min_size) {
        DoAddSpace(min_size - actual);
      }
    }

    /* Record that 'size' items have been added to the buffer, starting at the
       location returned by Space().  On return, the items are ready to be
       consumed.  This increases the value returned by DataSize() and decreases
       the value returned by SpaceSize(). */
    void MarkSpaceConsumed(size_t size) noexcept {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));
      assert(size <= SpaceSize());
      ItemCount += size;
    }

    /* Record that 'size' items have been consumed, starting at the location
       returned by Data().  This decreases the value returned by DataSize().
       It leaves the value returned by SpaceSize() unchanged, since the
       available space indicated by SpaceSize() is to the right of the
       unconsumed items, and consuming items makes space available at the left.
     */
    void MarkDataConsumed(size_t size) noexcept {
      assert(this);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));
      assert(size <= DataSize());
      ItemOffset += size;
      ItemCount -= size;

      if (ItemCount == 0) {
        ItemOffset = 0;
      }
    }

    private:
    void DoAddSpace(size_t min_to_add) {
      assert(this);
      assert(min_to_add);
      assert(Storage.empty() || (ItemOffset < Storage.size()));
      assert(ItemCount <= (Storage.size() - ItemOffset));
      size_t added_by_move = ItemOffset;
      MoveDataToFront();

      if (added_by_move < min_to_add) {
        Storage.resize(Storage.size() + min_to_add - added_by_move);
      }
    }

    /* Storage for buffer.  Items are added to the right and consumed from the
       left.  Items may be moved to left end of buffer to make space for more
       to be added. */
    TStorage Storage;

    /* Indicates position in 'Storage' of first item that is ready to be
       consumed. */
    size_t ItemOffset;

    /* Indicates number of items in 'Storage' that are ready to be consumed.
       Any space in 'Storage' following items available for consumption can be
       filled with additional items. */
    size_t ItemCount;
  };  // TBuf

}  // Base
