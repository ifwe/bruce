/* <capped/blob.h>

   ----------------------------------------------------------------------------
   Copyright 2013 if(we)

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

   A blob of data, stored as a series of linked blocks.
 */

#pragma once

#include <cassert>
#include <utility>

#include <capped/pool.h>

namespace Capped {

  class TReader;
  class TWriter;

  /* A blob of data, stored as a series of linked blocks. */
  class TBlob final {
    public:
    /* We use the same blocks that TPool uses. */
    using TBlock = TPool::TBlock;

    /* Default-construct an empty blob. */
    TBlob() noexcept
        : Pool(nullptr), FirstBlock(nullptr), LastBlockSize(0), NumBytes(0) {
    }

    /* Move the data from that blob into a new one, leaving that blob empty. */
    TBlob(TBlob &&that) noexcept
        : TBlob() {
      Swap(that);
    }

    /* No copying allowed. */
    TBlob(const TBlob &) = delete;

    /* Return our blocks to the pool, if we have any. */
    ~TBlob() noexcept {
      assert(this);

      if (Pool) {
        Pool->FreeList(FirstBlock);
      }
    }

    /* Move the data from that blob into this one, leaving that blob empty. */
    TBlob &operator=(TBlob &&that) noexcept {
      assert(this);
      TBlob temp(std::move(*this));
      return Swap(that);
    }

    /* No copying allowed. */
    TBlob &operator=(const TBlob &) = delete;

    /* Return the number of bytes in a (full) block.  All blocks except for
       possibly the last block are full. */
    size_t GetBlockSize() const {
      assert(this);
      assert(Pool);
      return Pool->GetDataSize();
    }

    /* True iff. this blob is non-empty. */
    operator bool() const noexcept {
      assert(this);
      return FirstBlock;
    }

    /* Return the total size in bytes of the data contained. */
    size_t Size() const {
      assert(this);
      return NumBytes;
    }

    /* On return, 'data' will point to the first byte of data contained in the
       first block, or will be set to nullptr if blob is empty.  Returned value
       is size in bytes of first block, or 0 if blob is empty. */
    size_t GetDataInFirstBlock(const char *&data) const {
      assert(this);
      char *block_data = nullptr;
      size_t ret = DoGetDataInFirstBlock(block_data);
      data = block_data;
      return ret;
    }

    /* On return, 'data' will point to the first byte of data contained in the
       first block, or will be set to nullptr if blob is empty.  Returned value
       is size in bytes of first block, or 0 if blob is empty. */
    size_t GetDataInFirstBlock(char *&data) {
      assert(this);
      return DoGetDataInFirstBlock(data);
    }

    /* Call back for each block of data in the blob.
       We avoid std::function here to remain low-memory safe. */
    template <typename TContext>
    bool ForEachBlock(bool (*cb)(const void *, size_t, TContext),
        TContext context) const {
      assert(this);
      assert(cb);

      for (TBlock *block = FirstBlock; block; block = block->NextBlock) {
        if (!cb(block->Data, block->NextBlock ? GetBlockSize() : LastBlockSize,
                context)) {
          return false;
        }
      }

      return true;
    }

    /* Return this blob to the default-constructed state; that is, empty, but
       still connected to the buffer pool. */
    TBlob &Reset() noexcept {
      assert(this);
      TBlob temp(std::move(*this));
      return *this;
    }

    /* Swap this blob with that one and return this one (which is now that
       one).  The two blobs can be connected to different pools; those
       connections are swapped, too. */
    TBlob &Swap(TBlob &that) noexcept {
      assert(this);
      std::swap(Pool, that.Pool);
      std::swap(FirstBlock, that.FirstBlock);
      std::swap(LastBlockSize, that.LastBlockSize);
      std::swap(NumBytes, that.NumBytes);
      return *this;
    }

    private:
    /* The constructor used by TWriter.  We just cache these values. */
    TBlob(TPool *pool, TBlock *first_block, size_t last_block_size,
        size_t num_bytes)
        : Pool(pool), FirstBlock(first_block), LastBlockSize(last_block_size),
          NumBytes(num_bytes) {
      assert((!pool && !first_block && !last_block_size) ||
             (pool && first_block && last_block_size));
    }

    size_t DoGetDataInFirstBlock(char *&data) const;

    /* The pool to which to return our blocks, or null if we're empty. */
    TPool *Pool;

    /* The first buffer in our linked list, or null if we're empty. */
    TBlock *FirstBlock;

    /* The number of bytes used in the last buffer's block.  All other buffers
       are completely full. */
    size_t LastBlockSize;

    /* The total size in bytes of the data contained. */
    size_t NumBytes;

    friend class TReader;
    friend class TWriter;

  };  // TBlob

}  // Capped

