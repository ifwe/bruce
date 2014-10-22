/* <capped/pool.h>

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

   A pool of storage blocks with capped memory usage.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <mutex>

#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <capped/memory_cap_reached.h>

namespace Capped {

  /* A pool of storage blocks with capped memory usage. */
  class TPool final {
    NO_COPY_SEMANTICS(TPool);

    public:
    enum class TSync : bool {
      Unguarded = false,
      Mutexed = true
    };  // TSync

    /* This structure sits at the start of each block of free storage. */
    struct TBlock final {

      /* Initialize this block and link it to the list. */
      TBlock(TBlock *&first_block) noexcept {
        Link(first_block);
      }

      /* Link this block to the head of the given list.  The previous linkage
         of this block is ignored and overwritten. */
      void Link(TBlock *&first_block) noexcept {
        assert(this);
        assert(&first_block);
        NextBlock = first_block;
        first_block = this;
      }

      /* Unlink and return the first block from a list, updating the list
         pointer.  If the list is empty, do nothing and return null. */
      static TBlock *Unlink(TBlock *&first_block) noexcept {
        assert(&first_block);
        TBlock *result;

        if (first_block) {
          result = first_block;
          first_block = result->NextBlock;
        } else {
          result = nullptr;
        }

        return result;
      }

      /* The block after this one in the list, or null if we're the last. */
      TBlock *NextBlock;

      /* A place-holder for data. */
      char Data[sizeof(void *)];
    };  // TPool::TBlock

    /* Return the number of bytes of overhead contained in a block.  This is
       the block size minus the amount of actual storage space in the block. */
    static size_t GetBlockOverhead() {
      return sizeof(void *);
    }

    /* Construct a pool which will hold the given number of blocks, each of
       which is of the given size.  */
    TPool(size_t block_size, size_t block_count, TSync sync_policy);

    /* Free all storage.  Make sure no one is using our storage before this
       happens. */
    ~TPool() noexcept;

    /* Allocate a block of storage, or throw TMemoryCapReached if we're out. */
    void *Alloc();

    /* Allocate a linked list of blocks, or throw TMemoryCapReached we don't
       have enough. */
    TBlock *AllocList(size_t block_count);

    /* Return a block of storage to the pool.  It's safe to free a null
       pointer, we just do nothing. */
    void Free(void *ptr) noexcept;

    /* Return a linked list of blocks to the pool.  It's safe to free an empty
       list, we just do nothing. */
    void FreeList(TBlock *first_block);

    /* The size of the data field in each block. */
    size_t GetDataSize() const {
      assert(this);
      return BlockSize - GetBlockOverhead();;
    }

    /* The number of blocks in the whole pool, free and allocated. */
    size_t GetBlockCount() const {
      assert(this);
      return BlockCount;
    }

    /* The size of each block, in bytes. */
    size_t GetBlockSize() const {
      assert(this);
      return BlockSize;
    }

    private:
    /* Similar to Free() but mutex is not acquired.  Assumes that 'ptr' is not
       null. */
    void DoFree(void *ptr) noexcept;

    /* Smilar to FreeList() but mutex is not acquired.  Assumes that
       'first_block' is not null. */
    void DoFreeList(TBlock *first_block);

    /* See accessors. */
    const size_t BlockSize, BlockCount;

    /* If true then the pool is protected by a mutex (see below).  Otherwise
       access to the pool is unsynchronized. */
    const bool Guarded;

    /* if 'Guarded' above is true then access to the pool is guarded by this
       mutex.  Otherwise the mutex is unused and access to the pool is
       unguarded. */
    std::mutex Mutex;

    /* The first block available to be allocated, or null if we're out of
       blocks. */
    TBlock *FirstFreeBlock;

    /* Our storage space.  Never null. */
    char *Storage;
  };  // TPool

}  // Capped
