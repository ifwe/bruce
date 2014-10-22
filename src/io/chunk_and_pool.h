/* <io/chunk_and_pool.h>

   ----------------------------------------------------------------------------
   Copyright 2010-2013 if(we)

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

   Chunks of data in a data stream and pools for managing them.
 */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <queue>
#include <stdexcept>

#include <base/no_copy_semantics.h>
#include <base/no_throw.h>

namespace Io {

  /* TChunk makes reference to TPool. */
  class TPool;

  /* A chunk of data in a data stream. */
  class TChunk {
    NO_COPY_SEMANTICS(TChunk);
    public:

    /* Passed to a constructor when constructing an empty chunk. */
    enum TEmpty { Empty };

    /* Passed to a constructor when constructing a full chunk. */
    enum TFull { Full };

    /* Construct as an empty chunk, ready to have data stored in it.
       We will not own the storage space, merely point at it.
       We will not be recycled in a pool. */
    TChunk(TEmpty, void *start, void *limit) {
      assert(start <= limit);
      Start = static_cast<char *>(start);
      Limit = static_cast<char *>(limit);
      Cursor = Start;
      MustFree = false;
    }

    /* Construct as an empty chunk, ready to have data stored in it.
       We will not own the storage space, merely point at it.
       We will not be recycled in a pool. */
    TChunk(TEmpty, void *start, size_t size) {
      Start = static_cast<char *>(start);
      Limit = Start + size;
      Cursor = Start;
      MustFree = false;
    }

    /* Construct as a chunk already full of data.
       It will not be possible to store any more data in this chunk.
       We will not own the storage space, merely point at it.
       We will not be recycled in a pool. */
    TChunk(TFull, const void *start, const void *limit) {
      assert(start <= limit);
      Start = const_cast<char *>(static_cast<const char *>(start));
      Limit = const_cast<char *>(static_cast<const char *>(limit));
      Cursor = Limit;
      MustFree = false;
    }

    /* Construct as a chunk already full of data.
       It will not be possible to store any more data in this chunk.
       We will not own the storage space, merely point at it.
       We will not be recycled in a pool. */
    TChunk(TFull, const void *start, size_t size) {
      Start = const_cast<char *>(static_cast<const char *>(start));
      Limit = Start + size;
      Cursor = Limit;
      MustFree = false;
    }

    /* Construct as a chunk already full of data.
       It will not be possible to store any more data in this chunk.
       We will not own the storage space, merely point at it.
       We will not be recycled in a pool. */
    TChunk(TFull, const char *str) {
      if (str) {
        Start = const_cast<char *>(str);
        Limit = Start + strlen(Start);
      } else {
        Start = 0;
        Limit = 0;
      }

      Cursor = Limit;
      MustFree = false;
    }

    /* Construct as a chunk already full of data.
       It will not be possible to store any more data in this chunk.
       We will not own the storage space, merely point at it.
       We will not be recycled in a pool. */
    TChunk(TFull, const std::string &str) {
      assert(&str);
      Start = const_cast<char *>(str.data());
      Limit = Start + str.size();
      Cursor = Limit;
      MustFree = false;
    }

    /* If we own our storage space, we'll free it now; otherwise, so we'll
       leave the storage space alone and die quietly. */
    ~TChunk();

    /* Commit the given number of bytes in the storage buffer. */
    void Commit(size_t size) {
      assert(this);
      Cursor += size;
      assert(Cursor <= Limit);
    }

    /* The start of the available storage space. */
    char *GetBuffer() const {
      assert(this);
      return Cursor;
    }

    /* Return pointers to the data in this chunk.
       Upon return, start will always be <= limit. */
    void GetData(const char *&start, const char *&limit) const NO_THROW {
      assert(this);
      assert(&start);
      assert(&limit);
      start = Start;
      limit = Cursor;
    }

    /* The number of bytes remaining in our storage space. */
    size_t GetRemainingSize() const {
      assert(this);
      return Limit - Cursor;
    }

    /* The number of bytes in this chunk. */
    size_t GetSize() const {
      assert(this);
      return Cursor - Start;
    }

    /* Read some data from the given fd, storing it at the end of our current
       data.  Return the number of bytes stored.  If we are full, read nothing
       and return zero. */
    size_t Store(int fd);

    /* Copy some data from the given buffer, storing it at the end of our
       current data.  If we we are not already full, then advance the given
       buffer pointer by the number of bytes copied, decrement the given size
       by the number of bytes copied, and return true; otherwise, copy nothing,
       leave the buffer pointer and size values alone, and return false. */
    bool Store(const char *&buf, size_t &size);

    private:
    /* Construct as part of a pool of recycling chunks.  We will allocate and
       our own storage space and free it when we destruct.  Only a pool can
       construct us in this way. */
    TChunk(size_t size);

    /* The start and limit of our storage space.  We may or may not have
       allocated this memory for ourselves.  It depends on how we were
       constructed.  See MustFree for more information. */
    char *Start, *Limit;

    /* The end of our current data.  When this is equal to Start, we are empty.
       When this is equal to Limit, we are full. */
    char *Cursor;

    /* If false, then we don't own the memory pointed to by Start; otherwise,
       we do own the memory must free it when we destruct.  This always false
       when we are constructed via our public constructors.  Only the private
       constructor used by pool can construct us such that this flag will be
       true. */
    bool MustFree;

    /* The pool to which we must return when released.  If MustFree is false,
       then this pointer will always be null.  Manually constructed chunks
       don't get recycled.  If MustFree is true but we're currently in our
       pool's free queue, this pointer will also be null.  This pointer is
       non-null only for a pooled chunk which is currently in use. */
    std::shared_ptr<TPool> Pool;

    /* For access to the private constructor, MustFree, Pool, and Cursor. */
    friend class TPool;
  };  // TChunk

  /* A pool of chunks. */
  class TPool : public std::enable_shared_from_this<TPool> {
    NO_COPY_SEMANTICS(TPool);

    public:
    /* The error thrown by AcquireChunk() when a pool of fixed size is empty.
     */
    class TOutOfChunksError : public std::runtime_error {
      public:
      /* Do-little. */
      TOutOfChunksError()
          : std::runtime_error("out of I/O chunks") {
      }
    };  // TOutOfChunksError

    /* Construction defaults for TArgs, below. */
    static const size_t
        DefaultChunkSize = 65536,
        DefaultInitialChunkCount = 0,
        DefaultAdditionalChunkCount = 1;

    /* Arguments used to construct a new pool. */
    class TArgs {
      public:
      /* Initialize. */
      explicit TArgs(
          size_t chunk_size = DefaultChunkSize,
          size_t initial_chunk_count = DefaultInitialChunkCount,
          size_t additional_chunk_count = DefaultAdditionalChunkCount,
          const std::shared_ptr<std::function<TChunk *()>> &next_chunk_cb =
              std::shared_ptr<std::function<TChunk *()>>())
          : ChunkSize(chunk_size),
            InitialChunkCount(initial_chunk_count),
            AdditionalChunkCount(additional_chunk_count),
            NextChunkCb(next_chunk_cb) {
      }

      /* When constructing a new chunk, this value determines how many bytes,
         at most, the chunk can contain.  Must not be zero. */
      size_t ChunkSize;

      /* The number of chunks to construct right away, before the pool's
         constructor returns.  Set this higher if you anticipate using lots of
         chunks all the time.  Can be zero. */
      size_t InitialChunkCount;

      /* When the pool runs out of chunks, and yet more are needed, this is the
         number of additional chunks to allocate at once.  If this is zero, the
         pool will be of fixed size. */
      size_t AdditionalChunkCount;

      std::shared_ptr<std::function<TChunk *()>> NextChunkCb;
    };  // Args

    /* The pool will create chunks of the given maximum size.  It will start
       with the given number of initial chunks ready to be used.  If additional
       count is non-zero, then the pool will grow by this many chunks each time
       it runs out.  If additional count is zero, the pool will not grow.  It
       will be of fixed size. */
    TPool(const TArgs &args = TArgs())
        : ChunkSize(args.ChunkSize),
          AdditionalChunkCount(args.AdditionalChunkCount),
          NextChunkCb(args.NextChunkCb) {
      assert(args.ChunkSize);
      EnqueueNewChunks(args.InitialChunkCount);
    }

    /* Also disposes of any unused chunks.  It is an error to destroy a pool
       while any of its chunks are still in use. */
    ~TPool();

    /* Return an empty chunk, ready to be filled with data.  If there are no
       more chunks and the pool is of fixed size, throw. */
    std::shared_ptr<TChunk> AcquireChunk();

    /* Push the given chunk onto the free queue.  If the push cannot be
       accomplished, delete the chunk and throw. */
    void EnqueueChunk(TChunk *chunk);

    /* The number of chunks in the free queue.  This information is useful for
       testing. */
    size_t GetFreeChunkCount() const {
      assert(this);
      return FreeChunks.size();
    }

    private:
    /* Grow the queue of free chunks. */
    void EnqueueNewChunks(size_t chunk_count);

    /* If we're part of a pool, then this function is called when our last
       shared pointer is released.  The result is that we rejoin our pool's
       collection of free chunks, ready to be used again.  If we're not part of
       a pool, then this function is never called. */
    static void OnRelease(TChunk *chunk);

    /* The size (in bytes) of each direct chunk in the pool. */
    size_t ChunkSize;

    /* The number of additional chunks to create each time we run out.  If this
       is zero, the pool will not grow and will remain fixed at its initial
       size. */
    size_t AdditionalChunkCount;

    /* The queue of free chunks.  This may be empty.  It will never contains
       nulls. */
    std::queue<TChunk *> FreeChunks;

    std::shared_ptr<std::function<TChunk *()>> NextChunkCb;
  };  // TPool

}  // Io
