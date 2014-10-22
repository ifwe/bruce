/* <io/chunk_and_pool.cc>

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

   Implements <io/chunk_and_pool.h>.
 */

#include <io/chunk_and_pool.h>

#include <algorithm>
#include <cstdlib>
#include <base/io_utils.h>

using namespace std;
using namespace Base;
using namespace Io;

TChunk::~TChunk() {
  assert(this);
  if (MustFree) {
    free(Start);
  }
}

size_t TChunk::Store(int fd) {
  assert(this);
  size_t read_size = GetRemainingSize();

  if (read_size) {
    read_size = ReadAtMost(fd, Cursor, read_size);
    Cursor += read_size;
  }

  return read_size;
}

bool TChunk::Store(const char *&buf, size_t &size) {
  assert(this);
  assert(&buf);
  assert(&size);
  assert(buf || !size);
  size_t copy_size = GetRemainingSize();
  bool success = (copy_size != 0);

  if (success) {
    copy_size = min(copy_size, size);
    memcpy(Cursor, buf, copy_size);
    Cursor += copy_size;
    buf += copy_size;
    size -= copy_size;
  }

  return success;
}

TChunk::TChunk(size_t size) {
  assert(size);
  Start = static_cast<char *>(malloc(size));

  if (!Start) {
    throw bad_alloc();
  }

  Limit = Start + size;
  Cursor = Start;
  MustFree = true;
}

TPool::~TPool() {
  assert(this);

  while (!FreeChunks.empty()) {
    delete FreeChunks.front();
    FreeChunks.pop();
  }
}

shared_ptr<TChunk> TPool::AcquireChunk() {
  assert(this);
  shared_ptr<TChunk> result;
  TChunk *chunk;

  if (!FreeChunks.empty()) {
    // Recycle a chunk from the free queue.
    chunk = FreeChunks.front();
    FreeChunks.pop();
  } else if (AdditionalChunkCount) {
    // Construct some new chunks.
    EnqueueNewChunks(AdditionalChunkCount - 1);
    chunk = new TChunk(ChunkSize);
  } else if (NextChunkCb) {
    // We're empty and have a callback for more chunks.
    chunk = (*NextChunkCb)();

    if (!chunk) {
      // We're empty and the callback has no more chunks.
      throw TOutOfChunksError();
    }
  } else {
    // We're empty and can't grow.
    throw TOutOfChunksError();
  }
  try {
    if (chunk->MustFree) {
      // Send the chunk into the world ready to return to us.
      chunk->Pool = shared_from_this();
      result = shared_ptr<TChunk>(chunk, OnRelease);
    } else {
      // This chunk isn't one of ours, so let it self-destruct.
      result = shared_ptr<TChunk>(chunk);
    }
  } catch (...) {
    delete chunk;
    throw;
  }

  return result;
}

void TPool::EnqueueChunk(TChunk *chunk) {
  assert(this);
  assert(chunk);

  try {
    FreeChunks.push(chunk);
  } catch (...) {
    delete chunk;
    throw;
  }
}

void TPool::EnqueueNewChunks(size_t chunk_count) {
  assert(this);

  for (size_t i = 0; i < chunk_count; ++i) {
    EnqueueChunk(new TChunk(ChunkSize));
  }
}

void TPool::OnRelease(TChunk *chunk) {
  assert(chunk);
  // Take over the chunk's pointer to its pool.
  shared_ptr<TPool> pool;
  swap(pool, chunk->Pool);
  assert(pool);
  // Reset the chunk to its empty state.
  chunk->Cursor = chunk->Start;
  // Return the chunk to its pool.
  pool->EnqueueChunk(chunk);
}
