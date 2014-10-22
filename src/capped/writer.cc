/* <capped/writer.cc>

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

   Implements <capped/writer.h>.
 */

#include <capped/writer.h>

#include <cstring>

using namespace std;
using namespace Capped;

TWriter::TWriter(TPool *pool) noexcept
   : Pool(pool) {
  assert(pool);
  Init();
}

TWriter::~TWriter() noexcept {
  assert(this);
  CancelBlob();
}

void TWriter::CancelBlob() noexcept {
  assert(this);
  Pool->FreeList(FirstBlock);
  Init();
}

TBlob TWriter::DraftBlob() noexcept {
  assert(this);
  TBlob result;

  if (FirstBlock) {
    result = TBlob(Pool, FirstBlock, Cursor - LastBlock->Data, NumBytes);
    Init();
  }

  return result;
}

TWriter &TWriter::Write(const void *data, size_t size) {
  assert(this);
  assert(data || !size);
  /* Determine if we need to allocate more blocks and, if so, where we'll link
     them on to our list. */
  size_t block_size = Pool->GetDataSize();
  TBlock **link;
  size_t bytes_to_write = size;

  if (LastBlock) {
    size_t avail = LastBlock->Data + block_size - Cursor;

    if (size <= avail) {
      /* The new data fits entirely in the existing space, so we won't need to
         link on any more blocks. */
      memcpy(Cursor, data, size);
      Cursor += size;
      NumBytes += size;
      return *this;
    }

    /* The new data doesn't entirely fit, but we'll make a start.  We will need
       to allocate more blocks, and we'll link them to the end of our existing
       list. */
    memcpy(Cursor, data, avail);
    reinterpret_cast<const char *&>(data) += avail;
    size -= avail;
    link = &(LastBlock->NextBlock);
  } else {
    /* We don't have any blocks yet, so we'll definitely need to allocate some.
       We'll link them on as the first blocks in our list. */
    link = &FirstBlock;
  }

  assert(link);

  /* Allocate enough blocks to hold the remainder of the new data and link them
     on. */
  TBlock *block = Pool->AllocList((size + block_size - 1) / block_size);
  *link = block;

  /* Copy as many whole blocks as we can. */
  while (size > block_size) {
    assert(block);
    memcpy(block->Data, data, block_size);
    reinterpret_cast<const char *&>(data) += block_size;
    size -= block_size;
    block = block->NextBlock;
  }

  /* Copy in the last of the new data (a whole or partial block) and get ready
     for the next write. */
  memcpy(block->Data, data, size);
  LastBlock = block;
  Cursor = block->Data + size;
  NumBytes += bytes_to_write;
  return *this;
}

void TWriter::Init() noexcept {
  assert(this);
  FirstBlock = nullptr;
  LastBlock = nullptr;
  Cursor = nullptr;
  NumBytes = 0;
}
