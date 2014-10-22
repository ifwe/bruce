/* <capped/reader.cc>

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

   Implements <capped/reader.h>.
 */

#include <capped/reader.h>

#include <cstring>

#include <base/error_utils.h>

using namespace std;
using namespace Base;
using namespace Capped;

TReader &TReader::Advance(void *data, size_t size) {
  assert(this);

  /* Getting the block size will fail in the case where the blob is empty, so
     we first protect ourselves. */

  if (size == 0) {
    return *this;
  }

  if (Blob->Size() == 0) {
    /* If we're out of data, throw. */
    throw TMemoryCapReached();
  }

  /* Now it's safe to get the block size. */
  size_t block_size = Blob->GetBlockSize();

  while (size) {
    /* If we're out of data, throw. */
    if (!Cursor) {
      throw TMemoryCapReached();
    }

    /* See how much data is available in the current block.  If it's more than
       enough to satisfy the read, we'll just finish up and exit the loop. */
    size_t avail = Block ?
        (Block->Data + (Block->NextBlock ?
             block_size : Blob->LastBlockSize) - Cursor) :
        0;
    assert(BytesRemaining >= avail);

    if (size < avail) {
      if (data) {
        memcpy(data, Cursor, size);
      }

      Cursor += size;
      BytesRemaining -= size;
      break;
    }

    /* Copy the available data from this block and reduce the remaining read.
       This might reduce the remaining read to zero. */

    if (data) {
      memcpy(data, Cursor, avail);
      reinterpret_cast<char *&>(data) += avail;
    }

    size -= avail;

    /* Try to move to the next block and, if successful, position the cursor at
       the start of the new block's block. */
    Block = Block->NextBlock;
    Cursor = Block ? Block->Data : nullptr;
    BytesRemaining -= avail;
  }

  return *this;
}
