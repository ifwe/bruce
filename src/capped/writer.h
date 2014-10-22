/* <capped/writer.h>

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

   Builds blobs incrementally.
 */

#pragma once

#include <base/no_copy_semantics.h>
#include <capped/blob.h>

namespace Capped {

  /* Builds blobs incrementally.  Construct a writer by giving it a pool from
     which to draw blocks.  Then you call Write() zero or more times to copy
     data into the writer.  The writer will allocate blocks from the pool as
     required.  When you have finished writing, call DraftBlob() to return your
     data in blob form.  The writer is then ready to be used again.  If you
     wish to reset the writer without constructing a blob, call CancelBlob().
   */
  class TWriter final {
    NO_COPY_SEMANTICS(TWriter);

    public:
    /* We use the same blocks that TPool uses. */
    using TBlock = TPool::TBlock;

    /* A newly created writer has no data. */
    TWriter(TPool *pool) noexcept;

    /* Any pending blob will be canceled automatically. */
    ~TWriter() noexcept;

    /* Drop any data we've written so far and return to the default-constructed
       state. */
    void CancelBlob() noexcept;

    /* Return the blob we've built and reset to the default-constructed state.
     */
    TBlob DraftBlob() noexcept;

    /* Copy the given data to the end of the blob we're building. */
    TWriter &Write(const void *data, size_t size);

    private:

    /* Returns the writer to the empty state.  This simply nulls out
       FirstBlock, LastBlock, and Cursor without freeing anything, so make sure
       ownership of the data has been transferred to some other structure
       before you call this function. */
    void Init() noexcept;

    /* The pool which we allocate blocks. */
    TPool *Pool;

    /* The first and last blocks in our linked list.  These are both null iff.
       we're empty.  If we're non-empty but, both pointers will be non-null.
       If all our data fits in one chunk, both pointers will point to the same
       block.  If we use two or more chunks, then these pointers will point to
       different blocks. */
    TBlock *FirstBlock, *LastBlock;

    /* The position with in our last block's chunk where Write() will append
       more data.  This is null iff. LastBlock is null. */
    char *Cursor;

    /* The total # of bytes that have been written so far. */
    size_t NumBytes;

  };  // TWriter

}  // Capped
