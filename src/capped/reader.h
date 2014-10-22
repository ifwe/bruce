/* <capped/reader.h>

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

   Provides sequential access to the data in a blob.
 */

#pragma once

#include <base/no_copy_semantics.h>
#include <capped/blob.h>
#include <capped/memory_cap_reached.h>

namespace Capped {

  /* Provides sequential access to the data in a blob.  This object provides a
     single forward pass over the blob.  A reader doesn't allow rewinding;
     however, readers can be cheaply copied.  Feel free to do so if you need to
     read ahead speculatively while keeping your finger on a particular
     position within a blob. */
  class TReader final {
    public:
    /* We use the same blocks that TPool uses. */
    using TBlock = TPool::TBlock;

    /* Construct a reader to read the data from the given blob.  The reader
       does NOT make its own copy of the blob's data, so you must make sure
       that the blob continues to exist as long as it has readers. */
    explicit TReader(const TBlob *blob) noexcept {
      assert(blob);
      Blob = blob;
      Block = blob->FirstBlock;
      Cursor = Block ? Block->Data : nullptr;
      BytesRemaining = blob->Size();
    }

    /* Shallow copy is ok. */
    TReader(const TReader &that) = default;

    /* Shallow copy is ok. */
    TReader &operator=(const TReader &) = default;

    /* True iff. we have not yet reached the end of the blob. */
    operator bool() const noexcept {
      assert(this);
      assert((Cursor == nullptr) == (BytesRemaining == 0));
      return Cursor;
    }

    /* Copy data from the blob and advance our position.  If you attempt to
       read past the end of the blob, this function throws TMemoryCapReached.
       Note that a read of zero bytes is always safe. */
    TReader &Read(void *data, size_t size) {
      assert(this);
      assert(data || (size == 0));
      return Advance(data, size);
    }

    /* Same as Read(), except data is skipped over rather than copied to
       caller-supplied buffer. */
    TReader &Skip(size_t size) {
      assert(this);
      return Advance(nullptr, size);
    }

    size_t GetBytesRemaining() const {
      assert(this);
      return BytesRemaining;
    }

    size_t GetBytesConsumed() const {
      assert(this);
      size_t total = Blob->Size();
      assert(BytesRemaining <= total);
      return total - BytesRemaining;
    }

    private:
    TReader &Advance(void *data, size_t size);

    /* The blob we are reading. */
    const TBlob *Blob;

    /* The block from which we are currently reading.  Null iff. we have
       reached the end. */
    TBlock *Block;

    /* Our position within our current block.  Null iff. we have reached the
       end. */
    char *Cursor;

    /* The total number of bytes remaining to be consumed. */
    size_t BytesRemaining;
  };  // TReader

}  // Capped
