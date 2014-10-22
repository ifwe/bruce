/* <io/input_consumer.h>

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

   A consumer of in-bound data, such as a reader or adapter.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <vector>

#include <base/no_copy_semantics.h>
#include <io/input_producer.h>

namespace Io {

  /* Forward declaration, because TInputConsumer refers to TMark. */
  class TMark;

  /* A consumer of in-bound data, such as a reader or adapter. */
  class TInputConsumer {
    NO_COPY_SEMANTICS(TInputConsumer);

    public:
    /* Thrown when an operation requires more data, but there is no more in the
       input stream. */
    class TPastEndError : public std::runtime_error {
      public:
      /* Do-little. */
      TPastEndError()
          : std::runtime_error("past end of input") {
      }
    };  // TPastEndError

    /* Thrown when we get input we don't know what to do with. */
    class TSyntaxError : public std::runtime_error {
      public:
      /* Do-little. */
      TSyntaxError()
          : std::runtime_error("syntax error") {
      }
    };  // TSyntaxError

    /* The number of chunks currently cached by this consumer.  This
       information is of limited real value, but is useful for debugging. */
    size_t GetCachedChunkCount() const {
      assert(this);
      return Chunks.size();
    }

    /* The producer of the input which we consume.  Never null. */
    const std::shared_ptr<TInputProducer> &GetInputProducer() const {
      assert(this);
      return InputProducer;
    }

    bool HasBufferedData() const;

    /* True iff. we have reached the end of the input stream. */
    bool IsAtEnd() {
      assert(this);
      TryRefresh();
      return ChunkIdx >= Chunks.size() && NoMoreChunks;
    }

    /* Peek at the pending data and copy it to a string.
       The stream will remain at its current position.
       This is useful for debugging a stream with syntax errors in it. */
    void PeekAndDump(std::string &out);

    protected:
    /* The number of bytes currently available to Peek() and TryPeek().
       This will always be > 0 unless we have reached the end of the input
       stream. */
    size_t GetPeekSize() {
      assert(this);
      TryRefresh();
      return Limit - Cursor;
    }

    /* The data at our current position.  Never null.
       Use GetPeekSize() to find out how many bytes here are valid.
       If we have reached the end of the input stream, throw TPastEndError. */
    const char *Peek() {
      assert(this);
      Refresh();
      return Cursor;
    }

    /* The byte at our current position.
       If we have reached the end of the input stream, throw TPastEndError. */
    char PeekChar() {
      assert(this);
      return *Peek();
    }

    /* Copy the requested number of bytes from our current position into the
       given buffer and advance the consumer the same number of bytes. */
    void ReadExactly(void *buf, size_t size);

    /* Advance the consumer the requested number of bytes. */
    void SkipExactly(size_t size);

    /* The data at our current position.  If we have not reached the end of the
       input stream, this will always point to at least one byte of data.  Use
       GetPeekSize() to find out how many bytes here are valid.  If we have
       reached the end of the input stream, this will be null. */
    const char *TryPeek() {
      assert(this);
      TryRefresh();
      return Cursor;
    }

    protected:
    /* Attach to the given producer, which must not be null.  We start with no
       chunks and no marks.  We will not ask our producer for a chunk until
       someone tries to read from us. */
    TInputConsumer(const std::shared_ptr<TInputProducer> &input_producer)
        : InputProducer(input_producer),
          ChunkIdx(0),
          Cursor(0),
          Limit(0),
          NoMoreChunks(false),
          NewestMark(0) {
      assert(input_producer);
    }

    /* Releases any remaining chunks.
       It is an error to try to destroy a consumer which still has marks. */
    virtual ~TInputConsumer();

    private:
    /* Refresh our cursor, then return the size given or the available number
       of bytes at the cursor, whichever is lesser. */
    size_t GetStepSize(size_t size) {
      assert(this);
      Refresh();
      size_t step_size = Limit - Cursor;
      return std::min(size, step_size);
    }

    /* Make sure we have at least one byte of usable data at our cursor.
       If we don't have one and can't get one, throw TPastEndError. */
    void Refresh() {
      assert(this);
      TryRefresh();

      if (ChunkIdx >= Chunks.size()) {
        throw TPastEndError();
      }
    }

    /* Try to make sure we have at least one byte of usable data at our cursor.
     */
    void TryRefresh();

    /* See accessor. */
    std::shared_ptr<TInputProducer> InputProducer;

    /* The chunks which make up our window into the input stream.  This will
       stretch from the first chunk with a mark in it up to the latest chunk we
       have consumed.  If there are no marks, this will contain at most a
       single chunk, which is our current chunk. */
    std::vector<std::shared_ptr<const TChunk>> Chunks;

    /* An index into Chunks, indicating where in the input stream we are
       currently positioned.  If this is >= Chunks.size(), then we have no
       current chunk. */
    size_t ChunkIdx;

    /* If we have a current chunk, then Cursor points into the data referenced
       by that chunk and Limit points to the end of that data.  If we don't
       have a current chunk, these are null. */
    const char *Cursor, *Limit;

    /* True iff. our producer has reported that there are no more chunks
       available to be consumed. */
    bool NoMoreChunks;

    /* The top of a stack of marks.  If this is null, we have no marks. */
    TMark *NewestMark;

    /* TMark needs to help maintain our stack of marks for us, and to update
       ChunkIdx, Cursor, and Limit during a rewind. */
    friend class TMark;
  };  // TInputConsumer

  /* Create a local instance of this class to mark a position in the input
     stream.  You can then Rewind() to this mark if you need to backtrack. */
  class TMark {
    NO_COPY_SEMANTICS(TMark);

    public:
    /* Mark the current position.  Once a mark is created, the consumer will
       begin to save its chunks.  The consumer will not destroy its chunks
       until the earliest mark is destroyed.  Attempting to mark the end of the
       input stream will throw TPastEndError. */
    TMark(TInputConsumer *input_consumer);

    /* If this is the only remaining mark, then the held chunks will be
       destroyed as well.  It is not legal to destroy a mark other than the
       most recent one. */
    ~TMark();

    /* Rewind to the point at which this mark created.  It is not legal to
       rewind to a mark other than the most recent one. */
    void Rewind() const;

    private:
    /* The consumer we mark.  Never null. */
    TInputConsumer *InputConsumer;

    /* The next mark, if any, in our consumer's stack of marks. */
    TMark *OlderMark;

    /* A copy of our consumer's ChunkIdx, as of the time we constructed. */
    size_t ChunkIdx;

    /* Copies of our consumer's Cursor and Limit pointers, as of the time we
       constructed. */
    const char *Cursor, *Limit;
  };  // TMark

}  // Io
