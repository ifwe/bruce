/* <io/input_consumer.cc>

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

   Implements <io/input_consumer.h>
 */

#include <io/input_consumer.h>

#include <cstring>
#include <iomanip>
#include <sstream>

using namespace std;
using namespace Io;

bool TInputConsumer::HasBufferedData() const {
  assert(this);
  return Cursor < Limit; // || ChunkIdx < Chunks.size();
}

void TInputConsumer::PeekAndDump(string &out) {
  assert(this);
  assert(&out);
  TryRefresh();
  size_t size = Limit - Cursor;
  ostringstream strm;
  strm << "size = " << size;

  if (size) {
    strm << ", bytes = <";

    for (const char *csr = Cursor; csr < Limit; ++csr) {
      strm << setfill('0') << setw(2) << hex << static_cast<unsigned>(*csr);
    }

    strm << '>';
  }

  out = strm.str();
}

void TInputConsumer::ReadExactly(void *buf, size_t size) {
  assert(this);
  assert(buf || !size);
  char *csr = static_cast<char *>(buf);

  while (size) {
    size_t step_size = GetStepSize(size);
    memcpy(csr, Cursor, step_size);
    csr += step_size;
    Cursor += step_size;
    size -= step_size;
  }
}

void TInputConsumer::SkipExactly(size_t size) {
  assert(this);

  while (size) {
    size_t step_size = GetStepSize(size);
    Cursor += step_size;
    size -= step_size;
  }
}

TInputConsumer::~TInputConsumer() {
  assert(this);
  assert(!NewestMark);
}

void TInputConsumer::TryRefresh() {
  assert(this);

  /* If we're out of data, try to get more; otherwise, do nothing. */
  if (Cursor >= Limit) {
    /* If we have a current chunk, we're now done with it.  If we're collecting
       chunks (because of one or more marks), then move our index forward;
       otherwise, release the current chunk and keep our index where it is. */
    if (ChunkIdx < Chunks.size()) {
      if (NewestMark) {
        ++ChunkIdx;
      } else {
        Chunks.erase(Chunks.begin());
      }
    }

    /* Get our new current chunk.  If we've not yet reached the end of our
       cache (because we rewound and are moving forward again), just take the
       next cached chunk; otherwise, try to get another chunk from our
       producer. */
    shared_ptr<const TChunk> chunk;

    if (ChunkIdx < Chunks.size()) {
      chunk = Chunks[ChunkIdx];
    } else if (!NoMoreChunks) {
      chunk = InputProducer->TryProduceInput();

      if (chunk) {
        Chunks.push_back(chunk);
      } else {
        NoMoreChunks = true;
      }
    }

    /* If we have a current chunk, start pointing at its data;
       otherwise, zero out our data pointers. */
    if (chunk) {
      chunk->GetData(Cursor, Limit);
      assert(Cursor < Limit);
    } else {
      Cursor = 0;
      Limit = 0;
    }
  }
}

TMark::TMark(TInputConsumer *input_consumer) {
  assert(input_consumer);
  /* Make sure our consumer is at a valid position, then cache that position.
   */
  input_consumer->Refresh();
  ChunkIdx = input_consumer->ChunkIdx;
  Cursor = input_consumer->Cursor;
  Limit = input_consumer->Limit;
  /* Push ourself onto our consumer's stack of marks. */
  InputConsumer = input_consumer;
  OlderMark = input_consumer->NewestMark;
  input_consumer->NewestMark = this;
}

TMark::~TMark() {
  assert(this);
  /* Marks must be destroyed in the opposite order in which they were created.
     If this assertion fails, it means we're being destroyed out of order. */
  assert(InputConsumer->NewestMark == this);
  /* Pop ourself from our consumer's stack of marks. */
  InputConsumer->NewestMark = OlderMark;

  if (!OlderMark) {
    /* We were the last mark, so it's time to clean up the collection of
       chunks.  Release all the chunks before the consumer's current chunk,
       making the current chunk the first in the container.  If it has no
       current chunk, we'll release all the chunks. */
    auto iter = InputConsumer->Chunks.begin();
    InputConsumer->Chunks.erase(iter, iter + InputConsumer->ChunkIdx);
    InputConsumer->ChunkIdx = 0;
  }
}

void TMark::Rewind() const {
  assert(this);
  /* You cannot rewind past the newest mark.
     If this assertion fails, it means we're rewinding out of order. */
  assert(InputConsumer->NewestMark == this);
  /* Copy our cached position information back to the consumer. */
  InputConsumer->ChunkIdx = ChunkIdx;
  InputConsumer->Cursor = Cursor;
  InputConsumer->Limit = Limit;
}
