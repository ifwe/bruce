/* <io/output_producer.h>

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

   A producer of out-bound data.
 */

#pragma once

#include <cassert>
#include <memory>

#include <base/no_copy_semantics.h>
#include <io/chunk_and_pool.h>
#include <io/output_consumer.h>

namespace Io {

  /* A producer of out-bound data. */
  class TOutputProducer {
    NO_COPY_SEMANTICS(TOutputProducer);
    public:

    /* The pool from which we acquire chunks.  Never null. */
    const std::shared_ptr<TPool> &GetPool() const {
      assert(this);
      return Pool;
    }

    /* The consumer of the output we produce.  Null if we're not pushing. */
    const std::shared_ptr<TOutputConsumer> &GetOutputConsumer() const {
      assert(this);
      return OutputConsumer;
    }

    protected:
    /* Attach to the given consumer.  If the consumer is null, then we won't
       push our output anywhere.  Use the given pool, which must not be null.
     */
    TOutputProducer(const std::shared_ptr<TOutputConsumer> &output_consumer,
        const std::shared_ptr<TPool> &pool)
        : OutputConsumer(output_consumer), Pool(pool) {
      assert(pool);
    }

    /* Flushes automatically before destruction. */
    virtual ~TOutputProducer();

    /* Push our current chunk to our consumer.  If we have no current chunk, do
       nothing. */
    void Flush();

    /* Write the contents of the given buffer to our current chunk.  If there
       is too much data for our chunk, flush to our consumer and begin a new
       chunk. */
    void WriteExactly(const void *buf, size_t size);

    private:
    /* See accessor. */
    std::shared_ptr<TOutputConsumer> OutputConsumer;

    /* See accessor. */
    std::shared_ptr<TPool> Pool;

    /* The chunk we are current filling, if any. */
    std::shared_ptr<TChunk> CurrentChunk;
  };  // TOutputProducer

}  // Io
