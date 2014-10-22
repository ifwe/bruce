/* <io/input_producer.h>

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

   A producer of in-bound data.
 */

#pragma once

#include <memory>

#include <base/no_copy_semantics.h>
#include <io/chunk_and_pool.h>

namespace Io {

  /* A producer of in-bound data. */
  class TInputProducer {
    NO_COPY_SEMANTICS(TInputProducer);

    public:
    /* Produce the next chunk of data.  If there is no more data, return null.
     */
    virtual std::shared_ptr<const TChunk> TryProduceInput() = 0;

    protected:
    /* Do-little. */
    TInputProducer() {
    }

    /* Do-little. */
    virtual ~TInputProducer();
  };  // TInputProducer

}  // Io

