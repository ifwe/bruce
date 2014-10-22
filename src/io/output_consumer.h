/* <io/output_consumer.h>

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

   A consumer of out-bound data.
 */

#pragma once

#include <memory>

#include <base/no_copy_semantics.h>
#include <io/chunk_and_pool.h>

namespace Io {

  /* A consumer of out-bound data. */
  class TOutputConsumer {
    NO_COPY_SEMANTICS(TOutputConsumer);

    public:
    /* Consume the next chunk of data. */
    virtual void ConsumeOutput(const std::shared_ptr<const TChunk> &chunk) = 0;

    protected:
    /* Do-little. */
    TOutputConsumer() {}

    /* Do-little. */
    virtual ~TOutputConsumer();
  };  // TOutputConsumer

}  // Io

