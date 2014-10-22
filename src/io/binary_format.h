/* <io/binary_format.h>

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

   Formatting options for binary I/O streams.
 */

#pragma once

#include <cassert>

#include <io/endian.h>

namespace Io {

  /* Formatting options for binary I/O streams. */
  class TBinaryFormat {
    public:
    /* By default, we use network byte ordering. */
    TBinaryFormat()
        : UseNbo(true) {
    }

    template <typename TVal>
    void ConvertInt(TVal &val) const {
      assert(this);
      assert(&val);

      if (UseNbo) {
        val = SwapEnds(val);
      }
    }

    /* If true, convert to network byte order when writing and convert back to
       host byte order when reading; otherwise, use host byte ordering for
       writing and reading. */
    bool UseNbo;

  };  // TBinaryFormat

}  // Io

