/* <io/endian.h>

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

   Swapping the big and little ends of integers.
 */

#pragma once

#include <cstdint>

namespace Io {

  /* The 8-bit version, to facilitate templates which can take any of the
     uint[n]_t types. */
  inline uint8_t SwapEnds(uint8_t val) {
    return val;
  }

  /* And the signed version. */
  inline int8_t SwapEnds(int8_t val) {
    return val;
  }

  /* The 16-bit version. */
  inline uint16_t SwapEnds(uint16_t val) {
    return __builtin_bswap16(val);
  }

  /* And the signed version. */
  inline int16_t SwapEnds(int16_t val) {
    return SwapEnds(static_cast<uint16_t>(val));
  }

  /* The 32-bit version. */
  inline uint32_t SwapEnds(uint32_t val) {
    return __builtin_bswap32(val);
  }

  /* And the signed version. */
  inline int32_t SwapEnds(int32_t val) {
    return SwapEnds(static_cast<uint32_t>(val));
  }

  /* The 64-bit version. */
  inline uint64_t SwapEnds(uint64_t val) {
    return __builtin_bswap64(val);
  }

  /* And the signed version. */
  inline int64_t SwapEnds(int64_t val) {
    return SwapEnds(static_cast<uint64_t>(val));
  }

}  // Io

