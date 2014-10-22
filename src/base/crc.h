/* <base/crc.h>

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

   Function for computing 32-bit CRC.
 */

#pragma once

#include <cstddef>

#include <boost/crc.hpp>

namespace Base {

  static inline uint32_t ComputeCrc32(const void *data, size_t data_size) {
    boost::crc_32_type result;
    result.process_bytes(data, data_size);
    return result.checksum();
  }

}  // Base
