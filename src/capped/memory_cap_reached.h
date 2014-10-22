/* <capped/memory_cap_reached.h>

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

   Exception type thrown when operation can not be performed due to memory cap.
 */

#pragma once

#include <stdexcept>

namespace Capped {

  class TMemoryCapReached : public std::runtime_error {
    public:
    TMemoryCapReached()
        : std::runtime_error("Memory cap reached") {
    }
  };  // TMemoryCapReached

}  // Capped

