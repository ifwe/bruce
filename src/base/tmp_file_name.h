/* <base/tmp_file_name.h>

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

   Generates a temporary filename (just a name, not an actual file).
 */

#pragma once

#include <cassert>
#include <stdexcept>
#include <vector>

#include <base/thrower.h>

namespace Base {

  class TTmpFileName final {
    public:
    DEFINE_ERROR(TCannotCreateName, std::runtime_error,
                 "Cannot create temporary filename");

    TTmpFileName(const char *name_template = "/tmp/bruce_tmp.XXXXXX");

    operator const char *() const {
      return &Name[0];
    }

    private:
    std::vector<char> Name;
  };  // TTmpFileName

}  // Base
