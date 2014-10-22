/* <base/tmp_dir.h>

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

   Generates a temporary directory.
 */

#pragma once

#include <cassert>
#include <vector>

#include <base/no_copy_semantics.h>

namespace Base {

  class TTmpDir final {
    NO_COPY_SEMANTICS(TTmpDir);

    public:
    TTmpDir(const char *name_template = "/tmp/bruce_tmp.XXXXXX",
            bool delete_on_destroy = false);

    ~TTmpDir();

    const char *GetName() const {
      assert(this);
      return &Name[0];
    }

    void SetDeleteOnDestroy(bool delete_on_destroy) {
      assert(this);
      DeleteOnDestroy = delete_on_destroy;
    }

    private:
    std::vector<char> Name;

    bool DeleteOnDestroy;
  };  // TTmpDir

}  // Base
