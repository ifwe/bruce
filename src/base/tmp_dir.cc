/* <base/tmp_dir.cc>

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

   Implements <base/tmp_dir.h>.
 */

#include <base/tmp_dir.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>

#include <unistd.h>

#include <base/error_utils.h>

using namespace Base;

TTmpDir::TTmpDir(const char *name_template, bool delete_on_destroy)
    : Name(1 + std::strlen(name_template)),
      DeleteOnDestroy(delete_on_destroy) {
  std::strncpy(&Name[0], name_template, Name.size());

  if (mkdtemp(&Name[0]) == nullptr) {
    ThrowSystemError(errno);
  }
}

TTmpDir::~TTmpDir() {
  if (DeleteOnDestroy) {
    std::string cmd("/bin/rm -fr ");
    cmd += &Name[0];
    std::system(cmd.c_str());
  }
}
