/* <base/basename.cc>

   ----------------------------------------------------------------------------
   Copyright 2014 if(we)

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

   Implements <base/basename.h>.
 */

#include <base/basename.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <vector>

#include <libgen.h>

using namespace Base;

std::string Base::Basename(const char *path) {
  assert(path);
  std::vector<char> name_buf(path, path + std::strlen(path) + 1);
  return basename(&name_buf[0]);
}
