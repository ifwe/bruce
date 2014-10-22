/* <base/tmp_file.cc>

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

   Implements <base/tmp_file.h>.
 */

#include <base/tmp_file.h>

#include <cstdlib>
#include <cstring>

#include <unistd.h>

#include <base/error_utils.h>

using namespace Base;

TTmpFile::TTmpFile(const char *name_template, bool delete_on_destroy)
    : Name(name_template, name_template + std::strlen(name_template) + 1),
      DeleteOnDestroy(delete_on_destroy) {
  size_t len = std::strlen(name_template);

  /* Number of consecutive 'X' chars found. */
  size_t x_count = 0;

  size_t i = len;

  /* Search 'name_template' in reverse for "XXXXXX".  If found, x_count will be
     6 and i will be the start index of the found substring on loop
     termination.  Otherwise, x_count will be left with some value < 6. */
  while (i) {
    --i;

    if (name_template[i] == 'X') {
      if (++x_count == 6) {
        break;
      }
    } else {
      x_count = 0;
    }
  }

  /* Verify that we found "XXXXXX". */
  assert(x_count == 6);

  assert(i <= len);
  assert((len - i) >= 6);

  /* Compute # of remaining chars after "XXXXXX". */
  size_t suffix_len = len - i - 6;

  Fd = IfLt0(mkstemps(&Name[0], suffix_len));
}

TTmpFile::~TTmpFile() {
  if (DeleteOnDestroy) {
    unlink(&Name[0]);
  }
}
