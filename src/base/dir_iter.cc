/* <base/dir_iter.cc>

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

   Implements <base/dir_iter.h>.
 */

#include <base/dir_iter.h>

#include <cstring>
#include <memory>
#include <stack>
#include <string>

#include <base/error_utils.h>

using namespace std;
using namespace Base;

TDirIter::TDirIter(const char *dir)
    : Handle(opendir(dir)), Pos(NotFresh) {
  if (!Handle) {
    ThrowSystemError(errno);
  }
}

TDirIter::~TDirIter() {
  assert(this);
  closedir(Handle);
}

void TDirIter::Rewind() {
  assert(this);
  rewinddir(Handle);
  Pos = NotFresh;
}

bool TDirIter::TryRefresh() const {
  assert(this);

  while (Pos == NotFresh) {
    dirent *ptr;
    IfLt0(readdir_r(Handle, &DirEnt, &ptr));

    if (ptr) {
      if (DirEnt.d_type != DT_DIR || (strcmp(DirEnt.d_name, "..") &&
          strcmp(DirEnt.d_name, "."))) {
        Pos = AtEntry;
      }
    } else {
      Pos = AtEnd;
    }
  }

  return (Pos == AtEntry);
}
