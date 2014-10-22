/* <base/dynamic_lib.cc>

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

   Implements <base/dynamic_lib.h>.
 */

#include <base/dynamic_lib.h>

using namespace Base;

std::string TDynamicLib::TLibLoadError::CreateMsg(const char *filename) {
  assert(filename);
  std::string msg("Failed to load library [");
  msg += filename;
  msg += "]";
  return std::move(msg);
}

std::string TDynamicLib::TSymLoadError::CreateMsg(const char *filename,
    const char *symname) {
  assert(filename);
  assert(symname);
  std::string msg("Failed to load symbol [");
  msg += symname;
  msg += "] for library [";
  msg += filename;
  msg += "]";
  return std::move(msg);
}

TDynamicLib::TDynamicLib(const char *filename, int flags)
    : LibName(filename),
      Handle(dlopen(filename, flags)) {
  if (Handle == nullptr) {
    throw TLibLoadError(filename);
  }
}
