/* <base/demangle.cc>

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

   Implements <base/demangle.h>.
 */

#include <base/demangle.h>
#include <base/error.h>

#include <cxxabi.h> // To demangle the true name of the exception type.

using namespace Base;

//NOTE: This can throw a TDemangleError
TDemangle::TDemangle(const std::type_info &t) : Buf(0) {
  DoDemangle(t.name());
}

TDemangle::TDemangle(const char *mangled) : Buf(0) {
  DoDemangle(mangled);
}

TDemangle::~TDemangle() {
  assert(this);
  assert(Buf);
  free(Buf); //The gcc man pages use free, so we use free.
}

const char *TDemangle::Get() const {
  assert(this);
  assert(Buf);
  return Buf;
}

void TDemangle::DoDemangle(const char *str) {
  int st = 0;
  Buf = abi::__cxa_demangle(str, nullptr, nullptr, &st);

  if (st != 0) {
    throw TDemangleError(HERE, st);
  }
}
