/* <base/demangle.h>

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

   Container to make it safe to demangle C++ names.
 */

#pragma once

#include <typeinfo>

#include <base/no_copy_semantics.h>

namespace Base {

  class TDemangle {
    NO_COPY_SEMANTICS(TDemangle);

    public:
    //NOTE: This can throw a TDemangleError, defined in <base/error.h>
    TDemangle(const std::type_info &t);

    //NOTE: This can throw a TDemangleError, defined in <base/error.h>
    TDemangle(const char *mangled_str);

    ~TDemangle();

    const char *Get() const;

    private:
    void DoDemangle(const char *str);

    char *Buf;
  };

}  // Base
