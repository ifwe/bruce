/* <base/error.cc>

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

   Implements <base/error.h>.
 */

#include <base/error.h>

#include <cstdlib>
#include <iostream>
#include <sstream>

#include <base/no_default_case.h>

using namespace std;
using namespace Base;

void TError::Abort(const TCodeLocation &code_location) {
  cerr << "aborting at " << code_location << endl;
  abort();
}

TError::TError() : WhatPtr("PostCtor() was not called") {}

void TError::PostCtor(const TCodeLocation &code_location,
    const char *details) {
  assert(this);
  assert(&code_location);

  try {
    CodeLocation = code_location;
    stringstream out_strm;
    out_strm << CodeLocation << ", ";

    try {
      TDemangle demangle(GetTypeInfo());
      out_strm<<demangle.Get();
    } catch (const TDemangleError &ex) {
      out_strm<<GetTypeInfo().name();
    }

    if (details) {
      out_strm << ", " << details;
    }

    WhatBuffer = out_strm.str();
    WhatPtr = WhatBuffer.c_str();
  } catch (...) {
    Abort(HERE);
  }
}


void TError::PostCtor(const TCodeLocation &code_location,
    const char *details_start, const char* details_end) {
  assert(this);
  assert(&code_location);

  try {
    CodeLocation = code_location;
    stringstream out_strm;
    out_strm << CodeLocation << ", ";

    try {
      TDemangle demangle(GetTypeInfo());
      out_strm<<demangle.Get();
    } catch (const TDemangleError &ex) {
      out_strm<<GetTypeInfo().name();
    }

    if (details_start) {
      out_strm << ", ";
      out_strm.write(details_start, details_end - details_start);
    }

    WhatBuffer = out_strm.str();
    WhatPtr = WhatBuffer.c_str();
  } catch (...) {
    Abort(HERE);
  }
}

TDemangleError::TDemangleError(const TCodeLocation &code_location, int ret) {
  const char *details = 0;

  switch (ret) {
    case 0:
      details = "The demangling operation succeeded";
      break;
    case -1:
      details = "A memory allocation failure occured";
      break;
    case -2:
      details =
          "mangled name is not a valid name under the C++ ABI name mangling "
          "rules";
      break;
    case -3:
      details = "One of the arguments is invalid";
      break;
    NO_DEFAULT_CASE;
  }

  assert(details);
  PostCtor(code_location, details);
}
