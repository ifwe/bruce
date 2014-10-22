/* <base/no_default_case.h>

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

   Implements a macro for handling switches without defaults.
 */

#pragma once

#include <base/error.h>

/* Use this macro to close out any switch which doesn't have a default case,
   like this:

      const char *GetText(int val) {
        const char *text;
        switch (val) {
          case 1: { text = "1"; break; }
          case 2: { text = "2"; break; }
          case 3: { text = "3"; break; }
          NO_DEFAULT_CASE;
        }
        return text;
      }

   If control reaches the macro, it will abort the program.  It is therefore
   safe to use the switch to conditionally initialize an otherwise unitialized
   variable, as shown. */
#define NO_DEFAULT_CASE default: ::Base::TError::Abort(HERE)
