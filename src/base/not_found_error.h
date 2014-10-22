/* <base/not_found_error.h>

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

   Whatever you were looking for ain't there.
 */

#pragma once

#include <base/error.h>

namespace Base {

  /* TODO */
  class TNotFoundError : public Base::TFinalError<TNotFoundError> {
    public:

    /* Constructor */
    TNotFoundError(const TCodeLocation &code_location,
        const char *message = nullptr) {
      PostCtor(code_location, message);
    }
  };  // TNotFoundError

}  // Base
