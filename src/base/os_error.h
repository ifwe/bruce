/* <base/os_error.h>

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

   Implements an exception for reporting operating system errors.
 */

#pragma once

#include <cerrno>
#include <cstring>

#include <base/error.h>

namespace Base {

  /* Throw an instance of this class to report an operating system error. */
  class TOsError : public TFinalError<TOsError> {
    public:
    /* If no number if explicitly given, the constructor will pick it up from
       the global 'errno' defined in <cerrno>. */
    TOsError(const TCodeLocation &code_location, int error_code = errno)
        : ErrorCode(error_code) {
      PostCtor(code_location, strerror(error_code));
    }

    int GetErrorCode() const {
      assert(this);
      return ErrorCode;
    }

    static void IfEq0(const TCodeLocation &code_location, int ret) {
      if (ret == 0) {
        throw TOsError(code_location, ret);
      }
    }

    /* Use this helper function when calling an OS function which returns < 0
       on failure, leaving an error code in errno, like this:

          TOsError::IfLt0(HERE, pipe(fds));

       The helper function will throw iff. the predicate fails. */
    static int IfLt0(const TCodeLocation &code_location, int ret) {
      if (ret < 0) {
        throw TOsError(code_location);
      }

      return ret;
    }

    /* Use this helper function when calling an OS function which returns an
       error code directly, like this:

          TOsError::IfNe0(HERE, pthread_setspecific(key, val));

       The helper function will throw iff. the function returns non-zero. */
    static void IfNe0(const TCodeLocation &code_location, int ret) {
      if (ret) {
        throw TOsError(code_location, ret);
      }
    }

    template <typename TVal>
    static TVal *IfNull(const TCodeLocation &code_location, TVal *ret) {
      if (!ret) {
        throw TOsError(code_location);
      }

      return ret;
    }

    private:
    int ErrorCode;
  };  // TOsError

}  // Base
