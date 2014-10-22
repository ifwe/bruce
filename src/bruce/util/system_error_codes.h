/* <bruce/util/system_error_codes.h>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 if(we)

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

   Functions for interpreting system error codes.
 */

#pragma once

#include <cerrno>
#include <system_error>

namespace Bruce {

  namespace Util {

    static inline bool LostTcpConnection(int errno_value) {
      return (errno_value == ECONNRESET) || (errno_value == EPIPE) ||
             (errno_value == EIO) || (errno_value == EHOSTUNREACH) ||
             (errno_value == EHOSTDOWN);
    }

    static inline bool LostTcpConnection(const std::system_error &x) {
      return LostTcpConnection(x.code().value());
    }

    static inline bool TimedOut(int errno_value) {
      return (errno_value == ETIMEDOUT);
    }

    static inline bool TimedOut(const std::system_error &x) {
      return TimedOut(x.code().value());
    }

  }  // Util

}  // Bruce
