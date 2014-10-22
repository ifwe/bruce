/* <bruce/util/exceptions.h>

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

   Exception classes.
 */

#pragma once

#include <stdexcept>

namespace Bruce {

  namespace Util {

    class TFileOpenError : public std::runtime_error {
      public:
      explicit TFileOpenError(const char *filename);

      explicit TFileOpenError(const std::string &filename)
          : TFileOpenError(filename.c_str()) {
      }

      virtual ~TFileOpenError() noexcept { }
    };  // TFileOpenError

    class TFileReadError : public std::runtime_error {
      public:
      explicit TFileReadError(const char *filename);

      explicit TFileReadError(const std::string &filename)
          : TFileReadError(filename.c_str()) {
      }

      virtual ~TFileReadError() noexcept { }
    };  // TFileReadError

    class TFileWriteError : public std::runtime_error {
      public:
      explicit TFileWriteError(const char *filename);

      explicit TFileWriteError(const std::string &filename)
          : TFileWriteError(filename.c_str()) {
      }

      virtual ~TFileWriteError() noexcept { }
    };  // TFileWriteError

  }  // Util

}  // Bruce
