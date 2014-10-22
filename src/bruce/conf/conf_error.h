/* <bruce/conf/conf_error.h>

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

   Base class for exceptions thrown by config file processing code.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <string>

namespace Bruce {

  namespace Conf {

    class TConfError : public std::runtime_error {
      public:
      virtual const char *what() const noexcept {
        assert(this);
        return Msg.c_str();
      }

      protected:
      explicit TConfError(std::string &&msg)
          : std::runtime_error(msg.c_str()),
            Msg(std::move(msg)) {
      }

      private:
      std::string Msg;
    };  // TConfError

  }  // Conf

}  // Bruce
