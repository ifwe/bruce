/* <bruce/util/host_and_port.h>

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

   Struct representing host and port.
 */

#pragma once

#include <algorithm>
#include <string>

#include <netinet/in.h>

namespace Bruce {

  namespace Util {

    struct THostAndPort {
      std::string Host;

      in_port_t Port;

      THostAndPort(const std::string &host, in_port_t port)
          : Host(host),
            Port(port) {
      }

      THostAndPort(std::string &&host, in_port_t port)
          : Host(std::move(host)),
            Port(port) {
      }

      THostAndPort(const THostAndPort &) = default;

      THostAndPort(THostAndPort &&) = default;

      THostAndPort &operator=(const THostAndPort &) = default;

      THostAndPort &operator=(THostAndPort &&) = default;
    };  // THostAndPort

  }  // Util

}  // Bruce
