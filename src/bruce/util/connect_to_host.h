/* <bruce/util/connect_to_host.h>

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

   Functions for opening a TCP connection to a host.
 */

#pragma once

#include <string>

#include <netinet/in.h>

#include <base/fd.h>

namespace Bruce {

  namespace Util {

    /* Attempt to open a TCP connection to the given hostname and port.  On
       return, if result_socket.IsOpen() returns true then the connection
       attempt was successful.  Otherwise, the connection attempt failed. */
    void ConnectToHost(const char *host_name, in_port_t port,
        Base::TFd &result_socket);

    inline void ConnectToHost(const std::string &host_name, in_port_t port,
        Base::TFd &result_socket) {
      ConnectToHost(host_name.c_str(), port, result_socket);
    }

  }  // Util

}  // namespace Bruce
