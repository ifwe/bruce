/* <bruce/util/connect_to_host.cc>

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

   Implements <bruce/util/connect_to_host.h>.
 */

#include <algorithm>
#include <cassert>
#include <cerrno>

#include <base/error_utils.h>
#include <bruce/util/connect_to_host.h>
#include <socket/db/cursor.h>

using namespace Base;
using namespace Socket;

void Bruce::Util::ConnectToHost(const char *host_name, in_port_t port,
    TFd &result_socket) {
  assert(host_name);
  result_socket.Reset();

  /* Iterate over our potential hosts. */
  for (Db::TCursor csr(host_name, nullptr, AF_INET, SOCK_STREAM, 0,
                       AI_PASSIVE);
       csr;
       ++csr) {
    /* Get the address of the host we're going to try and set the port. */
    TAddress address = *csr;
    address.SetPort(port);

    /* Create a socket that's compatible with candidate host. */
    TFd sock = csr.NewCompatSocket();

    if (!connect(sock, address, address.GetLen())) {
      result_socket = std::move(sock);
      break;  // success
    }

    /* What went wrong? */
    switch (errno) {
      case ECONNREFUSED:
      case ETIMEDOUT:
      case EHOSTUNREACH:
      case EHOSTDOWN: {
        /* These errors aren't serious.  Move on to the next host. */
        break;
      }
      default: {
        /* Anything else is big-time serious. */
        ThrowSystemError(errno);
      }
    }
  }
}
