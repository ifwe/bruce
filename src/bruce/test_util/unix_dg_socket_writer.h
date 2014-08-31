/* <bruce/test_util/unix_dg_socket_writer.h>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 Tagged

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

   Class for writing messages to a UNIX domain datagram socket.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>

#include <base/no_copy_semantics.h>
#include <socket/address.h>
#include <socket/named_unix_socket.h>

namespace Bruce {

  namespace TestUtil {

    /* Class for Writing messages to a UNIX domain datagram socket. */
    class TUnixDgSocketWriter final {
      NO_COPY_SEMANTICS(TUnixDgSocketWriter);

      public:
      explicit TUnixDgSocketWriter(const char *socket_path);

      /* Write 'msg' to the socket, where 'msg' is a C-style string. */
      void WriteMsg(const char *msg) const {
        assert(this);
        WriteMsg(msg, std::strlen(msg));
      }

      /* Write 'msg' to the socket. */
      void WriteMsg(const std::string &msg) const {
        assert(this);
        WriteMsg(msg.c_str(), msg.size());
      }

      /* Write message to socket, where 'msg' points to the first byte of the
         message, and 'msg_size' gives the message size in bytes. */
      void WriteMsg(const void *msg, size_t msg_size) const {
        assert(this);
        Socket::SendTo(Sock.GetFd(), msg, msg_size, 0, ServerAddress);
      }

      private:
      const std::string SocketPath;

      Socket::TNamedUnixSocket Sock;

      Socket::TAddress ServerAddress;
    };  // TUnixDgSocketWriter

  }  // TestUtil

}  // Bruce
