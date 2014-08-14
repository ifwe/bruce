/* <bruce/test_util/unix_dg_socket_writer.cc>

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

   Implements <bruce/test_util/unix_dg_socket_writer.h>.
 */

#include <bruce/test_util/unix_dg_socket_writer.h>

#include <sys/types.h>
#include <sys/socket.h>

#include <base/tmp_file_name.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::TestUtil;
using namespace Socket;

TUnixDgSocketWriter::TUnixDgSocketWriter(const char *socket_path)
    : SocketPath(socket_path),
      Sock(SOCK_DGRAM, 0) {
  assert(this);
  TAddress client_address;
  client_address.SetFamily(AF_LOCAL);
  TTmpFileName tmp_filename;
  client_address.SetPath(tmp_filename);
  ServerAddress.SetFamily(AF_LOCAL);
  ServerAddress.SetPath(SocketPath.c_str());
  Bind(Sock, client_address);
}
