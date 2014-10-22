/* <bruce/mock_kafka_server/cmd_handler.cc>

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

   Implements <bruce/mock_kafka_server/cmd_handler.h>.
 */

#include <bruce/mock_kafka_server/cmd_handler.h>

#include <algorithm>
#include <cassert>
#include <memory>

#include <base/debug_log.h>
#include <bruce/mock_kafka_server/cmd_worker.h>
#include <socket/address.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MockKafkaServer;
using namespace Socket;

void TCmdHandler::OnEvent(int fd, short /*flags*/) {
  assert(this);
  TAddress client_address;
  TFd client_socket(IfLt0(Accept(fd, client_address)));
  RunThread(new TCmdWorker(Ss, std::move(client_socket)));
}
