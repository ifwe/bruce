/* <bruce/mock_kafka_server/v0_client_handler_factory.cc>

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

   Implements <bruce/mock_kafka_server/v0_client_handler_factory.h>.
 */

#include <bruce/mock_kafka_server/v0_client_handler_factory.h>

#include <algorithm>
#include <cassert>

#include <bruce/mock_kafka_server/v0_client_handler.h>

using namespace Bruce;
using namespace Bruce::MockKafkaServer;

TSingleClientHandlerBase *
TV0ClientHandlerFactory::CreateClientHandler(
    const std::shared_ptr<TPortMap> &port_map, size_t port_offset,
    TSharedState &ss, Base::TFd &&client_socket) {
  assert(this);
  return new TV0ClientHandler(Config, Setup, port_map, port_offset, ss,
                              std::move(client_socket));
}
