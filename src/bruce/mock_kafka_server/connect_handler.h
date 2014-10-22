/* <bruce/mock_kafka_server/connect_handler.h>

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

   Mock Kafka server class for handling incoming connections.
 */

#pragma once

#include <cstddef>
#include <memory>

#include <base/no_copy_semantics.h>
#include <bruce/mock_kafka_server/client_handler_factory_base.h>
#include <bruce/mock_kafka_server/connect_handler_base.h>
#include <bruce/mock_kafka_server/port_map.h>
#include <bruce/mock_kafka_server/shared_state.h>
#include <fiber/dispatcher.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TConnectHandler final : public TConnectHandlerBase {
      NO_COPY_SEMANTICS(TConnectHandler);

      public:
      TConnectHandler(TSharedState &ss,
                      TClientHandlerFactoryBase &client_handler_factory,
                      size_t port_offset,
                      const std::shared_ptr<TPortMap> &port_map)
          : TConnectHandlerBase(ss),
            ClientHandlerFactory(client_handler_factory),
            PortOffset(port_offset),
            PortMap(port_map) {
      }

      virtual ~TConnectHandler() noexcept { }

      virtual void OnEvent(int fd, short flags) override;

      private:
      TClientHandlerFactoryBase &ClientHandlerFactory;

      size_t PortOffset;

      /* Mappings between virtual and physical ports.  See big comment in
         <bruce/mock_kafka_server/port_map.h> for an explanation of what is
         going on here. */
      std::shared_ptr<TPortMap> PortMap;
    };  // TConnectHandler

  }  // MockKafkaServer

}  // Bruce
