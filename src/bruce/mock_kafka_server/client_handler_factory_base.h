/* <bruce/mock_kafka_server/client_handler_factory_base.h>

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

   Factory that creates objects derived from TSingleClientHandlerBase according
   to what version of Kafka we are simulating.
 */

#pragma once

#include <cstddef>
#include <memory>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/mock_kafka_server/config.h>
#include <bruce/mock_kafka_server/port_map.h>
#include <bruce/mock_kafka_server/setup.h>
#include <bruce/mock_kafka_server/shared_state.h>
#include <bruce/mock_kafka_server/single_client_handler_base.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TClientHandlerFactoryBase {
      NO_COPY_SEMANTICS(TClientHandlerFactoryBase);

      public:
      virtual ~TClientHandlerFactoryBase() noexcept { }

      /* Create a TSingleClientHandlerBase of the appropriate subclass type
         depending on what version of Kafka we are simulating. */
      virtual TSingleClientHandlerBase *CreateClientHandler(
          const std::shared_ptr<TPortMap> &port_map, size_t port_offset,
          TSharedState &ss, Base::TFd &&client_socket) = 0;

      static TClientHandlerFactoryBase *CreateFactory(const TConfig &config,
          const TSetup::TInfo &setup);

      protected:
      const TConfig &Config;

      const TSetup::TInfo &Setup;

      /* Only our static CreateFactory() method creates these. */
      TClientHandlerFactoryBase(const TConfig &config,
                                const TSetup::TInfo &setup)
          : Config(config),
            Setup(setup) {
      }
    };  // TClientHandlerFactoryBase

  }  // MockKafkaServer

}  // Bruce
