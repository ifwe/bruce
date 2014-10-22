/* <bruce/mock_kafka_server/cmd_handler.h>

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

   Mock Kafka server class for command interface (error injection, etc.).
 */

#pragma once

#include <cstddef>

#include <base/no_copy_semantics.h>
#include <bruce/mock_kafka_server/connect_handler_base.h>
#include <bruce/mock_kafka_server/shared_state.h>
#include <fiber/dispatcher.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TCmdHandler final : public TConnectHandlerBase {
      NO_COPY_SEMANTICS(TCmdHandler);

      public:
      explicit TCmdHandler(TSharedState &ss)
          : TConnectHandlerBase(ss) {
      }

      virtual ~TCmdHandler() noexcept { }

      virtual void OnEvent(int fd, short flags) override;
    };  // TCmdHandler

  }  // MockKafkaServer

}  // Bruce
