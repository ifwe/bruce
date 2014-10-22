/* <bruce/mock_kafka_server/connect_handler_base.h>

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

   Base mock Kafka server class for TDispatcher handlers for incoming
   connections.
 */

#pragma once

#include <cstddef>

#include <base/no_copy_semantics.h>
#include <bruce/mock_kafka_server/mock_kafka_worker.h>
#include <bruce/mock_kafka_server/shared_state.h>
#include <fiber/dispatcher.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TConnectHandlerBase : public Fiber::TDispatcher::THandler {
      NO_COPY_SEMANTICS(TConnectHandlerBase);

      public:
      virtual ~TConnectHandlerBase() noexcept {
        Unregister();
      }

      virtual void OnEvent(int fd, short flags) = 0;

      virtual void OnShutdown() override;

      void RegisterWithDispatcher(Fiber::TDispatcher &dispatcher, int fd,
          short flags) {
        Register(&dispatcher, fd, flags);
      }

      protected:
      explicit TConnectHandlerBase(TSharedState &ss)
          : Ss(ss) {
      }

      void RunThread(TMockKafkaWorker *w);

      TSharedState &Ss;

      private:
      void DeleteThreadState(int shutdown_wait_fd);
    };  // TConnectHandlerBase

  }  // MockKafkaServer

}  // Bruce
