/* <bruce/mock_kafka_server/thread_terminate_handler.h>

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

   Mock Kafka server class for handling terminated threads.
 */

#pragma once

#include <algorithm>
#include <functional>

#include <base/no_copy_semantics.h>
#include <fiber/dispatcher.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TThreadTerminateHandler final : public Fiber::TDispatcher::THandler {
      NO_COPY_SEMANTICS(TThreadTerminateHandler);

      public:
      explicit TThreadTerminateHandler(
          std::function<void()> &&terminate_handler)
          : TerminateHandler(std::move(terminate_handler)) {
      }

      virtual ~TThreadTerminateHandler() noexcept {
        Unregister();
      }

      virtual void OnEvent(int fd, short flags) override;

      virtual void OnShutdown() override;

      void RegisterWithDispatcher(Fiber::TDispatcher &dispatcher, int fd,
          short flags) {
        Register(&dispatcher, fd, flags);
      }

      private:
      std::function<void()> TerminateHandler;
    };  // TThreadTerminateHandler

  }  // MockKafkaServer

}  // Bruce
