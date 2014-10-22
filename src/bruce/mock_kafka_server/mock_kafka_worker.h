/* <bruce/mock_kafka_server/mock_kafka_worker.h>

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

   Worker thread base class for mock Kafka server.
 */

#pragma once

#include <cstddef>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/util/worker_thread.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TMockKafkaWorker : public Bruce::Util::TWorkerThread {
      NO_COPY_SEMANTICS(TMockKafkaWorker);

      public:
      virtual ~TMockKafkaWorker() noexcept { }

      protected:
      explicit TMockKafkaWorker(Base::TFd &&client_socket)
          : ClientSocket(std::move(client_socket)) {  /* connected FD */
      }

      virtual void Run() = 0;

      enum class TIoResult {
        Success,
        Disconnected,
        UnexpectedEnd,
        EmptyReadUnexpectedEnd,
        GotShutdownRequest
      };  // TIoResult

      TIoResult TryReadExactlyOrShutdown(int fd, void *buf, size_t size);

      TIoResult TryWriteExactlyOrShutdown(int fd, const void *buf,
          size_t size);

      Base::TFd ClientSocket;
    };  // TMockKafkaWorker

  }  // MockKafkaServer

}  // Bruce
