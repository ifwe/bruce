/* <bruce/mock_kafka_server/cmd_worker.h>

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

   Worker thread class for command interface (error injection, etc.).
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <base/no_copy_semantics.h>
#include <bruce/mock_kafka_server/cmd.h>
#include <bruce/mock_kafka_server/mock_kafka_worker.h>
#include <bruce/mock_kafka_server/shared_state.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TCmdWorker final : public TMockKafkaWorker {
      NO_COPY_SEMANTICS(TCmdWorker);

      public:
      explicit TCmdWorker(TSharedState &ss, Base::TFd &&client_socket)
          : TMockKafkaWorker(std::move(client_socket)),  // connected FD
            Ss(ss) {
      }

      virtual ~TCmdWorker() noexcept;

      protected:
      virtual void Run() override;

      private:
      bool GetCmd();

      bool SendReply(bool success);

      TSharedState &Ss;

      std::vector<uint8_t> InputBuf;

      TCmd Cmd;
    };  // TCmdWorker

  }  // MockKafkaServer

}  // Bruce
