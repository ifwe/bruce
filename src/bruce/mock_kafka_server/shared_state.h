/* <bruce/mock_kafka_server/shared_state.h>

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

   Shared state for mock Kafka server.
 */

#pragma once

#include <memory>
#include <vector>

#include <base/no_copy_semantics.h>
#include <bruce/mock_kafka_server/cmd_bucket.h>
#include <bruce/mock_kafka_server/config.h>
#include <bruce/mock_kafka_server/mock_kafka_worker.h>
#include <bruce/mock_kafka_server/received_request_tracker.h>
#include <bruce/mock_kafka_server/setup.h>
#include <bruce/mock_kafka_server/thread_terminate_handler.h>
#include <fiber/dispatcher.h>

namespace Bruce {

  namespace MockKafkaServer {

    struct TSharedState {
      private:
      NO_COPY_SEMANTICS(TSharedState);

      public:
      struct TPerConnectionState {
        /* worker thread to handle client connection */
        std::shared_ptr<TMockKafkaWorker> Worker;

        /* handler to register with dispatcher; for monitoring worker shutdown
           wait fd */
        std::shared_ptr<TThreadTerminateHandler> TerminateHandler;
      };  // TPerConnectionState

      const TConfig &Config;

      const bool TrackReceivedRequests;

      TSetup::TInfo Setup;

      /* This must appear before ConnectHandlers, since all connection handlers
         must be destroyed before the dispatcher. */
      std::unique_ptr<Fiber::TDispatcher> Dispatcher;

      /* Key is shutdown wait FD for Worker in TPerConnectionState. */
      std::unordered_map<int, TPerConnectionState> PerConnectionMap;

      /* Error injection commands to be snarfed by worker threads. */
      TCmdBucket CmdBucket;

      /* Requests received and processed by the mock Kafka server.  Only used
         during unut tests.  The mock Kafka server adds items to this and the
         unit test code removes items. */
      TReceivedRequestTracker ReceivedRequests;

      TSharedState(const TConfig &config, bool track_received_requests)
          : Config(config),
            TrackReceivedRequests(track_received_requests) {
      }

    };  // TSharedState

  }  // MockKafkaServer

}  // Bruce
