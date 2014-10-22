/* <bruce/test_util/mock_router_thread.h>

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

   Mock router thread class for unit testing.
 */

#pragma once

#include <cstddef>

#include <base/event_semaphore.h>
#include <base/no_copy_semantics.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/config.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/metadata_timestamp.h>
#include <bruce/router_thread_api.h>
#include <bruce/util/gate.h>

namespace Bruce {

  namespace TestUtil {

    /* Mock router thread class for unit testing. */
    class TMockRouterThread final : public TRouterThreadApi {
      NO_COPY_SEMANTICS(TMockRouterThread);

      public:
      TMockRouterThread(const TConfig &config,
          const KafkaProto::TWireProtocol &kafka_protocol,
          TAnomalyTracker &anomaly_tracker,
          TMetadataTimestamp &metadata_timestamp)
          : Config(config),
            KafkaProtocol(kafka_protocol),
            AnomalyTracker(anomaly_tracker),
            MetadataTimestamp(metadata_timestamp) {
      }

      virtual ~TMockRouterThread() noexcept { }

      virtual const Base::TFd &GetInitWaitFd() const override;

      virtual TShutdownStatus GetShutdownStatus() const override;

      virtual TMsgChannel &GetMsgChannel() override;

      virtual size_t GetAckCount() const override;

      virtual Base::TEventSemaphore &GetMetadataUpdateRequestSem() override;

      virtual void Start() override;

      virtual bool IsStarted() const override;

      virtual void RequestShutdown() override;

      virtual const Base::TFd &GetShutdownWaitFd() const override;

      virtual void Join() override;

      void Run();

      const Base::TFd &GetShutdownRequestFd();

      void ClearShutdownRequest();

      /* The router thread receives messages from the input thread through this
         channel. */
      Util::TGate<TMsg::TPtr> MsgChannel;

      Base::TEventSemaphore InitFinishedSem;

      Base::TEventSemaphore ShutdownRequestedSem;

      Base::TEventSemaphore ShutdownFinishedSem;

      Base::TEventSemaphore MetadataUpdateRequestSem;

      const TConfig &Config;

      const KafkaProto::TWireProtocol &KafkaProtocol;

      TAnomalyTracker &AnomalyTracker;

      TMetadataTimestamp &MetadataTimestamp;
    };  // TMockRouterThread

  }  // TestUtil

}  // Bruce
