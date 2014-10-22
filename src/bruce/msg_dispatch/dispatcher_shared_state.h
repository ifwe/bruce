/* <bruce/msg_dispatch/dispatcher_shared_state.h>

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

   State shared by Kafka dispatcher and all of its threads.
 */

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <list>
#include <string>
#include <unordered_set>

#include <base/event_semaphore.h>
#include <base/no_copy_semantics.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/batch/global_batch_config.h>
#include <bruce/conf/compression_conf.h>
#include <bruce/config.h>
#include <bruce/debug/debug_setup.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/msg.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/util/pause_button.h>

namespace Bruce {

  namespace MsgDispatch {

    struct TDispatcherSharedState {
      NO_COPY_SEMANTICS(TDispatcherSharedState);

      const TConfig &Config;

      Conf::TCompressionConf CompressionConf;

      const KafkaProto::TWireProtocol &KafkaProtocol;

      TMsgStateTracker &MsgStateTracker;

      TAnomalyTracker &AnomalyTracker;

      const Debug::TDebugSetup &DebugSetup;

      Util::TPauseButton PauseButton;

      const Batch::TGlobalBatchConfig BatchConfig;

      TDispatcherSharedState(const TConfig &config,
          const Conf::TCompressionConf &compression_conf,
          const KafkaProto::TWireProtocol &kafka_protocol,
          TMsgStateTracker &msg_state_tracker,
          TAnomalyTracker &anomaly_tracker,
          const Debug::TDebugSetup &debug_setup,
          const Batch::TGlobalBatchConfig &batch_config);

      size_t GetAckCount() const {
        assert(this);
        return AckCount.load();
      }

      void IncrementAckCount() {
        assert(this);
        ++AckCount;
      }

      void Discard(TMsg::TPtr &&msg, TAnomalyTracker::TDiscardReason reason);

      void Discard(std::list<TMsg::TPtr> &&msg_list,
                   TAnomalyTracker::TDiscardReason reason);

      void Discard(std::list<std::list<TMsg::TPtr>> &&batch,
                   TAnomalyTracker::TDiscardReason reason);

      const Base::TFd &GetShutdownWaitFd() const {
        assert(this);
        return ShutdownFinished.GetFd();
      }

      size_t GetRunningThreadCount() const {
        assert(this);
        return RunningThreadCount.load();
      }

      void MarkAllThreadsRunning(size_t in_service_broker_count);

      /* Called by send and receive threads when finished shutting down. */
      void MarkThreadFinished();

      void ResetThreadFinishedState();

      private:
      /* This is the total number of send and receive threads that have been
         started and have not yet called MarkShutdownFinished(); */
      std::atomic<size_t> RunningThreadCount;

      Base::TEventSemaphore ShutdownFinished;

      std::atomic<size_t> AckCount;
    };  // TDispatcherSharedState

  }  // MsgDispatch

}  // Bruce
