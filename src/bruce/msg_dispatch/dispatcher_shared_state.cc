/* <bruce/msg_dispatch/dispatcher_shared_state.cc>

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

   Implements <bruce/msg_dispatch/dispatcher_shared_state.h>.
 */

#include <bruce/msg_dispatch/dispatcher_shared_state.h>

#include <algorithm>

#include <syslog.h>

#include <bruce/msg_state_tracker.h>
#include <server/counter.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;
using namespace Bruce::Conf;
using namespace Bruce::Debug;
using namespace Bruce::KafkaProto;
using namespace Bruce::MsgDispatch;

SERVER_COUNTER(LastDispatcherThreadFinished);

TDispatcherSharedState::TDispatcherSharedState(const TConfig &config,
     const TCompressionConf &compression_conf,
     const TWireProtocol &kafka_protocol, TMsgStateTracker &msg_state_tracker,
     TAnomalyTracker &anomaly_tracker, const TDebugSetup &debug_setup,
     const TGlobalBatchConfig &batch_config)
    : Config(config),
      CompressionConf(compression_conf),
      KafkaProtocol(kafka_protocol),
      MsgStateTracker(msg_state_tracker),
      AnomalyTracker(anomaly_tracker),
      DebugSetup(debug_setup),
      BatchConfig(batch_config),
      RunningThreadCount(0),
      AckCount(0) {
}

void TDispatcherSharedState::Discard(TMsg::TPtr &&msg,
    TAnomalyTracker::TDiscardReason reason) {
  assert(this);
  assert(msg);
  TMsg::TPtr to_discard(std::move(msg));
  AnomalyTracker.TrackDiscard(to_discard, reason);
  MsgStateTracker.MsgEnterProcessed(*to_discard);
}

void TDispatcherSharedState::Discard(std::list<TMsg::TPtr> &&msg_list,
                   TAnomalyTracker::TDiscardReason reason) {
  assert(this);
  std::list<TMsg::TPtr> to_discard(std::move(msg_list));

  for (TMsg::TPtr &msg : to_discard) {
    assert(msg);
    AnomalyTracker.TrackDiscard(msg, reason);
  }

  MsgStateTracker.MsgEnterProcessed(to_discard);
}

void TDispatcherSharedState::Discard(std::list<std::list<TMsg::TPtr>> &&batch,
                   TAnomalyTracker::TDiscardReason reason) {
  assert(this);
  std::list<std::list<TMsg::TPtr>> to_discard(std::move(batch));

  for (auto &msg_list : to_discard) {
    for (TMsg::TPtr &msg : msg_list) {
      assert(msg);
      AnomalyTracker.TrackDiscard(msg, reason);
    }
  }

  MsgStateTracker.MsgEnterProcessed(to_discard);
}

void TDispatcherSharedState::MarkAllThreadsRunning(
    size_t in_service_broker_count) {
  assert(this);
  assert(RunningThreadCount.load() == 0);
  assert(!ShutdownFinished.GetFd().IsReadable());

  /* There is a send thread and a receive thread for each in service broker, so
     multiply by 2. */
  std::atomic_store(&RunningThreadCount, 2 * in_service_broker_count);
}

void TDispatcherSharedState::MarkThreadFinished() {
  assert(this);

  if (--RunningThreadCount == 0) {
    syslog(LOG_NOTICE, "All send and receive threads finished shutting down");
    LastDispatcherThreadFinished.Increment();
    ShutdownFinished.Push();
  }
}

void TDispatcherSharedState::ResetThreadFinishedState() {
  assert(this);
  assert(RunningThreadCount.load() == 0);
  assert(ShutdownFinished.GetFd().IsReadable());
  ShutdownFinished.Reset();
}
