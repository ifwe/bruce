/* <bruce/msg_dispatch/kafka_dispatcher.cc>

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

   Implements <bruce/msg_dispatch/kafka_dispatcher.h>.
 */

#include <bruce/msg_dispatch/kafka_dispatcher.h>

#include <syslog.h>

#include <bruce/util/time_util.h>
#include <server/counter.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;
using namespace Bruce::Conf;
using namespace Bruce::Debug;
using namespace Bruce::KafkaProto;
using namespace Bruce::MsgDispatch;
using namespace Bruce::Util;

SERVER_COUNTER(BugDispatchBatchOutOfRangeIndex);
SERVER_COUNTER(BugDispatchMsgOutOfRangeIndex);
SERVER_COUNTER(BugGetAckWaitQueueOutOfRangeIndex);
SERVER_COUNTER(DispatchOneBatch);
SERVER_COUNTER(DispatchOneMsg);
SERVER_COUNTER(FinishDispatcherJoinAll);
SERVER_COUNTER(SkipOutOfServiceBroker);
SERVER_COUNTER(StartDispatcherFastShutdown);
SERVER_COUNTER(StartDispatcherJoinAll);
SERVER_COUNTER(StartDispatcherSlowShutdown);
SERVER_COUNTER(StartKafkaDispatcher);

TKafkaDispatcher::TKafkaDispatcher(const TConfig &config,
    const TCompressionConf &compression_conf,
    const TWireProtocol &kafka_protocol, TMsgStateTracker &msg_state_tracker,
    TAnomalyTracker &anomaly_tracker, const TGlobalBatchConfig &batch_config,
    const TDebugSetup &debug_setup)
    : Ds(config, compression_conf, kafka_protocol, msg_state_tracker,
         anomaly_tracker, debug_setup, batch_config),
      State(TState::Stopped),
      ShutdownStatus(TShutdownStatus::Normal) {
}

TKafkaDispatcherApi::TState TKafkaDispatcher::GetState() const {
  assert(this);
  return State;
}

size_t TKafkaDispatcher::GetBrokerCount() const {
  assert(this);
  return Connectors.size();
}

void TKafkaDispatcher::Start(const std::shared_ptr<TMetadata> &md) {
  assert(this);
  assert(md);
  assert(State == TState::Stopped);
  assert(!Ds.PauseButton.GetFd().IsReadable());
  assert(!Ds.GetShutdownWaitFd().IsReadable());
  assert(Ds.GetRunningThreadCount() == 0);
  StartKafkaDispatcher.Increment();
  ShutdownStatus = TShutdownStatus::Normal;
  const std::vector<TMetadata::TBroker> &brokers = md->GetBrokers();
  size_t num_in_service = md->NumInServiceBrokers();

  if (num_in_service > brokers.size()) {
    assert(false);
    syslog(LOG_ERR, "Bug!!! In service broker count %lu exceeds total broker "
           "count %lu", static_cast<unsigned long>(num_in_service),
           static_cast<unsigned long>(brokers.size()));
    num_in_service = brokers.size();
  }

  /* The connectors are not designed to be reused.  Therefore delete all
     connectors remaining from the last dispatcher execution and create new
     ones.  Doing things this way makes the connector implementation simpler
     and less susceptible to bugs being introduced. */

  Connectors.clear();
  Connectors.resize(num_in_service);
  Ds.MarkAllThreadsRunning(num_in_service);

  for (size_t i = 0; i < Connectors.size(); ++i) {
    assert(brokers[i].IsInService());
    std::unique_ptr<TConnector> &broker_ptr = Connectors[i];
    assert(!broker_ptr);
    broker_ptr.reset(new TConnector(i, Ds));
    syslog(LOG_NOTICE, "Starting send and receive threads for broker index "
           "%lu (Kafka ID %lu)", static_cast<unsigned long>(i),
           static_cast<unsigned long>(brokers[i].GetId()));
    broker_ptr->Start(md);
  }

  for (size_t i = Connectors.size(); i < brokers.size(); ++i) {
    assert(!brokers[i].IsInService());
    syslog(LOG_NOTICE,
           "Skipping out of service broker index %lu (Kafka ID %lu)",
           static_cast<unsigned long>(i),
           static_cast<unsigned long>(brokers[i].GetId()));
    SkipOutOfServiceBroker.Increment();
  }

  State = TState::Started;
}

void TKafkaDispatcher::Dispatch(TMsg::TPtr &&msg, size_t broker_index) {
  assert(this);
  assert(msg);
  assert(State != TState::Stopped);
  DispatchOneMsg.Increment();

  if (broker_index >= Connectors.size()) {
    assert(false);
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Bug!!! Cannot dispatch message because broker index is "
             "out of range: index %lu broker count %lu",
             static_cast<unsigned long>(broker_index),
             static_cast<unsigned long>(Connectors.size()));
    }

    BugDispatchMsgOutOfRangeIndex.Increment();
    Ds.Discard(std::move(msg), TAnomalyTracker::TDiscardReason::Bug);
    return;
  }

  assert(Connectors[broker_index]);
  Connectors[broker_index]->Dispatch(std::move(msg));
  assert(!msg);
}

void TKafkaDispatcher::DispatchNow(TMsg::TPtr &&msg, size_t broker_index) {
  assert(this);
  assert(msg);
  assert(State != TState::Stopped);
  DispatchOneMsg.Increment();

  if (broker_index >= Connectors.size()) {
    assert(false);
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Bug!!! Cannot dispatch message because broker index is "
             "out of range: index %lu broker count %lu",
             static_cast<unsigned long>(broker_index),
             static_cast<unsigned long>(Connectors.size()));
    }

    BugDispatchMsgOutOfRangeIndex.Increment();
    Ds.Discard(std::move(msg), TAnomalyTracker::TDiscardReason::Bug);
    return;
  }

  assert(Connectors[broker_index]);
  Connectors[broker_index]->DispatchNow(std::move(msg));
  assert(!msg);
}

void TKafkaDispatcher::DispatchNow(std::list<std::list<TMsg::TPtr>> &&batch,
    size_t broker_index) {
  assert(this);
  assert(State != TState::Stopped);

  if (batch.empty()) {
    return;
  }

  DispatchOneBatch.Increment();

  if (broker_index >= Connectors.size()) {
    assert(false);
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Bug!!! Cannot dispatch message batch because broker "
             "index is out of range: index %lu broker count %lu",
             static_cast<unsigned long>(broker_index),
             static_cast<unsigned long>(Connectors.size()));
    }

    BugDispatchBatchOutOfRangeIndex.Increment();
    Ds.Discard(std::move(batch), TAnomalyTracker::TDiscardReason::Bug);
    return;
  }

  assert(Connectors[broker_index]);
  Connectors[broker_index]->DispatchNow(std::move(batch));
  assert(batch.empty());
}

void TKafkaDispatcher::StartSlowShutdown(uint64_t start_time) {
  assert(this);
  assert(State != TState::Stopped);
  StartDispatcherSlowShutdown.Increment();

  for (std::unique_ptr<TConnector> &c : Connectors) {
    assert(c);
    c->StartSlowShutdown(start_time);
  }

  for (std::unique_ptr<TConnector> &c : Connectors) {
    assert(c);
    c->WaitForShutdownAcks();
  }

  State = TState::ShuttingDown;
}

void TKafkaDispatcher::StartFastShutdown() {
  assert(this);
  assert(State != TState::Stopped);
  StartDispatcherFastShutdown.Increment();

  for (std::unique_ptr<TConnector> &c : Connectors) {
    assert(c);
    c->StartFastShutdown();
  }

  for (std::unique_ptr<TConnector> &c : Connectors) {
    assert(c);
    c->WaitForShutdownAcks();
  }

  State = TState::ShuttingDown;
}

const TFd &TKafkaDispatcher::GetPauseFd() const {
  assert(this);
  return Ds.PauseButton.GetFd();
}

const TFd &TKafkaDispatcher::GetShutdownWaitFd() const {
  assert(this);
  return Ds.GetShutdownWaitFd();
}

void TKafkaDispatcher::JoinAll() {
  assert(this);
  assert(State != TState::Stopped);
  StartDispatcherJoinAll.Increment();
  syslog(LOG_NOTICE, "Waiting for dispatcher shutdown status");
  TShutdownStatus shutdown_status = TShutdownStatus::Normal;

  for (std::unique_ptr<TConnector> &c : Connectors) {
    assert(c);
    c->JoinAll();

    if (c->GetShutdownStatus() != TShutdownStatus::Normal) {
      shutdown_status = TShutdownStatus::Error;
    }
  }

  ShutdownStatus = shutdown_status;
  Ds.PauseButton.Reset();
  assert(Ds.GetShutdownWaitFd().IsReadable());
  Ds.ResetThreadFinishedState();
  FinishDispatcherJoinAll.Increment();
  syslog(LOG_NOTICE, "Finished waiting for dispatcher shutdown status");
  State = TState::Stopped;
}

TKafkaDispatcherApi::TShutdownStatus
TKafkaDispatcher::GetShutdownStatus() const {
  assert(this);
  return ShutdownStatus;
}

std::list<std::list<TMsg::TPtr>>
TKafkaDispatcher::GetAckWaitQueueAfterShutdown(size_t broker_index) {
  assert(this);
  assert(State == TState::Stopped);

  if (broker_index >= Connectors.size()) {
    assert(false);
    syslog(LOG_ERR, "Bug!!! Cannot get ACK wait queue for out of range broker "
           "index %lu broker count %lu",
           static_cast<unsigned long>(broker_index),
           static_cast<unsigned long>(Connectors.size()));
    BugGetAckWaitQueueOutOfRangeIndex.Increment();
    return std::list<std::list<TMsg::TPtr>>();
  }

  assert(Connectors[broker_index]);
  return Connectors[broker_index]->GetAckWaitQueueAfterShutdown();
}

std::list<std::list<TMsg::TPtr>>
TKafkaDispatcher::GetSendWaitQueueAfterShutdown(size_t broker_index) {
  assert(this);
  assert(State == TState::Stopped);

  if (broker_index >= Connectors.size()) {
    assert(false);
    syslog(LOG_ERR, "Bug!!! Cannot get send wait queue for out of range "
           "broker index %lu broker count %lu",
           static_cast<unsigned long>(broker_index),
           static_cast<unsigned long>(Connectors.size()));
    return std::list<std::list<TMsg::TPtr>>();
  }

  assert(Connectors[broker_index]);
  return Connectors[broker_index]->GetSendWaitQueueAfterShutdown();
}

size_t TKafkaDispatcher::GetAckCount() const {
  assert(this);
  return Ds.GetAckCount();
}
