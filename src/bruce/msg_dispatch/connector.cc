/* <bruce/msg_dispatch/connector.cc>

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

   Implements <bruce/msg_dispatch/connector.h>.
 */

#include <bruce/msg_dispatch/connector.h>

#include <bruce/msg_dispatch/common.h>
#include <server/counter.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MsgDispatch;

SERVER_COUNTER(ConnectorFinishJoinAll);
SERVER_COUNTER(ConnectorStart);
SERVER_COUNTER(ConnectorStartFastShutdown);
SERVER_COUNTER(ConnectorStartJoinAll);
SERVER_COUNTER(ConnectorStartSlowShutdown);

TConnector::TConnector(size_t my_broker_index, TDispatcherSharedState &ds)
    : Sender(my_broker_index, ds, Cs),
      Receiver(my_broker_index, ds, Cs, Sender.GetShutdownWaitFd()),
      ShutdownStatus(TShutdownStatus::Normal) {
}

void TConnector::Start(const std::shared_ptr<TMetadata> &md) {
  assert(this);
  assert(md);
  assert(Cs.SendWaitAfterShutdown.empty());
  assert(Cs.AckWaitAfterShutdown.empty());
  assert(!Sender.IsStarted());
  assert(!Receiver.IsStarted());
  assert(!Sender.GetShutdownWaitFd().IsReadable());
  assert(!Receiver.GetShutdownWaitFd().IsReadable());
  assert(!Cs.ConnectFinished.GetFd().IsReadable());
  ShutdownStatus = TShutdownStatus::Normal;
  ConnectorStart.Increment();
  Sender.SetMetadata(md);
  Sender.Start();
  Receiver.SetMetadata(md);
  Receiver.Start();
}

void TConnector::StartSlowShutdown(uint64_t start_time) {
  assert(this);
  ConnectorStartSlowShutdown.Increment();
  Sender.StartSlowShutdown(start_time);
  Receiver.StartSlowShutdown(start_time);
}

void TConnector::StartFastShutdown() {
  assert(this);
  ConnectorStartFastShutdown.Increment();
  Sender.StartFastShutdown();
  Receiver.StartFastShutdown();
}

void TConnector::WaitForShutdownAcks() {
  assert(this);
  Sender.WaitForShutdownAck();
  Receiver.WaitForShutdownAck();
}

void TConnector::JoinAll() {
  assert(this);
  ConnectorStartJoinAll.Increment();
  Sender.Join();
  Receiver.Join();
  ShutdownStatus =
      ((Sender.GetShutdownStatus() != TShutdownStatus::Normal) ||
       (Receiver.GetShutdownStatus() != TShutdownStatus::Normal)) ?
      TShutdownStatus::Error : TShutdownStatus::Normal;

  /* The order of the remaining steps matters because we want to avoid getting
     messages unnecessarily out of order. */

  assert(Cs.SendWaitAfterShutdown.empty());
  Cs.SendWaitAfterShutdown.splice(Cs.SendWaitAfterShutdown.end(),
      Cs.ResendQueue.NonblockingGet());
  Sender.ExtractMsgs();
  assert(Cs.AckWaitAfterShutdown.empty());
  Receiver.ExtractMsgs();
  std::list<TProduceRequest> send_finished;
  send_finished.splice(send_finished.end(),
                       Cs.SendFinishedQueue.NonblockingGet());

  for (auto &request : send_finished) {
    EmptyAllTopics(request.second, Cs.AckWaitAfterShutdown);
  }

  Cs.Sock.Reset();
  ConnectorFinishJoinAll.Increment();
}
