/* <bruce/msg_dispatch/sender.cc>

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

   Implements <bruce/msg_dispatch/sender.h>.
 */

#include <bruce/msg_dispatch/sender.h>

#include <exception>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include <base/error_utils.h>
#include <base/gettid.h>
#include <base/no_default_case.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/util/connect_to_host.h>
#include <bruce/util/msg_util.h>
#include <bruce/util/system_error_codes.h>
#include <server/counter.h>
#include <socket/db/error.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;
using namespace Bruce::Debug;
using namespace Bruce::KafkaProto;
using namespace Bruce::MsgDispatch;
using namespace Bruce::Util;

SERVER_COUNTER(BugProduceRequestEmpty);
SERVER_COUNTER(SenderCheckInputQueue);
SERVER_COUNTER(SenderConnectFail);
SERVER_COUNTER(SenderConnectSuccess);
SERVER_COUNTER(SenderFinishRun);
SERVER_COUNTER(SenderFinishWaitShutdownAck);
SERVER_COUNTER(SenderHandleResendRequest);
SERVER_COUNTER(SenderSocketError);
SERVER_COUNTER(SenderSocketTimeout);
SERVER_COUNTER(SenderStartConnect);
SERVER_COUNTER(SenderStartFastShutdown);
SERVER_COUNTER(SenderStartRun);
SERVER_COUNTER(SenderStartSlowShutdown);
SERVER_COUNTER(SenderStartWaitShutdownAck);
SERVER_COUNTER(SendProduceRequestOk);

TSender::TSender(size_t my_broker_index, TDispatcherSharedState &ds,
    TConnectionSharedState &cs)
    : MyBrokerIndex(my_broker_index),
      Ds(ds),
      Cs(cs),
      InputQueue(ds.BatchConfig, ds.MsgStateTracker),
      DebugLogger(ds.DebugSetup, TDebugSetup::TLogId::MSG_SEND,
                  !ds.Config.OmitTimestamp, ds.Config.UseOldOutputFormat),
      RequestFactory(ds.Config, ds.BatchConfig, ds.CompressionConf,
                     ds.KafkaProtocol, my_broker_index,
                     !ds.Config.OmitTimestamp, ds.Config.UseOldOutputFormat),
      PauseInProgress(false),
      Destroying(false),
      ShutdownStatus(TShutdownStatus::Normal) {
}

TSender::~TSender() noexcept {
  /* This will shut down the thread if something unexpected happens.  Setting
     the 'Destroying' flag tells the thread to shut down immediately when it
     gets the shutdown request. */
  Destroying = true;
  ShutdownOnDestroy();
}

void TSender::StartSlowShutdown(uint64_t start_time) {
  assert(this);
  assert(IsStarted());
  assert(!OptShutdownCmd.IsKnown());
  SenderStartSlowShutdown.Increment();
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Sending slow shutdown request to send thread (index %lu "
         "broker %ld)", static_cast<unsigned long>(MyBrokerIndex), broker_id);
  OptShutdownCmd.MakeKnown(start_time);
  RequestShutdown();
}

void TSender::StartFastShutdown() {
  assert(this);
  assert(IsStarted());
  assert(!OptShutdownCmd.IsKnown());
  SenderStartFastShutdown.Increment();
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Sending fast shutdown request to send thread (index %lu "
         "broker %ld)", static_cast<unsigned long>(MyBrokerIndex),
         broker_id);
  OptShutdownCmd.MakeKnown();
  RequestShutdown();
}

void TSender::WaitForShutdownAck() {
  assert(this);
  SenderStartWaitShutdownAck.Increment();
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Waiting for shutdown ACK from send thread (index %lu "
         "broker %ld)", static_cast<unsigned long>(MyBrokerIndex), broker_id);

  /* In addition to waiting for the shutdown ACK, we must wait for shutdown
     finished, since the thread may have started shutting down on its own
     immediately before we sent the shutdown request. */
  static const size_t POLL_ARRAY_SIZE = 2;
  struct pollfd poll_array[POLL_ARRAY_SIZE];
  poll_array[0].fd = ShutdownAck.GetFd();
  poll_array[0].events = POLLIN;
  poll_array[0].revents = 0;
  poll_array[1].fd = GetShutdownWaitFd();
  poll_array[1].events = POLLIN;
  poll_array[1].revents = 0;

  /* Don't check for EINTR, since this thread has signals masked. */
  IfLt0(poll(poll_array, POLL_ARRAY_SIZE, -1));

  const char *blurb = poll_array[0].revents ?
      "shutdown ACK" : "shutdown finished notification";
  syslog(LOG_NOTICE, "Got %s from send thread (index %lu broker %ld)", blurb,
         static_cast<unsigned long>(MyBrokerIndex), broker_id);
  SenderFinishWaitShutdownAck.Increment();
  OptShutdownCmd.Reset();
}

void TSender::ExtractMsgs() {
  assert(this);

  /* The order in which we move remaining messages to Cs.SendWaitAfterShutdown
     matters because we want to avoid getting messages unnecessarily out of
     order. */

  std::list<std::list<TMsg::TPtr>> &sw = Cs.SendWaitAfterShutdown;

  if (CurrentRequest.IsKnown()) {
    EmptyAllTopics(CurrentRequest->second, sw);
  }

  sw.splice(sw.end(), RequestFactory.GetAll());
  Metadata.reset();
  assert(!Destroying);
  ShutdownStatus = TShutdownStatus::Normal;
  Cs.SendWaitAfterShutdown.splice(Cs.SendWaitAfterShutdown.end(),
                                  InputQueue.Reset());

  /* After emptying out the sender, don't bother reinitializing it to a newly
     constructed state.  It will be destroyed and recreated before the
     dispatcher restarts. */
}

void TSender::Run() {
  assert(this);
  assert(Metadata);
  SenderStartRun.Increment();
  long broker_id = ~0;

  try {
    assert(MyBrokerIndex < Metadata->GetBrokers().size());
    broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
    syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) started",
           static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id);
    DoRun();
  } catch (const TShutdownOnDestroy &) {
    /* Nothing to do here. */
  } catch (const std::exception &x) {
    syslog(LOG_ERR, "Fatal error in send thread %d (index %lu broker %ld): %s",
           static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id, x.what());
    _exit(EXIT_FAILURE);
  } catch (...) {
    syslog(LOG_ERR, "Fatal unknown error in send thread %d (index %lu broker "
           "%ld)", static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id);
    _exit(EXIT_FAILURE);
  }

  syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) finished %s",
         static_cast<int>(Gettid()), static_cast<unsigned long>(MyBrokerIndex),
         broker_id, (ShutdownStatus == TShutdownStatus::Normal) ?
                    "normally" : "on error");
  Ds.MarkThreadFinished();
  SenderFinishRun.Increment();
}

bool TSender::DoConnect() {
  assert(!Cs.Sock.IsOpen());
  const TMetadata::TBroker &broker = Metadata->GetBrokers()[MyBrokerIndex];
  assert(broker.IsInService());
  const std::string &host = broker.GetHostname();
  uint16_t port = broker.GetPort();
  long broker_id = broker.GetId();
  syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) connecting to "
         "host %s port %u", static_cast<int>(Gettid()),
         static_cast<unsigned long>(MyBrokerIndex), broker_id, host.c_str(),
         static_cast<unsigned>(port));

  try {
    ConnectToHost(host, port, Cs.Sock);
  } catch (const std::system_error &x) {
    syslog(LOG_ERR, "Starting pause on failure to connect to broker %s port "
           "%u: %s", host.c_str(), static_cast<unsigned>(port), x.what());
    assert(!Cs.Sock.IsOpen());
    return false;
  } catch (const Socket::Db::TError &x) {
    syslog(LOG_ERR, "Starting pause on failure to connect to broker %s port "
           "%u: %s", host.c_str(), static_cast<unsigned>(port), x.what());
    assert(!Cs.Sock.IsOpen());
    return false;
  }

  if (!Cs.Sock.IsOpen()) {
    syslog(LOG_ERR, "Starting pause on failure to connect to broker %s port "
           "%u", host.c_str(), static_cast<unsigned>(port));
    return false;
  }

  syslog(LOG_NOTICE,
         "Send thread %d (index %lu broker %ld) connect successful",
         static_cast<int>(Gettid()), static_cast<unsigned long>(MyBrokerIndex),
         broker_id);
  return true;
}

bool TSender::ConnectToBroker() {
  assert(this);
  SenderStartConnect.Increment();
  bool success = DoConnect();

  if (success) {
    SenderConnectSuccess.Increment();
  } else {
    SenderConnectFail.Increment();
    Ds.PauseButton.Push();
  }

  Cs.ConnectFinished.Push();  // notify receive thread
  return success;
}

void TSender::InitMainLoopPollArray() {
  struct pollfd &sock_item = MainLoopPollArray[TMainLoopPollItem::SockWrite];
  struct pollfd &shutdown_item =
      MainLoopPollArray[TMainLoopPollItem::ShutdownRequest];
  struct pollfd &pause_item =
      MainLoopPollArray[TMainLoopPollItem::PauseButton];
  struct pollfd &input_item = MainLoopPollArray[TMainLoopPollItem::InputQueue];
  struct pollfd &resend_item =
      MainLoopPollArray[TMainLoopPollItem::ResendQueue];
  sock_item.fd = RequestFactory.IsEmpty() ? -1 : int(Cs.Sock);
  sock_item.events = POLLOUT;
  sock_item.revents = 0;
  shutdown_item.fd = GetShutdownRequestFd();
  shutdown_item.events = POLLIN;
  shutdown_item.revents = 0;
  pause_item.fd = Ds.PauseButton.GetFd();
  pause_item.events = POLLIN;
  pause_item.revents = 0;
  input_item.fd = OptInProgressShutdown.IsKnown() ?
                  -1 : int(InputQueue.GetSenderNotifyFd());
  input_item.events = POLLIN;
  input_item.revents = 0;

  /* Note: In the case of a slow shutdown, the send thread stops handling
     resend requests.  Handling resend requests during slow shutdown would make
     things substantially more complex, and it's not a problem worth solving
     anyway.  It is preferable to not shut down bruce until clients have
     stopped sending messages and bruce has finished processing messages. */
  resend_item.fd = OptInProgressShutdown.IsKnown() ?
                   -1 : int(Cs.ResendQueue.GetMsgAvailableFd());
  resend_item.events = POLLIN;
  resend_item.revents = 0;
}

void TSender::InitSendLoopPollArray() {
  struct pollfd &sock_item = SendLoopPollArray[TSendLoopPollItem::SockWrite];
  struct pollfd &shutdown_item =
      SendLoopPollArray[TSendLoopPollItem::ShutdownRequest];
  sock_item.fd = Cs.Sock;
  sock_item.events = POLLOUT;
  sock_item.revents = 0;
  shutdown_item.fd = GetShutdownRequestFd();
  shutdown_item.events = POLLIN;
  shutdown_item.revents = 0;
}

int TSender::ComputeShutdownTimeout(uint64_t now) const {
  assert(this);

  if (OptInProgressShutdown.IsUnknown()) {
    return -1;
  }

  uint64_t shutdown_deadline = OptInProgressShutdown->Deadline;
  return (now > shutdown_deadline) ? 0 : (shutdown_deadline - now);
}

int TSender::ComputeMainLoopPollTimeout(uint64_t now,
    int shutdown_timeout) const {
  assert(this);
  int timeout = RequestFactory.IsEmpty() ?
      -1 : static_cast<int>(Ds.Config.KafkaSocketTimeout * 1000);

  if (shutdown_timeout >= 0) {
    timeout = (timeout < 0) ?
        shutdown_timeout : std::min(timeout, shutdown_timeout);
  }

  if (OptNextBatchExpiry.IsKnown()) {
    uint64_t batch_deadline = *OptNextBatchExpiry;
    int batch_timeout = (now > batch_deadline) ?
        0 : (batch_deadline - now);
    timeout = (timeout < 0) ?
        batch_timeout : std::min(timeout, batch_timeout);
  }

  return timeout;
}

void TSender::CheckInputQueue(bool pop_sem) {
  assert(this);
  SenderCheckInputQueue.Increment();
  std::list<std::list<TMsg::TPtr>> ready_msgs;
  TMsg::TTimestamp expiry = 0;
  uint64_t now = GetEpochMilliseconds();
  bool has_expiry = pop_sem ?
      InputQueue.Get(now, expiry, ready_msgs) :
      InputQueue.NonblockingGet(now, expiry, ready_msgs);
  OptNextBatchExpiry.Reset();

  if (has_expiry) {
    OptNextBatchExpiry.MakeKnown(expiry);
  }

  RequestFactory.Put(std::move(ready_msgs));
}

void TSender::SetFastShutdownState() {
  assert(this);
  uint64_t deadline = GetEpochMilliseconds() +
                      Ds.Config.DispatcherRestartMaxDelay;

  if (OptInProgressShutdown.IsKnown()) {
    TInProgressShutdown &shutdown_state = *OptInProgressShutdown;
    shutdown_state.Deadline = std::min(shutdown_state.Deadline, deadline);
    shutdown_state.FastShutdown = true;
  } else {
    OptInProgressShutdown.MakeKnown(deadline, true);
  }
}

void TSender::HandlePauseDetected(bool sending) {
  assert(this);
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) detected pause %s",
         static_cast<int>(Gettid()), static_cast<unsigned long>(MyBrokerIndex),
         broker_id, sending ? "while sending" : "in main loop: finishing now");

  if (sending) {
    PauseInProgress = true;
    SetFastShutdownState();
  }
}

bool TSender::HandleShutdownRequest(bool sending) {
  assert(this);

  if (Destroying) {
    throw TShutdownOnDestroy();
  }

  bool keep_running = true;
  assert(OptShutdownCmd.IsKnown());
  const TShutdownCmd &cmd = *OptShutdownCmd;
  bool is_fast = cmd.OptSlowShutdownStartTime.IsUnknown();

  if (is_fast) {
    if (sending) {
      SetFastShutdownState();
    } else {
      keep_running = false;
    }
  } else {
    /* Before sending the slow shutdown request, the router thread routed all
       remaining messages to the dispatcher.  Get all remaining messages before
       we stop monitoring our input queue. */
    RequestFactory.Put(InputQueue.GetAllOnShutdown());

    uint64_t deadline = *cmd.OptSlowShutdownStartTime +
                        Ds.Config.ShutdownMaxDelay;

    if (OptInProgressShutdown.IsKnown()) {
      TInProgressShutdown &shutdown_state = *OptInProgressShutdown;
      shutdown_state.Deadline = std::min(shutdown_state.Deadline, deadline);
    } else {
      OptInProgressShutdown.MakeKnown(deadline, false);
    }
  }

  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) sending ACK for "
         "%s shutdown", static_cast<int>(Gettid()),
         static_cast<unsigned long>(MyBrokerIndex), broker_id,
         is_fast ? "fast" : "slow");
  ShutdownAck.Push();
  ClearShutdownRequest();

  if (!keep_running) {
    syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) finishing now "
           "on fast shutdown", static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id);
  }

  return keep_running;
}

bool TSender::WaitForSendReady() {
  assert(this);

  /* We don't allow a shutdown deadline or pause to interrupt us in the middle
     of sending a produce request.  Therefore our deadline is based only on our
     socket timeout value.  If the pause button is pushed while we are sending,
     the main loop will detect it once we have finished the send.

     TODO: Prevent integer overflow if socket timeout is ridiculously large. */
  uint64_t now = GetEpochMilliseconds();
  uint64_t deadline = now + (Ds.Config.KafkaSocketTimeout * 1000);
  int timeout = deadline - now;

  for (; ; ) {
    InitSendLoopPollArray();

    /* Don't check for EINTR, since this thread has signals masked. */
    int ret = IfLt0(poll(SendLoopPollArray, SendLoopPollArray.Size(),
                         timeout));

    if (ret == 0) {
      return false;  // socket timeout
    }

    if (SendLoopPollArray[TSendLoopPollItem::ShutdownRequest].revents) {
      HandleShutdownRequest(true);
    }

    if (SendLoopPollArray[TSendLoopPollItem::SockWrite].revents) {
      break;
    }

    now = GetEpochMilliseconds();
    timeout = (now > deadline) ? 0 : (deadline - now);
  }

  return true;
}

TSender::TSockOpResult TSender::SendOneProduceRequest() {
  assert(this);
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  size_t bytes_sent = 0;

  for (; ; ) {
    try {
      assert(SendBuf.size() >= bytes_sent);
      bytes_sent += IfLt0(send(Cs.Sock, &SendBuf[0] + bytes_sent,
                               SendBuf.size() - bytes_sent, MSG_NOSIGNAL));
    } catch (const std::system_error &x) {
      if (LostTcpConnection(x)) {
        syslog(LOG_ERR, "Send thread %d (index %lu broker %ld) starting pause "
               "due to lost TCP connection", static_cast<int>(Gettid()),
               static_cast<unsigned long>(MyBrokerIndex), broker_id);
        SenderSocketError.Increment();
        Ds.PauseButton.Push();
        return TSockOpResult::Error;
      }

      throw;  // anything else is fatal
    }

    assert(bytes_sent <= SendBuf.size());

    if (bytes_sent == SendBuf.size()) {
      break;
    }

    if (!WaitForSendReady()) {
      syslog(LOG_ERR, "Send thread %d (index %lu broker %ld) starting pause "
             "due to socket timeout", static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex), broker_id);
      SenderSocketTimeout.Increment();
      Ds.PauseButton.Push();
      return TSockOpResult::Error;
    }
  }

  return (OptInProgressShutdown.IsKnown() &&
          (OptInProgressShutdown->FastShutdown ||
           (GetEpochMilliseconds() >= OptInProgressShutdown->Deadline))) ?
      TSockOpResult::OkStop : TSockOpResult::OkContinue;
}

/* Create and send a single produce request. */
TSender::TSockOpResult TSender::HandleSockWriteReady() {
  assert(this);
  assert(CurrentRequest.IsUnknown());
  CurrentRequest = RequestFactory.BuildRequest(SendBuf);

  if (CurrentRequest.IsUnknown()) {
    assert(false);
    syslog(LOG_ERR, "Bug!!! Produce request is empty");
    BugProduceRequestEmpty.Increment();
    return TSockOpResult::OkContinue;
  }

  TSockOpResult send_result = SendOneProduceRequest();

  switch (send_result) {
    case TSockOpResult::OkContinue: {
      break;
    }
    case TSockOpResult::OkStop: {
      syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) finishing "
             "after send during shutdown", static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex),
             static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()));
      break;
    }
    case TSockOpResult::Error: {
      syslog(LOG_ERR, "Send thread %d (index %lu broker %ld) finishing due to "
             "socket error on send", static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex),
             static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()));

      /* Socket error on sttempted send: pause has been initiated.  Leave
         'CurrentRequest' in place, and the messages it contains will be
         rerouted once we have new metadata and the dispatcher has been
         restarted. */
      return send_result;
    }
    NO_DEFAULT_CASE;
  }

  SendProduceRequestOk.Increment();
  TAllTopics &all_topics = CurrentRequest->second;

  for (auto &topic_elem : all_topics) {
    TMultiPartitionGroup &group = topic_elem.second;

    for (auto &msg_set_elem : group) {
      Ds.MsgStateTracker.MsgEnterAckWait(msg_set_elem.second.Contents);
      DebugLogger.LogMsgList(msg_set_elem.second.Contents);
    }
  }

  /* Forward request to receive thread for ACK wait. */
  Cs.SendFinishedQueue.Put(std::move(*CurrentRequest));
  CurrentRequest.Reset();

  return send_result;
}

void TSender::DoRun() {
  assert(this);
  ShutdownStatus = TShutdownStatus::Error;
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();

  if (!ConnectToBroker()) {
    return;
  }

  for (; ; ) {
    if (OptInProgressShutdown.IsKnown() && RequestFactory.IsEmpty()) {
      ShutdownStatus = TShutdownStatus::Normal;
      syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) finishing "
             "after emptying its queue on shutdown", static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex), broker_id);
      break;
    }

    InitMainLoopPollArray();
    uint64_t start_time = GetEpochMilliseconds();
    int shutdown_timeout = ComputeShutdownTimeout(start_time);

    if (shutdown_timeout == 0) {
      /* Stop doing work immediately even if FDs are ready. */
      ShutdownStatus = TShutdownStatus::Normal;
      syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) finishing on "
             "shutdown time limit expiration 1", static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex), broker_id);
      break;
    }

    /* Don't check for EINTR, since this thread has signals masked. */
    int ret = IfLt0(poll(MainLoopPollArray, MainLoopPollArray.Size(),
        ComputeMainLoopPollTimeout(start_time, shutdown_timeout)));

    /* Handle possibly nonmonotonic clock. */
    uint64_t finish_time = std::max(start_time, GetEpochMilliseconds());

    if (ret == 0) {  // poll() timed out
      if ((MainLoopPollArray[TMainLoopPollItem::SockWrite].fd >= 0) &&
          ((finish_time - start_time) >=
           (Ds.Config.KafkaSocketTimeout * 1000))) {
        syslog(LOG_ERR, "Send thread %d (index %lu broker %ld) starting pause "
               "due to socket timeout in main loop",
               static_cast<int>(Gettid()),
               static_cast<unsigned long>(MyBrokerIndex), broker_id);
        SenderSocketTimeout.Increment();
        Ds.PauseButton.Push();
        break;
      }

      if (OptInProgressShutdown.IsKnown() &&
          (finish_time >= OptInProgressShutdown->Deadline)) {
        ShutdownStatus = TShutdownStatus::Normal;
        syslog(LOG_NOTICE, "Send thread %d (index %lu broker %ld) finishing "
               "on shutdown time limit expiration 2",
               static_cast<int>(Gettid()),
               static_cast<unsigned long>(MyBrokerIndex), broker_id);
        break;
      }

      CheckInputQueue(false);
      continue;
    }

    if (MainLoopPollArray[TMainLoopPollItem::ShutdownRequest].revents) {
      if (!HandleShutdownRequest(false)) {
        ShutdownStatus = TShutdownStatus::Normal;
        break;
      }

      continue;  // recompute timeout before doing more work
    }

    if (MainLoopPollArray[TMainLoopPollItem::PauseButton].revents) {
      HandlePauseDetected(false);
      ShutdownStatus = TShutdownStatus::Normal;
      break;
    }

    if (MainLoopPollArray[TMainLoopPollItem::ResendQueue].revents) {
      SenderHandleResendRequest.Increment();
      RequestFactory.PutFront(Cs.ResendQueue.Get());
    }

    if (MainLoopPollArray[TMainLoopPollItem::InputQueue].revents) {
      CheckInputQueue(true);
    }

    if (MainLoopPollArray[TMainLoopPollItem::SockWrite].revents) {
      TSockOpResult result = HandleSockWriteReady();

      if (result != TSockOpResult::OkContinue) {
        if (result != TSockOpResult::Error) {
          ShutdownStatus = TShutdownStatus::Normal;
        }

        break;
      }
    }
  }
}
