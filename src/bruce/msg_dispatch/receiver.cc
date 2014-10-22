/* <bruce/msg_dispatch/receiver.cc>

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

   Implements <bruce/msg_dispatch/receiver.h>.
 */

#include <bruce/msg_dispatch/receiver.h>

#include <cstring>
#include <exception>
#include <stdexcept>
#include <string>

#include <sys/socket.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include <base/error_utils.h>
#include <base/gettid.h>
#include <base/no_default_case.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/util/msg_util.h>
#include <bruce/util/system_error_codes.h>
#include <bruce/util/time_util.h>
#include <server/counter.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;
using namespace Bruce::Debug;
using namespace Bruce::KafkaProto;
using namespace Bruce::MsgDispatch;
using namespace Bruce::Util;

SERVER_COUNTER(BadProduceResponse);
SERVER_COUNTER(BadProduceResponseSize);
SERVER_COUNTER(CorrelationIdMismatch);
SERVER_COUNTER(DiscardOnFailedDeliveryAttemptLimit);
SERVER_COUNTER(ProduceResponseShortPartitionList);
SERVER_COUNTER(ProduceResponseShortTopicList);
SERVER_COUNTER(ProduceResponseUnexpectedPartition);
SERVER_COUNTER(ProduceResponseUnexpectedTopic);
SERVER_COUNTER(ReceiveBufCheckSpace);
SERVER_COUNTER(ReceiveBufGrow);
SERVER_COUNTER(ReceiveBufMove);
SERVER_COUNTER(ReceiverCheckSendFinishedQueue);
SERVER_COUNTER(ReceiverDoSocketRead);
SERVER_COUNTER(ReceiverFinishRun);
SERVER_COUNTER(ReceiverFinishWaitForConnect);
SERVER_COUNTER(ReceiverFinishWaitShutdownAck);
SERVER_COUNTER(ReceiverSocketBrokerClose);
SERVER_COUNTER(ReceiverSocketError);
SERVER_COUNTER(ReceiverSocketReadSuccess);
SERVER_COUNTER(ReceiverSocketTimeout);
SERVER_COUNTER(ReceiverSocketTimeoutUnknown);
SERVER_COUNTER(ReceiverStartFastShutdown);
SERVER_COUNTER(ReceiverStartRun);
SERVER_COUNTER(ReceiverStartSlowShutdown);
SERVER_COUNTER(ReceiverStartWaitForConnect);
SERVER_COUNTER(ReceiverStartWaitShutdownAck);
SERVER_COUNTER(ReceiveThreadGotDiscardAck);
SERVER_COUNTER(ReceiveThreadGotDiscardAndPauseAck);
SERVER_COUNTER(ReceiveThreadGotErrorProduceResponse);
SERVER_COUNTER(ReceiveThreadGotOkProduceResponse);
SERVER_COUNTER(ReceiveThreadGotPauseAck);
SERVER_COUNTER(ReceiveThreadGotResendAck);
SERVER_COUNTER(ReceiveThreadGotSuccessfulAck);
SERVER_COUNTER(ReceiveThreadQueueResendList);

TReceiver::TReceiver(size_t my_broker_index, TDispatcherSharedState &ds,
                     TConnectionSharedState &cs,
                     const Base::TFd &sender_shutdown_wait)
    : MyBrokerIndex(my_broker_index),
      Ds(ds),
      Cs(cs),
      SenderShutdownWait(sender_shutdown_wait),
      SizeFieldSize(ds.KafkaProtocol.GetBytesNeededToGetResponseSize()),
      ResponseReader(ds.KafkaProtocol.CreateProduceResponseReader()),
      /* Set initial buffer size.  Buffer may grow, but will never shrink. */
      ReceiveBuf(64 * 1024, 0),
      ReceiveBufDataOffset(0),
      ReceiveBufDataSize(0),
      FullResponseInReceiveBuf(false),
      PauseInProgress(false),
      SendThreadTerminated(false),
      Destroying(false),
      ShutdownStatus(TShutdownStatus::Normal),
      DebugLogger(ds.DebugSetup, TDebugSetup::TLogId::MSG_GOT_ACK,
                  !ds.Config.OmitTimestamp, ds.Config.UseOldOutputFormat) {
}

TReceiver::~TReceiver() noexcept {
  /* This will shut down the thread if something unexpected happens.  Setting
     the 'Destroying' flag tells the thread to shut down immediately when it
     gets the shutdown request. */
  Destroying = true;
  ShutdownOnDestroy();
}

void TReceiver::StartSlowShutdown(uint64_t start_time) {
  assert(this);
  assert(IsStarted());
  assert(!OptShutdownCmd.IsKnown());
  ReceiverStartSlowShutdown.Increment();
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Sending slow shutdown request to receive thread (index "
         "%lu broker %ld)", static_cast<unsigned long>(MyBrokerIndex),
         broker_id);
  OptShutdownCmd.MakeKnown(start_time);
  RequestShutdown();
}

void TReceiver::StartFastShutdown() {
  assert(this);
  assert(IsStarted());
  assert(!OptShutdownCmd.IsKnown());
  ReceiverStartFastShutdown.Increment();
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Sending fast shutdown request to receive thread (index "
         "%lu broker %ld)", static_cast<unsigned long>(MyBrokerIndex),
         broker_id);
  OptShutdownCmd.MakeKnown();
  RequestShutdown();
}

void TReceiver::WaitForShutdownAck() {
  assert(this);
  ReceiverStartWaitShutdownAck.Increment();
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Waiting for shutdown ACK from receive thread (index %lu "
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
  syslog(LOG_NOTICE, "Got %s from receive thread (index %lu broker %ld)",
         blurb, static_cast<unsigned long>(MyBrokerIndex), broker_id);
  ReceiverFinishWaitShutdownAck.Increment();
  OptShutdownCmd.Reset();
}

void TReceiver::ExtractMsgs() {
  assert(this);

  /* The order in which we move remaining messages to Cs.AckWaitAfterShutdown
     matters because we want to avoid getting messages unnecessarily out of
     order. */

  std::list<std::list<TMsg::TPtr>> &aw = Cs.AckWaitAfterShutdown;
  aw.splice(aw.end(), std::move(RerouteAfterPause));

  for (TProduceRequest &request : AckWaitQueue) {
    EmptyAllTopics(request.second, aw);
  }

  /* After emptying out the receiver, don't bother reinitializing it to a newly
     constructed state.  It will be destroyed and recreated before the
     dispatcher restarts. */

  Metadata.reset();
  assert(!Destroying);
  ShutdownStatus = TShutdownStatus::Normal;
}

void TReceiver::Run() {
  assert(this);
  assert(Metadata);
  ReceiverStartRun.Increment();
  long broker_id = ~0;

  try {
    assert(MyBrokerIndex < Metadata->GetBrokers().size());
    broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
    syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) started",
           static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id);
    DoRun();
  } catch (const TShutdownOnDestroy &) {
    /* Nothing to do here. */
  } catch (const std::exception &x) {
    syslog(LOG_ERR, "Fatal error in receive thread %d (index %lu broker %ld): "
           "%s", static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id, x.what());
    _exit(EXIT_FAILURE);
  } catch (...) {
    syslog(LOG_ERR, "Fatal unknown error in receive thread %d (index %lu "
           "broker %ld)", static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id);
    _exit(EXIT_FAILURE);
  }

  syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) finished %s",
         static_cast<int>(Gettid()), static_cast<unsigned long>(MyBrokerIndex),
         broker_id, (ShutdownStatus == TShutdownStatus::Normal) ?
                    "normally" : "on error");
  Ds.MarkThreadFinished();
  ReceiverFinishRun.Increment();
}

bool TReceiver::WaitForConnect() {
  assert(this);
  ReceiverStartWaitForConnect.Increment();
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) start wait for "
         "connect", static_cast<int>(Gettid()),
         static_cast<unsigned long>(MyBrokerIndex), broker_id);
  Cs.ConnectFinished.Pop();
  syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) finish wait "
         "for connect", static_cast<int>(Gettid()),
         static_cast<unsigned long>(MyBrokerIndex), broker_id);
  bool success = Cs.Sock.IsOpen();

  if (!success) {
    syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) finishing "
           "because send thread failed to connect", static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id);
  }

  ReceiverFinishWaitForConnect.Increment();
  return success;
}

void TReceiver::InitMainLoopPollArray() {
  struct pollfd &sock_item = MainLoopPollArray[TMainLoopPollItem::SockRead];
  struct pollfd &shutdown_item =
      MainLoopPollArray[TMainLoopPollItem::ShutdownRequest];
  struct pollfd &pause_item =
      MainLoopPollArray[TMainLoopPollItem::PauseButton];
  struct pollfd &send_finished_item =
      MainLoopPollArray[TMainLoopPollItem::SendFinishedQueue];
  struct pollfd &send_thread_terminated_item =
      MainLoopPollArray[TMainLoopPollItem::SendThreadTerminated];
  sock_item.fd = (AckWaitQueue.empty() || FullResponseInReceiveBuf) ?
      -1 : int(Cs.Sock);
  sock_item.events = POLLIN;
  sock_item.revents = 0;
  shutdown_item.fd = GetShutdownRequestFd();
  shutdown_item.events = POLLIN;
  shutdown_item.revents = 0;
  pause_item.fd = PauseInProgress ? -1 : int(Ds.PauseButton.GetFd());
  pause_item.events = POLLIN;
  pause_item.revents = 0;
  send_finished_item.fd = Cs.SendFinishedQueue.GetMsgAvailableFd();
  send_finished_item.events = POLLIN;
  send_finished_item.revents = 0;
  send_thread_terminated_item.fd = SendThreadTerminated ?
      -1 : int(SenderShutdownWait);
  send_thread_terminated_item.events = POLLIN;
  send_thread_terminated_item.revents = 0;
}

int TReceiver::ComputeMainLoopPollTimeout(uint64_t now) const {
  assert(this);
  int timeout = AckWaitQueue.empty() ?
      -1 : static_cast<int>(Ds.Config.KafkaSocketTimeout * 1000);

  if (OptInProgressShutdown.IsKnown()) {
    uint64_t shutdown_deadline = OptInProgressShutdown->Deadline;
    int shutdown_timeout = (now > shutdown_deadline) ?
        0 : (shutdown_deadline - now);
    timeout = (timeout < 0) ?
        shutdown_timeout : std::min(timeout, shutdown_timeout);
  }

  return timeout;
}

void TReceiver::SetFastShutdownState() {
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

void TReceiver::HandlePauseDetected() {
  assert(this);
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
  syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) detected "
         "pause: starting fast shutdown", static_cast<int>(Gettid()),
         static_cast<unsigned long>(MyBrokerIndex), broker_id);
  PauseInProgress = true;
  SetFastShutdownState();
}

void TReceiver::HandleShutdownRequest() {
  assert(this);

  if (Destroying) {
    throw TShutdownOnDestroy();
  }

  assert(OptShutdownCmd.IsKnown());
  const TShutdownCmd &cmd = *OptShutdownCmd;
  bool is_fast = cmd.OptSlowShutdownStartTime.IsUnknown();

  if (is_fast) {
    SetFastShutdownState();
  } else {
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
  syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) sending ACK "
         "for %s shutdown", static_cast<int>(Gettid()),
         static_cast<unsigned long>(MyBrokerIndex), broker_id,
         is_fast ? "fast" : "slow");
  ShutdownAck.Push();
  ClearShutdownRequest();
}

void TReceiver::MakeSpaceInBuf(
    size_t min_size) {
  assert(this);
  ReceiveBufCheckSpace.Increment();

  if (ReceiveBufDataOffset > ReceiveBuf.size()) {
    /* better than trashing memory */
    throw std::logic_error("ReceiveBufDataOffset > ReceiveBuf.size() in "
                           "TReceiver::MakeSpaceInBuf()");
  }

  size_t bytes_to_end = ReceiveBuf.size() - ReceiveBufDataOffset;

  if (ReceiveBufDataSize > bytes_to_end) {
    /* better than trashing memory */
    throw std::logic_error("ReceiveBufDataSize > bytes_to_end in "
                           "TReceiver::MakeSpaceInBuf()");
  }

  /* The second part of the "if" condition is a heuristic that tries to avoid
     doing lots of little reads while avoiding excessive time spent moving data
     in memory. */
  if ((min_size > bytes_to_end) ||
      (ReceiveBufDataOffset >= (ReceiveBuf.size() / 2))) {
    ReceiveBufMove.Increment();
    assert(ReceiveBuf.size() >= ReceiveBufDataOffset);
    std::memmove(&ReceiveBuf[0], &ReceiveBuf[0] + ReceiveBufDataOffset,
                 ReceiveBufDataSize);
    ReceiveBufDataOffset = 0;

    if (min_size > ReceiveBuf.size()) {
      ReceiveBufGrow.Increment();
      ReceiveBuf.resize(min_size);
    }
  }

  assert(min_size <= (ReceiveBuf.size() - ReceiveBufDataOffset));
}

bool TReceiver::DoSockRead(size_t min_size) {
  assert(this);
  assert(min_size);
  ReceiverDoSocketRead.Increment();
  MakeSpaceInBuf(min_size);
  size_t read_offset = ReceiveBufDataOffset + ReceiveBufDataSize;

  if (read_offset > ReceiveBuf.size()) {
    /* better than trashing memory */
    throw std::logic_error("read_offset > ReceiveBuf.size() in "
                           "TReceiver::DoSockRead()");
  }

  size_t bytes_to_read = ReceiveBuf.size() - read_offset;
  ssize_t result = 0;

  try {
    result = IfLt0(recv(Cs.Sock, &ReceiveBuf[0] + read_offset, bytes_to_read,
                        0));
  } catch (const std::system_error &x) {
    if (LostTcpConnection(x)) {
      long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
      syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) lost TCP "
             "connection on attempted read", static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex), broker_id);
      ReceiverSocketError.Increment();
      return false;
    }

    throw;  // anything else is fatal
  }

  if (result == 0) {
    long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
    syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) detected TCP "
           "connection unexpectedly closed by broker on attempted read",
           static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id);
    ReceiverSocketBrokerClose.Increment();
    return false;
  }

  /* Read was successful, although the amount of data obtained may be less than
     what the caller hoped for. */
  ReceiveBufDataSize += result;
  ReceiverSocketReadSuccess.Increment();
  return true;
}

bool TReceiver::TryReadProduceResponses() {
  assert(this);
  bool did_read = false;

  if (ReceiveBufDataSize < SizeFieldSize) {
    if (!DoSockRead(SizeFieldSize - ReceiveBufDataSize)) {
      return false;  // socket error
    }

    did_read = true;

    if (ReceiveBufDataSize < SizeFieldSize) {
      return true;  // still not enough data: try again later
    }
  }

  size_t response_size =
      Ds.KafkaProtocol.GetResponseSize(&ReceiveBuf[ReceiveBufDataOffset]);

  if ((ReceiveBufDataSize < response_size) && !did_read &&
       !DoSockRead(response_size - ReceiveBufDataSize)) {
    return false;  // socket error
  }

  /* Ok, we made our best attempt to get enough data for a produce request
     without blocking.  Return true to indicate that no error occurred.  Our
     caller will determine whether there is now enough data, and act
     appropriately. */
  return true;
}

void TReceiver::ReportBadResponseTopic(const std::string &topic) const {
  assert(this);
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) got produce "
           "response with unexpected topic [%s]", static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex),
           static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()),
           topic.c_str());
  }

  ProduceResponseUnexpectedTopic.Increment();
}

void TReceiver::ReportBadResponsePartition(int32_t partition) const {
  assert(this);
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) got produce "
           "response with unexpected partition: %d",
           static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex),
           static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()),
           static_cast<int>(partition));
  }

  ProduceResponseUnexpectedPartition.Increment();
}

void TReceiver::ReportShortResponsePartitionList(
    const std::string &topic) const {
  assert(this);
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) got produce "
           "response with short partition list for topic [%s]",
           static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex),
           static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()),
           topic.c_str());
  }

  ProduceResponseShortPartitionList.Increment();
}

void TReceiver::ReportShortResponseTopicList() const {
  assert(this);
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) got produce "
           "response with short topic list", static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex),
           static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()));
  }

  ProduceResponseShortTopicList.Increment();
}

void TReceiver::ProcessResendList(
    std::list<std::list<TMsg::TPtr>> &&resend_list) {
  assert(this);

  if (resend_list.empty()) {
    return;
  }

  ReceiveThreadQueueResendList.Increment();
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) queueing msg "
           "set list for resend", static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex),
           static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()));
  }

  Ds.MsgStateTracker.MsgEnterSendWait(resend_list);
  Cs.ResendQueue.Put(std::move(resend_list));
}

void TReceiver::ProcessPauseAndResendMsgSet(std::list<TMsg::TPtr> &msg_set,
    const std::string &topic) {
  assert(this);

  for (auto iter = msg_set.begin(), next = iter;
       iter != msg_set.end();
       iter = next) {
    ++next;
    TMsg::TPtr &msg = *iter;
    assert(msg->GetTopic() == topic);

    if (msg->CountFailedDeliveryAttempt() >
        Ds.Config.MaxFailedDeliveryAttempts) {
      DiscardOnFailedDeliveryAttemptLimit.Increment();

      if (!Ds.Config.NoLogDiscard) {
        static TLogRateLimiter lim(std::chrono::seconds(30));

        if (lim.Test()) {
          syslog(LOG_ERR, "Discarding message because failed delivery attempt "
                 "limit reached (topic: [%s])", msg->GetTopic().c_str());
        }
      }

      Ds.Discard(std::move(msg),
          TAnomalyTracker::TDiscardReason::FailedDeliveryAttemptLimit);
      msg_set.erase(iter);
    } else {
      /* When the router thread reroutes this message after restarting the
         dispatcher, don't report the message as a possible duplicate (since we
         got an error ACK). */
      msg->SetErrorAckReceived(true);
    }
  }
}

bool TReceiver::ProcessOneAck(std::list<TMsg::TPtr> &msg_set, int16_t ack,
    const std::string &topic, int32_t partition) {
  assert(this);
  assert(!msg_set.empty());
  Ds.IncrementAckCount();

  switch (Ds.KafkaProtocol.ProcessAck(ack, topic, partition)) {
    case TAction::Ok: {  // got successful ACK
      ReceiveThreadGotSuccessfulAck.Increment();
      DebugLogger.LogMsgList(msg_set);
      Ds.MsgStateTracker.MsgEnterProcessed(msg_set);
      msg_set.clear();
      break;
    }
    case TAction::Resend: {
      ReceiveThreadGotResendAck.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) got ACK "
            "error that triggers resend", static_cast<int>(Gettid()),
            static_cast<unsigned long>(MyBrokerIndex),
            static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()));
      }

      /* Leave contents of 'msg_set' alone.  Our caller will handle the details
         of resending the messages. */
      break;
    }
    case TAction::Discard: {
      ReceiveThreadGotDiscardAck.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        /* Write a syslog message even if Ds.Config.NoLogDiscard is true
           because these events are always interesting enough to be worth
           logging. */
        syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) got ACK "
            "error that triggers discard without pause: topic [%s], %lu "
            "messages in set with total data size %lu",
            static_cast<int>(Gettid()),
            static_cast<unsigned long>(MyBrokerIndex),
            static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()),
            msg_set.front()->GetTopic().c_str(),
            static_cast<unsigned long>(msg_set.size()), GetDataSize(msg_set));
      }

      Ds.Discard(std::move(msg_set),
                 TAnomalyTracker::TDiscardReason::KafkaErrorAck);
      break;
    }
    case TAction::Pause: {
      ReceiveThreadGotPauseAck.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) got ACK "
            "error that triggers pause", static_cast<int>(Gettid()),
            static_cast<unsigned long>(MyBrokerIndex),
            static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()));
      }

      /* Messages may be discarded here due to the failed delivery attempt
         limit.  Our caller will arrange for any remaining messages to be
         rerouted after the dispatcher has been restarted. */
      ProcessPauseAndResendMsgSet(msg_set, topic);
      return false;
    }
    case TAction::DiscardAndPause: {
      ReceiveThreadGotDiscardAndPauseAck.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        /* Write a syslog message even if Ds.Config.NoLogDiscard is true
           because these events are always interesting enough to be worth
           logging. */
        syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) got ACK "
            "error that triggers discard and pause",
            static_cast<int>(Gettid()),
            static_cast<unsigned long>(MyBrokerIndex),
            static_cast<long>(Metadata->GetBrokers()[MyBrokerIndex].GetId()));
      }

      Ds.Discard(std::move(msg_set),
                 TAnomalyTracker::TDiscardReason::KafkaErrorAck);
      return false;
    }
    NO_DEFAULT_CASE;
  }

  return true;
}

bool TReceiver::ProcessResponseAcks(TProduceRequest &request) {
  assert(this);
  assert(RerouteAfterPause.empty());
  std::list<std::list<TMsg::TPtr>> resend_list;
  std::string topic;
  bool will_pause = false;
  bool bad_response = false;
  TAllTopics &all_topics = request.second;

  while (ResponseReader->NextTopic()) {
    topic.assign(ResponseReader->GetCurrentTopicNameBegin(),
                 ResponseReader->GetCurrentTopicNameEnd());
    auto topic_iter = all_topics.find(topic);

    if (topic_iter == all_topics.end()) {
      ReportBadResponseTopic(topic);
      bad_response = true;
      break;
    }

    TMultiPartitionGroup &all_partitions = topic_iter->second;

    while (ResponseReader->NextPartitionInTopic()) {
      int32_t partition = ResponseReader->GetCurrentPartitionNumber();
      auto partition_iter = all_partitions.find(partition);

      if (partition_iter == all_partitions.end()) {
        ReportBadResponsePartition(partition);
        bad_response = true;
        break;
      }

      std::list<TMsg::TPtr> &msg_set = partition_iter->second.Contents;
      assert(!msg_set.empty());

      if (!ProcessOneAck(msg_set,
                         ResponseReader->GetCurrentPartitionErrorCode(), topic,
                         partition)) {
        will_pause = true;  // we will pause, but keep processing ACKs
      }

      if (!msg_set.empty()) {
        resend_list.push_back(std::move(msg_set));
      }

      all_partitions.erase(partition_iter);
    }

    if (!all_partitions.empty()) {
      ReportShortResponsePartitionList(topic);
      bad_response = true;
      break;
    }

    all_topics.erase(topic_iter);
  }

  if (!bad_response && !all_topics.empty()) {
    bad_response = true;
    ReportShortResponseTopicList();
  }

  if (bad_response) {
    will_pause = true;
    EmptyAllTopics(all_topics, resend_list);
  }

  assert(all_topics.empty());

  if (will_pause) {
    /* We will be starting a pause.  The pause handling code in the router
       thread will reroute these messages after the dispatcher has restarted
       with new metadata. */
    RerouteAfterPause = std::move(resend_list);
    return false;
  }

  ProcessResendList(std::move(resend_list));
  assert(resend_list.empty());
  return true;
}

bool TReceiver::ProcessSingleProduceResponse(size_t response_size) {
  assert(this);
  assert(RerouteAfterPause.empty());
  assert(!AckWaitQueue.empty());
  TProduceRequest request(std::move(AckWaitQueue.front()));
  AckWaitQueue.pop_front();
  assert(ReceiveBuf.size() >= ReceiveBufDataOffset);
  assert((ReceiveBuf.size() - ReceiveBufDataOffset) >= response_size);
  ResponseReader->SetResponse(&ReceiveBuf[0] + ReceiveBufDataOffset,
                              response_size);
  TCorrId corr_id = ResponseReader->GetCorrelationId();

  if (corr_id != request.first) {
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
      syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) got "
           "correlation ID mismatch: expected %ld actual %ld",
           static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id,
           static_cast<long>(request.first), static_cast<long>(corr_id));
    }

    CorrelationIdMismatch.Increment();

    /* We will be starting a pause.  The pause handling code in the router
       thread will reroute these messages after the dispatcher has restarted
       with new metadata. */
    EmptyAllTopics(request.second, RerouteAfterPause);
    return false;
  }

  bool pause_not_needed = ProcessResponseAcks(request);
  assert(request.second.empty());
  assert(RerouteAfterPause.empty() || !pause_not_needed);
  return pause_not_needed;
}

bool TReceiver::TryProcessProduceResponses() {
  assert(this);
  assert(!AckWaitQueue.empty());

  for (; ; ) {
    if (ReceiveBufDataSize < SizeFieldSize) {
      FullResponseInReceiveBuf = false;
      break;
    }

    /* TODO: Add code to guard against a ridiculously large response size field
       written by a buggy Kafka broker. */
    size_t response_size =
        Ds.KafkaProtocol.GetResponseSize(&ReceiveBuf[ReceiveBufDataOffset]);

    if (response_size > ReceiveBufDataSize) {
      FullResponseInReceiveBuf = false;
      break;
    }

    FullResponseInReceiveBuf = true;

    if (AckWaitQueue.empty()) {
      /* We have at least one full produce response, but no more sent requests
         from the send thread.  Wait until we get the next sent request. */
      break;
    }

    if (!ProcessSingleProduceResponse(response_size)) {
      ReceiveThreadGotErrorProduceResponse.Increment();
      return false;  // error occurred
    }

    ReceiveThreadGotOkProduceResponse.Increment();

    /* Mark produce response as consumed. */
    ReceiveBufDataOffset += response_size;
    ReceiveBufDataSize -= response_size;
  }

  return true;
}

/* Attempt a single large read (possibly more bytes than a single produce
   response will require).  Then consider the following cases:

       Case 1: We got a socket error.  Return false to notify the main loop
           that an error occurred.

       Case 2: While processing the response data, at some point we either
           found something invalid in the response or got an error ACK
           indicating the need for new metadata.  In this case, return false to
           notify the main loop of the error.  If a response was partially
           processed when the error was detected, we will leave behind enough
           state that things can be sorted out once the dispatcher has finished
           shutting down in preparation for the metadata update.

       Case 3: We got some data that looks valid at first glance, but there is
           not enough to complete a produce response.  Leave the data we got in
           the buffer and return true (indicating no error).  The main loop
           will call us again when it detects that the socket is ready.

       Case 4: We got enough data to complete at least one produce response,
           and encountered no serious errors while processing it.  In this
           case, we process the data in the buffer (possibly multiple produce
           responses) until there is not enough left for another complete
           produce response.  Then return true to indicate no error.  The main
           loop will call us again when appropriate. */
bool TReceiver::HandleSockReadReady() {
  assert(this);
  assert(!AckWaitQueue.empty());
  assert(!FullResponseInReceiveBuf);

  try {
    if (!TryReadProduceResponses() || !TryProcessProduceResponses()) {
      long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
      syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) starting "
             "pause due to error reading or processing produce responses",
             static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex), broker_id);
      Ds.PauseButton.Push();
      return false;
    }
  } catch (const TWireProtocol::TBadResponseSize &x) {
    long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
    syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) starting pause "
           "due to unexpected response from Kafka: %s",
           static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id, x.what());
    BadProduceResponseSize.Increment();
    Ds.PauseButton.Push();
    return false;
  } catch (const TProduceResponseReaderApi::TBadProduceResponse &x) {
    long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();
    syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) starting pause "
           "due to unexpected response from Kafka: %s",
           static_cast<int>(Gettid()),
           static_cast<unsigned long>(MyBrokerIndex), broker_id, x.what());
    BadProduceResponse.Increment();
    Ds.PauseButton.Push();
    return false;
  }

  return true;  // no error
}

void TReceiver::DoRun() {
  assert(this);
  ShutdownStatus = TShutdownStatus::Error;
  long broker_id = Metadata->GetBrokers()[MyBrokerIndex].GetId();

  if (!WaitForConnect()) {
    return;
  }

  do {
    if (SendThreadTerminated && AckWaitQueue.empty()) {
      ShutdownStatus = TShutdownStatus::Normal;
      syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) finishing "
             "after emptying its queue on shutdown",
             static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex), broker_id);
      break;
    }

    InitMainLoopPollArray();
    uint64_t start_time = GetEpochMilliseconds();
    int timeout = ComputeMainLoopPollTimeout(start_time);

    /* Don't check for EINTR, since this thread has signals masked. */
    int ret = IfLt0(poll(MainLoopPollArray, MainLoopPollArray.Size(),
                         timeout));

    /* Handle possibly nonmonotonic clock. */
    uint64_t finish_time = std::max(start_time, GetEpochMilliseconds());

    if (ret == 0) {  // poll() timed out
      if ((MainLoopPollArray[TMainLoopPollItem::SockRead].fd >= 0) &&
          ((finish_time - start_time) >=
           (Ds.Config.KafkaSocketTimeout * 1000))) {
        syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) starting "
               "pause due to socket timeout in main loop",
               static_cast<int>(Gettid()),
               static_cast<unsigned long>(MyBrokerIndex), broker_id);
        ReceiverSocketTimeout.Increment();
        Ds.PauseButton.Push();
        break;
      }

      if (OptInProgressShutdown.IsKnown() &&
          (finish_time >= OptInProgressShutdown->Deadline)) {
        ShutdownStatus = TShutdownStatus::Normal;
        syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) "
               "finishing on shutdown time limit expiration",
               static_cast<int>(Gettid()),
               static_cast<unsigned long>(MyBrokerIndex), broker_id);
        break;
      }

      /* A nonmonotonic clock can cause this behavior. */
      syslog(LOG_WARNING, "Receive thread %d (index %lu broker %ld) got "
             "socket timeout for unknown reason in main loop (possibly due to "
             "nonmonotonic system clock)",
             static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex), broker_id);
      ReceiverSocketTimeoutUnknown.Increment();
      continue;
    }

    if (MainLoopPollArray[TMainLoopPollItem::ShutdownRequest].revents) {
      HandleShutdownRequest();
    }

    if (MainLoopPollArray[TMainLoopPollItem::PauseButton].revents) {
      HandlePauseDetected();
      /* Keep executing until (we detect send thread termination and run out of
         sent requests to get responses for) or (fast shutdown time limit is
         reached).  When the send thread detects the pause event, it will
         finish sending any request it is currently sending and then terminate
         immediately. */
    }

    if (MainLoopPollArray[TMainLoopPollItem::SendFinishedQueue].revents) {
      ReceiverCheckSendFinishedQueue.Increment();
      AckWaitQueue.splice(AckWaitQueue.end(), Cs.SendFinishedQueue.Get());

      if (!AckWaitQueue.empty() && FullResponseInReceiveBuf &&
          !TryProcessProduceResponses()) {
        syslog(LOG_ERR, "Receive thread %d (index %lu broker %ld) starting "
               "pause due to error previously detected while processing "
               "produce response data", static_cast<int>(Gettid()),
               static_cast<unsigned long>(MyBrokerIndex), broker_id);
        Ds.PauseButton.Push();
        break;  // got error ACK or invalid response from Kafka
      }
    }

    if (MainLoopPollArray[TMainLoopPollItem::SendThreadTerminated].revents) {
      SendThreadTerminated = true;
      syslog(LOG_NOTICE, "Receive thread %d (index %lu broker %ld) detected "
             "send thread termination", static_cast<int>(Gettid()),
             static_cast<unsigned long>(MyBrokerIndex), broker_id);
      AckWaitQueue.splice(AckWaitQueue.end(),
          Cs.SendFinishedQueue.NonblockingGet());
    }

    /* Stop iterating if the socket was ready and we either got an error trying
       to read from it or got an invalid response from Kafka. */
  } while (!MainLoopPollArray[TMainLoopPollItem::SockRead].revents ||
           HandleSockReadReady());
}
