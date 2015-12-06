/* <bruce/msg_dispatch/produce_response_processor.cc>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 if(we)
   Copyright 2015 Dave Peterson <dave@dspeterson.com>

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

   Implements <bruce/msg_dispatch/produce_response_processor.h>.
 */

#include <bruce/msg_dispatch/produce_response_processor.h>

#include <syslog.h>

#include <base/gettid.h>
#include <base/no_default_case.h>
#include <bruce/util/msg_util.h>
#include <bruce/util/time_util.h>
#include <server/counter.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MsgDispatch;
using namespace Bruce::Util;

SERVER_COUNTER(ConnectorGotDiscardAck);
SERVER_COUNTER(ConnectorGotDiscardAndPauseAck);
SERVER_COUNTER(ConnectorGotOkProduceResponse);
SERVER_COUNTER(ConnectorGotPauseAck);
SERVER_COUNTER(ConnectorGotResendAck);
SERVER_COUNTER(ConnectorGotSuccessfulAck);
SERVER_COUNTER(ConnectorQueueImmediateResendMsgSet);
SERVER_COUNTER(ConnectorQueueNoAckMsgs);
SERVER_COUNTER(ConnectorQueuePauseAndResendMsgSet);
SERVER_COUNTER(CorrelationIdMismatch);
SERVER_COUNTER(DiscardOnFailedDeliveryAttemptLimit);
SERVER_COUNTER(ProduceResponseShortPartitionList);
SERVER_COUNTER(ProduceResponseShortTopicList);
SERVER_COUNTER(ProduceResponseUnexpectedPartition);
SERVER_COUNTER(ProduceResponseUnexpectedTopic);

TProduceResponseProcessor::TAction
TProduceResponseProcessor::ProcessResponse(TProduceRequest &request,
    uint8_t *response_buf, size_t response_buf_size) {
  assert(this);
  ResponseReader.SetResponse(response_buf, response_buf_size);
  TCorrId corr_id = ResponseReader.GetCorrelationId();

  if (corr_id != request.first) {
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) starting "
          "pause due to correlation ID mismatch: expected %ld actual %ld",
          static_cast<int>(Gettid()), MyBrokerIndex, MyBrokerId,
          static_cast<long>(request.first), static_cast<long>(corr_id));
    }

    CorrelationIdMismatch.Increment();

    /* The pause handling code in the router thread will reroute these messages
       after the dispatcher has restarted with new metadata. */
    ProcessNoAckMsgs(request.second);
    return TAction::PauseAndFinishNow;
  }

  TAction action = ProcessResponseAcks(request);
  assert(request.second.empty());
  return action;
}

void TProduceResponseProcessor::ReportBadResponseTopic(
    const std::string &topic) const {
  assert(this);
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) starting "
        "pause due to produce response with unexpected topic [%s]",
        static_cast<int>(Gettid()), MyBrokerIndex, MyBrokerId, topic.c_str());
  }

  ProduceResponseUnexpectedTopic.Increment();
}

void TProduceResponseProcessor::ReportBadResponsePartition(
    int32_t partition) const {
  assert(this);
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) starting "
        "pause due to produce response with unexpected partition: %d",
        static_cast<int>(Gettid()), MyBrokerIndex, MyBrokerId,
        static_cast<int>(partition));
  }

  ProduceResponseUnexpectedPartition.Increment();
}

void TProduceResponseProcessor::ReportShortResponsePartitionList(
    const std::string &topic) const {
  assert(this);
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) starting "
        "pause due to produce response with short partition list for topic "
        "[%s]", static_cast<int>(Gettid()), MyBrokerIndex, MyBrokerId,
        topic.c_str());
  }

  ProduceResponseShortPartitionList.Increment();
}

void TProduceResponseProcessor::ReportShortResponseTopicList() const {
  assert(this);
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) starting "
        "pause due to produce response with short topic list",
        static_cast<int>(Gettid()), MyBrokerIndex, MyBrokerId);
  }

  ProduceResponseShortTopicList.Increment();
}

void TProduceResponseProcessor::ProcessImmediateResendMsgSet(
    std::list<TMsg::TPtr> &&msg_set, const std::string &topic) {
  assert(this);
  assert(!msg_set.empty());
  ConnectorQueueImmediateResendMsgSet.Increment();
  static TLogRateLimiter lim(std::chrono::seconds(30));

  if (lim.Test()) {
    syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) queueing msg "
        "set (topic: [%s]) for immediate resend", static_cast<int>(Gettid()),
        MyBrokerIndex, MyBrokerId, topic.c_str());
  }

  Ds.MsgStateTracker.MsgEnterSendWait(msg_set);
  ImmediateResendAckMsgs.push_back(std::move(msg_set));
}

void TProduceResponseProcessor::ProcessPauseAndResendMsgSet(
    std::list<TMsg::TPtr> &&msg_set, const std::string &topic) {
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
    }
  }

  if (!msg_set.empty()) {
    ConnectorQueuePauseAndResendMsgSet.Increment();
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) queueing "
          "msg set (topic: [%s]) for resend after pause",
          static_cast<int>(Gettid()), MyBrokerIndex, MyBrokerId,
          topic.c_str());
    }

    Ds.MsgStateTracker.MsgEnterSendWait(msg_set);
    PauseAndResendAckMsgs.push_back(std::move(msg_set));
  }
}

void TProduceResponseProcessor::ProcessNoAckMsgs(TAllTopics &all_topics) {
  assert(this);
  std::list<std::list<TMsg::TPtr>> tmp;
  EmptyAllTopics(all_topics, tmp);

  if (!tmp.empty()) {
    ConnectorQueueNoAckMsgs.Increment();
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) processing "
          "msgs without ACKs after error", static_cast<int>(Gettid()),
          MyBrokerIndex, MyBrokerId);
    }

    Ds.MsgStateTracker.MsgEnterSendWait(tmp);
    MsgsWithoutAcks.splice(MsgsWithoutAcks.end(), std::move(tmp));
  }
}

bool TProduceResponseProcessor::ProcessOneAck(std::list<TMsg::TPtr> &&msg_set,
    int16_t ack, const std::string &topic, int32_t partition) {
  assert(this);
  assert(!msg_set.empty());
  Ds.IncrementAckCount();

  switch (Ds.KafkaProtocol.ProcessAck(ack, topic, partition)) {
    case TAckResultAction::Ok: {  // got successful ACK
      ConnectorGotSuccessfulAck.Increment();
      DebugLogger.LogMsgList(msg_set);
      Ds.MsgStateTracker.MsgEnterProcessed(msg_set);
      msg_set.clear();
      break;
    }
    case TAckResultAction::Resend: {
      ConnectorGotResendAck.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) got ACK "
            "error that triggers immediate resend without pause",
            static_cast<int>(Gettid()), MyBrokerIndex, MyBrokerId);
      }

      /* These messages can be immediately resent without pausing and rerouting
         based on new metadata. */
      ProcessImmediateResendMsgSet(std::move(msg_set), topic);
      break;
    }
    case TAckResultAction::Discard: {
      ConnectorGotDiscardAck.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        /* Write a syslog message even if Ds.Config.NoLogDiscard is true
           because these events are always interesting enough to be worth
           logging. */
        syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) got ACK "
            "error that triggers discard without pause: topic [%s], %lu "
            "messages in set with total data size %lu",
            static_cast<int>(Gettid()), MyBrokerIndex, MyBrokerId,
            msg_set.front()->GetTopic().c_str(),
            static_cast<unsigned long>(msg_set.size()), GetDataSize(msg_set));
      }

      Ds.Discard(std::move(msg_set),
          TAnomalyTracker::TDiscardReason::KafkaErrorAck);
      break;
    }
    case TAckResultAction::Pause: {
      ConnectorGotPauseAck.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) got ACK "
            "error that triggers deferred pause", static_cast<int>(Gettid()),
            MyBrokerIndex, MyBrokerId);
      }

      /* Messages may be discarded here due to the failed delivery attempt
         limit.  Messages not discarded will be rerouted after the dispatcher
         has been restarted. */
      ProcessPauseAndResendMsgSet(std::move(msg_set), topic);
      return false;
    }
    case TAckResultAction::DiscardAndPause: {
      ConnectorGotDiscardAndPauseAck.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        /* Write a syslog message even if Ds.Config.NoLogDiscard is true
           because these events are always interesting enough to be worth
           logging. */
        syslog(LOG_ERR, "Connector thread %d (index %lu broker %ld) got ACK "
            "error that triggers discard and deferred pause",
            static_cast<int>(Gettid()), MyBrokerIndex, MyBrokerId);
      }

      Ds.Discard(std::move(msg_set),
          TAnomalyTracker::TDiscardReason::KafkaErrorAck);
      return false;
    }
    NO_DEFAULT_CASE;
  }

  assert(msg_set.empty());
  return true;
}

TProduceResponseProcessor::TAction
TProduceResponseProcessor::ProcessResponseAcks(TProduceRequest &request) {
  assert(this);
  std::string topic;
  bool got_pause_ack = false;
  bool bad_response = false;
  TAllTopics &all_topics = request.second;

  while (ResponseReader.NextTopic()) {
    topic.assign(ResponseReader.GetCurrentTopicNameBegin(),
        ResponseReader.GetCurrentTopicNameEnd());
    auto topic_iter = all_topics.find(topic);

    if (topic_iter == all_topics.end()) {
      ReportBadResponseTopic(topic);
      bad_response = true;
      break;
    }

    TMultiPartitionGroup &all_partitions = topic_iter->second;

    while (ResponseReader.NextPartitionInTopic()) {
      int32_t partition = ResponseReader.GetCurrentPartitionNumber();
      auto partition_iter = all_partitions.find(partition);

      if (partition_iter == all_partitions.end()) {
        ReportBadResponsePartition(partition);
        bad_response = true;
        break;
      }

      std::list<TMsg::TPtr> &msg_set = partition_iter->second.Contents;
      assert(!msg_set.empty());

      if (!ProcessOneAck(std::move(msg_set),
              ResponseReader.GetCurrentPartitionErrorCode(), topic,
              partition)) {
        got_pause_ack = true;  // we will pause, but keep processing ACKs
      }

      assert(msg_set.empty());
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
    ProcessNoAckMsgs(all_topics);
  }

  assert(all_topics.empty());

  if (bad_response) {
    return TAction::PauseAndFinishNow;
  }

  ConnectorGotOkProduceResponse.Increment();
  return got_pause_ack ? TAction::PauseAndDeferFinish : TAction::KeepRunning;
}
