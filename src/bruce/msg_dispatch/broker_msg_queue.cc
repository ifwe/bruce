/* <bruce/msg_dispatch/broker_msg_queue.cc>

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

   Implements <bruce/msg_dispatch/broker_msg_queue.h>.
 */

#include <bruce/msg_dispatch/broker_msg_queue.h>

#include <algorithm>

#include <bruce/msg_state_tracker.h>
#include <server/counter.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;
using namespace Bruce::MsgDispatch;

SERVER_COUNTER(BrokerMsgQueueNotify);
SERVER_COUNTER(BrokerMsgQueueSkipNotify);
SERVER_COUNTER(CombinedTopicsBatchAnyPartition);
SERVER_COUNTER(CombinedTopicsBatchPartitionKey);
SERVER_COUNTER(NoBatchAnyPartition);
SERVER_COUNTER(NoBatchPartitionKey);
SERVER_COUNTER(PerTopicBatchPartitionKey);

static TOpt<TMsg::TTimestamp>
min_opt_ts(const TOpt<TMsg::TTimestamp> &t1,
    const TOpt<TMsg::TTimestamp> &t2) {
  if (t1.IsKnown() && t2.IsKnown()) {
    return TOpt<TMsg::TTimestamp>(std::min(*t1, *t2));
  }

  if (t1.IsKnown()) {
    return t1;
  }

  if (t2.IsKnown()) {
    return t2;
  }

  return TOpt<TMsg::TTimestamp>();
}

TBrokerMsgQueue::TBrokerMsgQueue(const TGlobalBatchConfig &batch_config,
    TMsgStateTracker &msg_state_tracker)
    : PerTopicBatcher(batch_config.GetPerTopicConfig()),
      CombinedTopicsBatcher(batch_config.GetCombinedTopicsConfig()),
      MsgStateTracker(msg_state_tracker) {
}

void TBrokerMsgQueue::Put(TMsg::TTimestamp now, TMsg::TPtr &&msg) {
  assert(this);
  assert(msg);
  TExpiryStatus per_topic_status, combined_topics_status;
  bool ready_list_empty_initial = false;
  bool ready_list_empty_final = false;

  {
    std::lock_guard<std::mutex> lock(Mutex);
    ready_list_empty_initial = ReadyList.empty();
    TryBatchPerTopic(now, std::move(msg), per_topic_status);

    if (msg) {
      TMsg::TRoutingType routing_type = msg->GetRoutingType();
      TryBatchCombinedTopics(now, std::move(msg), combined_topics_status);

      if (msg) {
        MsgStateTracker.MsgEnterSendWait(*msg);
        std::list<TMsg::TPtr> single_item_list;
        single_item_list.push_back(std::move(msg));
        ReadyList.push_back(std::move(single_item_list));

        if (routing_type == TMsg::TRoutingType::PartitionKey) {
          NoBatchPartitionKey.Increment();
        } else {
          assert(routing_type == TMsg::TRoutingType::AnyPartition);
          NoBatchAnyPartition.Increment();
        }
      } else {
        if (routing_type == TMsg::TRoutingType::PartitionKey) {
          CombinedTopicsBatchPartitionKey.Increment();
        } else {
          assert(routing_type == TMsg::TRoutingType::AnyPartition);
          CombinedTopicsBatchAnyPartition.Increment();
        }
      }
    } else {
      PerTopicBatchPartitionKey.Increment();
      combined_topics_status.OptInitialExpiry =
          CombinedTopicsBatcher.GetNextCompleteTime();
      combined_topics_status.OptFinalExpiry =
          combined_topics_status.OptInitialExpiry;
    }

    ready_list_empty_final = ReadyList.empty();
  }

  bool notify = (ready_list_empty_initial && !ready_list_empty_final);

  if (!notify) {
    TOpt<TMsg::TTimestamp> min_initial_expiry =
        min_opt_ts(per_topic_status.OptInitialExpiry,
                   combined_topics_status.OptInitialExpiry);
    TOpt<TMsg::TTimestamp> min_final_expiry =
        min_opt_ts(per_topic_status.OptFinalExpiry,
                   combined_topics_status.OptFinalExpiry);

    if (min_final_expiry.IsKnown() &&
        (!min_initial_expiry.IsKnown() ||
         (*min_final_expiry < *min_initial_expiry))) {
      notify = true;
    }
  }

  if (notify) {
    BrokerMsgQueueNotify.Increment();
    SenderNotify.Push();
  } else {
    BrokerMsgQueueSkipNotify.Increment();
  }
}

void TBrokerMsgQueue::PutNow(TMsg::TTimestamp now, TMsg::TPtr &&msg) {
  assert(this);
  assert(msg);
  MsgStateTracker.MsgEnterSendWait(*msg);
  std::list<TMsg::TPtr> single_item_list;
  single_item_list.push_back(std::move(msg));
  TExpiryStatus per_topic_status, combined_topics_status;
  bool was_empty = false;

  {
    std::lock_guard<std::mutex> lock(Mutex);
    was_empty = ReadyList.empty();

    /* Transfer any ready batches from the batchers to 'ReadyList'. */
    CheckBothBatchers(now, per_topic_status, combined_topics_status);

    ReadyList.push_back(std::move(single_item_list));
  }

  if (was_empty) {
    BrokerMsgQueueNotify.Increment();
    SenderNotify.Push();
  } else {
    BrokerMsgQueueSkipNotify.Increment();
  }
}

void TBrokerMsgQueue::PutNow(TMsg::TTimestamp now,
    std::list<std::list<TMsg::TPtr>> &&batch) {
  assert(this);

  if (batch.empty()) {
    return;
  }

  MsgStateTracker.MsgEnterSendWait(batch);
  TExpiryStatus per_topic_status, combined_topics_status;
  bool was_empty = false;

  {
    std::lock_guard<std::mutex> lock(Mutex);
    was_empty = ReadyList.empty();

    /* Transfer any ready batches from the batchers to 'ReadyList'. */
    CheckBothBatchers(now, per_topic_status, combined_topics_status);

    ReadyList.splice(ReadyList.end(), std::move(batch));
    assert(!ReadyList.empty());
  }

  if (was_empty) {
    BrokerMsgQueueNotify.Increment();
    SenderNotify.Push();
  } else {
    BrokerMsgQueueSkipNotify.Increment();
  }
}

bool TBrokerMsgQueue::NonblockingGet(TMsg::TTimestamp now,
    TMsg::TTimestamp &next_batch_complete_time,
    std::list<std::list<TMsg::TPtr>> &ready_msgs) {
  assert(this);
  TExpiryStatus per_topic_status, combined_topics_status;

  {
    std::lock_guard<std::mutex> lock(Mutex);

    /* Transfer any ready batches from the batchers to 'ReadyList'. */
    CheckBothBatchers(now, per_topic_status, combined_topics_status);

    ready_msgs.splice(ready_msgs.end(), std::move(ReadyList));
  }

  TOpt<TMsg::TTimestamp> opt_next_expiry =
      min_opt_ts(per_topic_status.OptFinalExpiry,
                 combined_topics_status.OptFinalExpiry);

  if (opt_next_expiry.IsKnown()) {
    next_batch_complete_time = *opt_next_expiry;
    return true;
  }

  return false;
}

std::list<std::list<TMsg::TPtr>> TBrokerMsgQueue::GetAllOnShutdown() {
  assert(this);

  std::lock_guard<std::mutex> lock(Mutex);
  return GetAllMsgs();
}

std::list<std::list<TMsg::TPtr>> TBrokerMsgQueue::Reset() {
  assert(this);
  SenderNotify.Reset();
  return GetAllMsgs();
}

void TBrokerMsgQueue::TryBatchPerTopic(TMsg::TTimestamp now,
    TMsg::TPtr &&msg_ptr, TExpiryStatus &expiry_status) {
  assert(this);
  expiry_status.Clear();

  if (!PerTopicBatcher.IsEnabled()) {
    return;
  }

  expiry_status.OptInitialExpiry = PerTopicBatcher.GetNextCompleteTime();

  if (msg_ptr->GetRoutingType() == TMsg::TRoutingType::PartitionKey) {
    TMsg &msg = *msg_ptr;
    std::list<std::list<TMsg::TPtr>> batch_list =
        PerTopicBatcher.AddMsg(std::move(msg_ptr), now);

    /* Note: msg_ptr may still contain the message here, since the batcher only
       accepts messages when appropriate.  If msg_ptr is empty, then the
       batcher now contains the message so we transition its state to batching.
     */
    if (!msg_ptr) {
      MsgStateTracker.MsgEnterBatching(msg);
    }

    MsgStateTracker.MsgEnterSendWait(batch_list);
    ReadyList.splice(ReadyList.end(), std::move(batch_list));
    expiry_status.OptFinalExpiry = PerTopicBatcher.GetNextCompleteTime();
  } else {
    expiry_status.OptFinalExpiry = expiry_status.OptInitialExpiry;
  }
}

void TBrokerMsgQueue::TryBatchCombinedTopics(TMsg::TTimestamp now,
    TMsg::TPtr &&msg_ptr, TExpiryStatus &expiry_status) {
  assert(this);
  expiry_status.OptInitialExpiry = CombinedTopicsBatcher.GetNextCompleteTime();
  TMsg &msg = *msg_ptr;
  std::list<std::list<TMsg::TPtr>> batch_list =
      CombinedTopicsBatcher.AddMsg(std::move(msg_ptr), now);

  /* Note: msg_ptr may still contain the message here, since the batcher only
     accepts messages when appropriate.  If msg_ptr is empty, then the batcher
     now contains the message so we transition its state to batching.
   */
  if (!msg_ptr) {
    MsgStateTracker.MsgEnterBatching(msg);
  }

  MsgStateTracker.MsgEnterSendWait(batch_list);
  ReadyList.splice(ReadyList.end(), std::move(batch_list));
  expiry_status.OptFinalExpiry = CombinedTopicsBatcher.GetNextCompleteTime();
}

std::list<std::list<TMsg::TPtr>>
TBrokerMsgQueue::CheckPerTopicBatcher(TMsg::TTimestamp now,
    TExpiryStatus &expiry_status) {
  assert(this);
  expiry_status.Clear();
  std::list<std::list<TMsg::TPtr>> ready_batches;
  expiry_status.OptInitialExpiry = PerTopicBatcher.GetNextCompleteTime();

  if (expiry_status.OptInitialExpiry.IsKnown()) {
    if (*expiry_status.OptInitialExpiry <= now) {
      ready_batches.splice(ready_batches.end(),
                           PerTopicBatcher.GetCompleteBatches(now));
      expiry_status.OptFinalExpiry = PerTopicBatcher.GetNextCompleteTime();
    } else {
      expiry_status.OptFinalExpiry = expiry_status.OptInitialExpiry;
    }
  }

  MsgStateTracker.MsgEnterSendWait(ready_batches);
  return std::move(ready_batches);
}

std::list<std::list<TMsg::TPtr>>
TBrokerMsgQueue::CheckCombinedTopicsBatcher(TMsg::TTimestamp now,
    TExpiryStatus &expiry_status) {
  assert(this);
  expiry_status.Clear();
  std::list<std::list<TMsg::TPtr>> ready_batches;
  expiry_status.OptInitialExpiry = CombinedTopicsBatcher.GetNextCompleteTime();

  if (expiry_status.OptInitialExpiry.IsKnown()) {
    if (*expiry_status.OptInitialExpiry <= now) {
      assert(!CombinedTopicsBatcher.IsEmpty());
      ready_batches.splice(ready_batches.end(),
                           CombinedTopicsBatcher.TakeBatch());
      assert(CombinedTopicsBatcher.IsEmpty());
    } else {
      expiry_status.OptFinalExpiry = expiry_status.OptInitialExpiry;
    }
  }

  MsgStateTracker.MsgEnterSendWait(ready_batches);
  return std::move(ready_batches);
}

void TBrokerMsgQueue::CheckBothBatchers(TMsg::TTimestamp now,
    TExpiryStatus &per_topic_status, TExpiryStatus &combined_topics_status) {
  assert(this);
  std::list<std::list<TMsg::TPtr>> per_topic_batches =
      CheckPerTopicBatcher(now, per_topic_status);
  std::list<std::list<TMsg::TPtr>> combined_topics_batches =
      CheckCombinedTopicsBatcher(now, combined_topics_status);

  if (per_topic_status.OptInitialExpiry.IsKnown() &&
      combined_topics_status.OptInitialExpiry.IsKnown()) {
    /* Queue the list with the earliest initial expiry first.  Note: either or
       both lists may be empty. */
    if (*per_topic_status.OptInitialExpiry <
        *combined_topics_status.OptInitialExpiry) {
      ReadyList.splice(ReadyList.end(), std::move(per_topic_batches));
      ReadyList.splice(ReadyList.end(), std::move(combined_topics_batches));
    } else {
      ReadyList.splice(ReadyList.end(), std::move(combined_topics_batches));
      ReadyList.splice(ReadyList.end(), std::move(per_topic_batches));
    }
  } else {
    /* Here, at most one list will be nonempty so we can queue them in either
       order. */
    ReadyList.splice(ReadyList.end(), std::move(per_topic_batches));
    ReadyList.splice(ReadyList.end(), std::move(combined_topics_batches));
  }
}

std::list<std::list<TMsg::TPtr>>
TBrokerMsgQueue::GetAllMsgs() {
  assert(this);
  TOpt<TMsg::TTimestamp> per_topic_expiry =
      PerTopicBatcher.GetNextCompleteTime();
  TOpt<TMsg::TTimestamp> combined_topics_expiry =
      CombinedTopicsBatcher.GetNextCompleteTime();
  std::list<std::list<TMsg::TPtr>> per_topic = PerTopicBatcher.GetAllBatches();
  std::list<std::list<TMsg::TPtr>> combined_topics =
      CombinedTopicsBatcher.TakeBatch();
  MsgStateTracker.MsgEnterSendWait(per_topic);
  MsgStateTracker.MsgEnterSendWait(combined_topics);

  /* If expiry is defined for both batchers, queue the contents of the batcher
     with the earliest expiry first.  Otherwise choose arbitrarily. */
  if (per_topic_expiry.IsKnown() && combined_topics_expiry.IsKnown() &&
      (*combined_topics_expiry < *per_topic_expiry)) {
    ReadyList.splice(ReadyList.end(), std::move(combined_topics));
    ReadyList.splice(ReadyList.end(), std::move(per_topic));
  } else {
    ReadyList.splice(ReadyList.end(), std::move(per_topic));
    ReadyList.splice(ReadyList.end(), std::move(combined_topics));
  }

  return std::move(ReadyList);
}
