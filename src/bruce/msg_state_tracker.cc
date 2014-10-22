/* <bruce/msg_state_tracker.cc>

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

   Implements <bruce/msg_state_tracker.h>.
 */

#include <bruce/msg_state_tracker.h>

#include <syslog.h>

#include <base/no_default_case.h>
#include <bruce/util/time_util.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Util;

void TMsgStateTracker::MsgEnterNew() {
  assert(this);

  std::lock_guard<std::mutex> lock(Mutex);
  ++NewCount;
  assert(NewCount > 0);
}

void TMsgStateTracker::MsgEnterBatching(TMsg &msg) {
  assert(this);
  TDeltaComputer comp;
  comp.CountBatchingEntered(msg.GetState());
  msg.SetState(TMsg::TState::Batching);
  UpdateStats(msg.GetTopic(), comp);
}

void TMsgStateTracker::MsgEnterSendWait(TMsg &msg) {
  assert(this);
  TDeltaComputer comp;
  comp.CountSendWaitEntered(msg.GetState());
  msg.SetState(TMsg::TState::SendWait);
  UpdateStats(msg.GetTopic(), comp);
}

void TMsgStateTracker::MsgEnterSendWait(
    const std::list<TMsg::TPtr> &msg_list) {
  assert(this);

  if (msg_list.empty()) {
    return;
  }

  const std::string &topic = msg_list.front()->GetTopic();
  TDeltaComputer comp;

  for (auto &msg_ptr : msg_list) {
    assert(msg_ptr);
    TMsg &msg = *msg_ptr;
    assert(msg.GetTopic() == topic);
    comp.CountSendWaitEntered(msg.GetState());
    msg.SetState(TMsg::TState::SendWait);
  }

  UpdateStats(topic, comp);
}

void TMsgStateTracker::MsgEnterSendWait(
    const std::list<std::list<TMsg::TPtr>> &msg_list_list) {
  assert(this);

  for (const auto &msg_list : msg_list_list) {
    MsgEnterSendWait(msg_list);
  }
}

void TMsgStateTracker::MsgEnterAckWait(TMsg &msg) {
  assert(this);
  TDeltaComputer comp;
  comp.CountAckWaitEntered(msg.GetState());
  msg.SetState(TMsg::TState::AckWait);
  UpdateStats(msg.GetTopic(), comp);
}

void TMsgStateTracker::MsgEnterAckWait(const std::list<TMsg::TPtr> &msg_list) {
  assert(this);

  if (msg_list.empty()) {
    return;
  }

  const std::string &topic = msg_list.front()->GetTopic();
  TDeltaComputer comp;

  for (auto &msg_ptr : msg_list) {
    assert(msg_ptr);
    TMsg &msg = *msg_ptr;
    assert(msg.GetTopic() == topic);
    comp.CountAckWaitEntered(msg.GetState());
    msg.SetState(TMsg::TState::AckWait);
  }

  UpdateStats(topic, comp);
}

void TMsgStateTracker::MsgEnterAckWait(
    const std::list<std::list<TMsg::TPtr>> &msg_list_list) {
  assert(this);

  for (const auto &msg_list : msg_list_list) {
    MsgEnterAckWait(msg_list);
  }
}

void TMsgStateTracker::MsgEnterProcessed(TMsg &msg) {
  assert(this);
  TDeltaComputer comp;
  comp.CountProcessedEntered(msg.GetState());
  msg.SetState(TMsg::TState::Processed);
  UpdateStats(msg.GetTopic(), comp);
}

void TMsgStateTracker::MsgEnterProcessed(
    const std::list<TMsg::TPtr> &msg_list) {
  assert(this);

  if (msg_list.empty()) {
    return;
  }

  const std::string &topic = msg_list.front()->GetTopic();
  TDeltaComputer comp;

  for (auto &msg_ptr : msg_list) {
    assert(msg_ptr);
    TMsg &msg = *msg_ptr;
    assert(msg.GetTopic() == topic);
    comp.CountProcessedEntered(msg.GetState());
    msg.SetState(TMsg::TState::Processed);
  }

  UpdateStats(topic, comp);
}

void TMsgStateTracker::MsgEnterProcessed(
    const std::list<std::list<TMsg::TPtr>> &msg_list_list) {
  assert(this);

  for (const auto &msg_list : msg_list_list) {
    MsgEnterProcessed(msg_list);
  }
}

void TMsgStateTracker::GetStats(std::vector<TTopicStatsItem> &result,
    long &new_count) const {
  assert(this);
  result.clear();

  std::lock_guard<std::mutex> lock(Mutex);

  for (const auto &item : TopicStats) {
    const TTopicStats &stats = item.second.TopicStats;

    if (stats.BatchingCount || stats.SendWaitCount || stats.AckWaitCount) {
      result.push_back(std::make_pair(item.first, stats));
    }
  }

  new_count = NewCount;
}

void TMsgStateTracker::PruneTopics(const TTopicExistsFn &topic_exists_fn) {
  assert(this);
  std::lock_guard<std::mutex> lock(Mutex);

  for (auto iter = TopicStats.begin(); iter != TopicStats.end(); ) {
    TTopicStatsWrapper &w = iter->second;
    w.OkToDelete = !topic_exists_fn(iter->first);

    if (w.OkToDelete && (w.TopicStats.BatchingCount == 0) &&
        (w.TopicStats.SendWaitCount == 0) &&
        (w.TopicStats.AckWaitCount == 0)) {
      iter = TopicStats.erase(iter);
    } else {
      ++iter;
    }
  }
}

void TMsgStateTracker::TDeltaComputer::CountBatchingEntered(
    TMsg::TState prev_state) {
  assert(this);

  switch (prev_state) {
    case TMsg::TState::New: {
      --NewDelta;
      ++BatchingDelta;
      break;
    }
    case TMsg::TState::Batching: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot reenter state 'Batching'");
      }

      assert(false);
      break;
    }
    case TMsg::TState::SendWait: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot enter state 'Batching' from 'SendWait'");
      }

      assert(false);
      break;
    }
    case TMsg::TState::AckWait: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot enter state 'Batching' from 'AckWait'");
      }

      assert(false);
      break;
    }
    case TMsg::TState::Processed: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot enter state 'Batching' from 'Processed'");
      }

      assert(false);
      break;
    }
    NO_DEFAULT_CASE;
  }
}

void TMsgStateTracker::TDeltaComputer::CountSendWaitEntered(
    TMsg::TState prev_state) {
  assert(this);

  switch (prev_state) {
    case TMsg::TState::New: {
      --NewDelta;
      ++SendWaitDelta;
      break;
    }
    case TMsg::TState::Batching: {
      --BatchingDelta;
      ++SendWaitDelta;
      break;
    }
    case TMsg::TState::SendWait: {
      break;
    }
    case TMsg::TState::AckWait: {
      --AckWaitDelta;
      ++SendWaitDelta;
      break;
    }
    case TMsg::TState::Processed: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot enter state 'SendWait' from 'Processed'");
      }

      assert(false);
      break;
    }
    NO_DEFAULT_CASE;
  }
}

void TMsgStateTracker::TDeltaComputer::CountAckWaitEntered(
    TMsg::TState prev_state) {
  assert(this);

  switch (prev_state) {
    case TMsg::TState::New: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
               "Bug: Cannot enter state 'AckWait' from 'New'");
      }

      assert(false);
      break;
    }
    case TMsg::TState::Batching: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot enter state 'AckWait' from 'Batching'");
      }

      assert(false);
      break;
    }
    case TMsg::TState::SendWait: {
      --SendWaitDelta;
      ++AckWaitDelta;
      break;
    }
    case TMsg::TState::AckWait: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot reenter state 'AckWait'");
      }

      assert(false);
      break;
    }
    case TMsg::TState::Processed: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot enter state 'AckWait' from 'Processed'");
      }

      assert(false);
      break;
    }
    NO_DEFAULT_CASE;
  }
}

void TMsgStateTracker::TDeltaComputer::CountProcessedEntered(
    TMsg::TState prev_state) {
  assert(this);

  switch (prev_state) {
    case TMsg::TState::New: {
      --NewDelta;
      break;
    }
    case TMsg::TState::Batching: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot enter state 'Processed' from 'Batching'");
      }

      assert(false);
      break;
    }
    case TMsg::TState::SendWait: {
      --SendWaitDelta;
      break;
    }
    case TMsg::TState::AckWait: {
      --AckWaitDelta;
      break;
    }
    case TMsg::TState::Processed: {
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Bug: Cannot reenter state 'Processed'");
      }

      assert(false);
      break;
    }
    NO_DEFAULT_CASE;
  }
}

void TMsgStateTracker::UpdateStats(const std::string &topic,
    const TDeltaComputer &comp) {
  assert(this);
  long new_delta = comp.GetNewDelta();
  long batching_delta = comp.GetBatchingDelta();
  long send_wait_delta = comp.GetSendWaitDelta();
  long ack_wait_delta = comp.GetAckWaitDelta();

  std::lock_guard<std::mutex> lock(Mutex);

  if (batching_delta || send_wait_delta || ack_wait_delta) {
    /* We could accomplish the same thing with a single call to
       TopicStats.insert(), but this way we avoid creation of a temporary
       string in the common case where the map already contains the topic. */
    auto iter = TopicStats.find(topic);

    if (iter == TopicStats.end()) {
      auto result = TopicStats.insert(
          std::make_pair(topic, TTopicStatsWrapper()));
      assert(result.second);
      iter = result.first;
    }

    assert(iter != TopicStats.end());
    TTopicStatsWrapper &w = iter->second;
    w.TopicStats.BatchingCount += batching_delta;
    assert(w.TopicStats.BatchingCount >= 0);
    w.TopicStats.SendWaitCount += send_wait_delta;
    assert(w.TopicStats.SendWaitCount >= 0);
    w.TopicStats.AckWaitCount += ack_wait_delta;
    assert(w.TopicStats.AckWaitCount >= 0);

    if (w.OkToDelete && (w.TopicStats.BatchingCount == 0) &&
        (w.TopicStats.SendWaitCount == 0) &&
        (w.TopicStats.AckWaitCount == 0)) {
      TopicStats.erase(iter);
    }
  }

  NewCount += new_delta;
}
