/* <bruce/msg_state_tracker.h>

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

   Singleton class for tracking info on message states.
 */

#pragma once

#include <cassert>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <base/no_copy_semantics.h>
#include <bruce/msg.h>

namespace Bruce {

  /* Singleton class for tracking info on message states.  If Kafka starts
     falling behind, this lets us see which topics are lagging. */
  class TMsgStateTracker final {
    NO_COPY_SEMANTICS(TMsgStateTracker);

    public:
    struct TTopicStats {
      long BatchingCount;

      long SendWaitCount;

      long AckWaitCount;

      TTopicStats()
          : BatchingCount(0),
            SendWaitCount(0),
            AckWaitCount(0) {
      }
    };  // TTopicStats

    TMsgStateTracker()
        : NewCount(0) {
    }

    /* A brand new message has been created.  Update our stats to indicate
       this. */
    void MsgEnterNew();

    /* Set the state of 'msg' to TMsg::TState::Batching and update our stats to
       reflect this.  This indicates that the message is being batched. */
    void MsgEnterBatching(TMsg &msg);

    /* Set the state of 'msg' to TMsg::TState::SendWait and update our stats to
       reflect this.   This is called after the message has been batched if
       appropriate and added to a broker's queue of messages ready to be
       immediately sent. */
    void MsgEnterSendWait(TMsg &msg);

    /* Same as above, but process an entire list of messages.  All messages in
       list _must_ have same topic. */
    void MsgEnterSendWait(const std::list<TMsg::TPtr> &msg_list);

    /* Same as above, but process an entire list of message lists.  All
       messages in each inner list _must_ have same topic, but outer list can
       contain multiple topics. */
    void MsgEnterSendWait(
        const std::list<std::list<TMsg::TPtr>> &msg_list_list);

    /* Set the state of 'msg' to TMsg::TState::AckWait and update our stats to
       reflect this.  This is called immediately before the message is sent to
       Kafka. */
    void MsgEnterAckWait(TMsg &msg);

    /* Same as above, but process an entire list of messages.  All messages in
       list _must_ have same topic. */
    void MsgEnterAckWait(const std::list<TMsg::TPtr> &msg_list);

    /* Same as above, but process an entire list of message lists.  All
       messages in each inner list _must_ have same topic, but outer list can
       contain multiple topics. */
    void MsgEnterAckWait(
        const std::list<std::list<TMsg::TPtr>> &msg_list_list);

    /* Set the state of 'msg' to TMsg::TState::Processed and update our stats
       to reflect this.  This is called when a message is just about to be
       destroyed (due to either successful delivery or discard).  Do not call
       this from a destructor, since it may throw. */
    void MsgEnterProcessed(TMsg &msg);

    /* Same as above, but process an entire list of messages.  All messages in
       list _must_ have same topic. */
    void MsgEnterProcessed(const std::list<TMsg::TPtr> &msg_list);

    /* Same as above, but process an entire list of message lists.  All
       messages in each inner list _must_ have same topic, but outer list can
       contain multiple topics. */
    void MsgEnterProcessed(
        const std::list<std::list<TMsg::TPtr>> &msg_list_list);

    /* The first item is the topic, and the second item is stats for that
       topic. */   
    using TTopicStatsItem = std::pair<std::string, TTopicStats>;

    /* On return, 'topic_stats' will be filled with per-topic message stats and
       new_count will indicate the current number of messages with state
       TMsg::TState::New.  Per-topic stats are returned only for topics that
       have at least one nonzero count. */
    void GetStats(std::vector<TTopicStatsItem> &topic_stats,
                  long &new_count) const;

    /* A topic name is passed as a parameter.  Function returns true if topic
       is present in metadata or false otherwise. */
    using TTopicExistsFn = std::function<bool(const std::string &)>;

    /* When metadata is updated, this is called to identify topics in our stats
       that are no longer present in the metadata.  Any such topics whose
       counts are both 0 are deleted immediately.  Any such topics with at
       least one nonzero count is marked for deletion once its counts reach 0.
       'topic_exists_fn' indicates whether a topic is present in the new
       metadata. */
    void PruneTopics(const TTopicExistsFn &topic_exists_fn);

    private:
    class TDeltaComputer final {
      public:
      TDeltaComputer()
          : NewDelta(0),
            BatchingDelta(0),
            SendWaitDelta(0),
            AckWaitDelta(0) {
      }

      long GetNewDelta() const {
        assert(this);
        return NewDelta;
      }

      long GetBatchingDelta() const {
        assert(this);
        return BatchingDelta;
      }

      long GetSendWaitDelta() const {
        assert(this);
        return SendWaitDelta;
      }

      long GetAckWaitDelta() const {
        assert(this);
        return AckWaitDelta;
      }

      void CountBatchingEntered(TMsg::TState prev_state);

      void CountSendWaitEntered(TMsg::TState prev_state);

      void CountAckWaitEntered(TMsg::TState prev_state);

      void CountProcessedEntered(TMsg::TState prev_state);

      private:
      long NewDelta;

      long BatchingDelta;

      long SendWaitDelta;

      long AckWaitDelta;
    };  // TDeltaComputer

    struct TTopicStatsWrapper {
      TTopicStats TopicStats;

      /* A true value indicates that this topic is no longer present in the
         metadata, and can therefore be deleted from our stats once its counts
         reach 0. */
      bool OkToDelete;

      TTopicStatsWrapper()
          : OkToDelete(false) {
      }
    };  // TTopicStatsWrapper

    void UpdateStats(const std::string &topic, const TDeltaComputer &comp);

    /* Protects 'TopicStats' and 'NewCount'. */
    mutable std::mutex Mutex;

    /* Keys are topics, and values are per-topic stats. */
    std::unordered_map<std::string, TTopicStatsWrapper> TopicStats;

    /* Messages in state TMsg::TState::New are not broken down by topic, since
       some may have invalid topics. */
    long NewCount;
  };  // TMsgStateTracker

}  // Bruce
