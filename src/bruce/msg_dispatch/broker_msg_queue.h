/* <bruce/msg_dispatch/broker_msg_queue.h>

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

   A queue of messages waiting to be sent to a broker.  Each send thread
   maintains one of these.  Broker-level batching is done here.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <list>
#include <mutex>
#include <string>
#include <unordered_set>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/batch/combined_topics_batcher.h>
#include <bruce/batch/global_batch_config.h>
#include <bruce/batch/per_topic_batcher.h>
#include <bruce/msg.h>
#include <bruce/msg_state_tracker.h>

namespace Bruce {

  namespace MsgDispatch {

    class TBrokerMsgQueue final {
      NO_COPY_SEMANTICS(TBrokerMsgQueue);

      public:
      TBrokerMsgQueue(const Batch::TGlobalBatchConfig &batch_config,
          TMsgStateTracker &msg_state_tracker);

      /* Returns an FD that becomes readable when an incoming message or batch
         of messages from the router thread triggers one of the following
         conditions:

             1.  The queue of messages ready to be immediately sent becomes
                 nonempty.

             2.  A batching time limit becomes set when no time limit was
                 previously set, or the time limit changes to an earlier value.
       */
      const Base::TFd &GetSenderNotifyFd() {
        assert(this);
        return SenderNotify.GetFd();
      }

      /* Put 'msg' into the queue, batching it at the broker level if
         appropriate.  The FD returned by GetSenderNotifyFd() will become
         readable if this triggers at least one of the following conditions:

             1.  The batcher expiration time changes (or becomes defined due to
                 the batcher becoming nonempty).

             2.  The ready list was previously empty and now is nonempty.

         In either case, the queue needs attention from its send thread. */
      void Put(TMsg::TTimestamp now, TMsg::TPtr &&msg);

      /* Same as above, but 'msg' bypasses broker-level batching. */
      void PutNow(TMsg::TTimestamp now, TMsg::TPtr &&msg);

      /* Same as above, except handles batch of messages.  The batch bypasses
         broker-level batching and goes directly to the ready list. */
      void PutNow(TMsg::TTimestamp now,
          std::list<std::list<TMsg::TPtr>> &&batch);

      /* Get all messages ready to send (grouped in per-topic lists) and pass
         them back in 'ready_msgs', which may be empty on return.  If any
         queued messages remain in the batcher, and there is a time limit on
         the batch then return true.  Otherwise return false.  In the case
         where true is returned, 'next_batch_complete_time' provides the
         expiration time.  Otherwise 'next_batch_complete_time' is left
         unmodified.  Pops the semaphore associated with the FD returned by
         GetSenderNotifyFd().  This should only be called when FD returned by
         GetSenderNotifyFd() is readable on entry, and is guaranteed not to
         block in that case. */
      bool Get(TMsg::TTimestamp now,
               TMsg::TTimestamp &next_batch_complete_time,
               std::list<std::list<TMsg::TPtr>> &ready_msgs) {
        assert(this);
        SenderNotify.Pop();
        return NonblockingGet(now, next_batch_complete_time, ready_msgs);
      }

      /* Same as Get(), but avoids popping the semaphore.  Therefore guaranteed
         not to block even if FD returned by GetSenderNotifyFd() is not
         readable on entry. */
      bool NonblockingGet(TMsg::TTimestamp now,
                          TMsg::TTimestamp &next_batch_complete_time,
                          std::list<std::list<TMsg::TPtr>> &ready_msgs);

      /* Get entire contents of batcher and ready list, regardless of batch
         state.  Avoid popping the semaphore. */
      std::list<std::list<TMsg::TPtr>> GetAllOnShutdown();

      /* Reset the queue to its initial state and return all messages it
         formerly contained.  Intended to be called _after_ the send thread has
         been shut down, and therefore does _not_ acquire 'Mutex'. */
      std::list<std::list<TMsg::TPtr>> Reset();

      private:
      struct TExpiryStatus {
        Base::TOpt<TMsg::TTimestamp> OptInitialExpiry;

        Base::TOpt<TMsg::TTimestamp> OptFinalExpiry;

        void Clear() {
          assert(this);
          OptInitialExpiry.Reset();
          OptFinalExpiry.Reset();
        }
      };  // TExpiryStatus

      void TryBatchPerTopic(TMsg::TTimestamp now, TMsg::TPtr &&msg_ptr,
          TExpiryStatus &expiry_status);

      void TryBatchCombinedTopics(TMsg::TTimestamp now, TMsg::TPtr &&msg_ptr,
          TExpiryStatus &expiry_status);

      std::list<std::list<TMsg::TPtr>>
      CheckPerTopicBatcher(TMsg::TTimestamp now, TExpiryStatus &expiry_status);

      std::list<std::list<TMsg::TPtr>>
      CheckCombinedTopicsBatcher(TMsg::TTimestamp now,
          TExpiryStatus &expiry_status);

      void CheckBothBatchers(TMsg::TTimestamp now,
          TExpiryStatus &per_topic_status,
          TExpiryStatus &combined_topics_status);

      Base::TOpt<TMsg::TTimestamp> CheckBothBatchers(TMsg::TTimestamp now);

      std::list<std::list<TMsg::TPtr>> GetAllMsgs();

      /* Becomes readable to notify the kafka dispatcher send thread that the
         queue needs attention. */
      Base::TEventSemaphore SenderNotify;

      /* Protects 'PerTopicBatcher', 'CombinedTopicsBatcher', and 'ReadyList'
         from concurrent access by router thread and kafka dispatcher send and
         receive threads. */
      std::mutex Mutex;

      /* Per-topic batching for PartitionKey messages is done here.  Per-topic
         batching for AnyPartition messages is done by the router thread. */
      Batch::TPerTopicBatcher PerTopicBatcher;

      /* Messages being batched at the broker level, not on a per-topic basis.
         Both AnyPartition and PartitionKey messages may get batched here. */
      Batch::TCombinedTopicsBatcher CombinedTopicsBatcher;

      /* Messages ready to send immediately. */
      std::list<std::list<TMsg::TPtr>> ReadyList;

      TMsgStateTracker &MsgStateTracker;
    };  // TBrokerMsgQueue

  }  // MsgDispatch

}  // Bruce
