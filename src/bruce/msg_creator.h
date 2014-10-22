/* <bruce/msg_creator.h>

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

   Simple class for creating TMsg objects.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include <base/no_construction.h>
#include <bruce/msg.h>
#include <bruce/msg_state_tracker.h>
#include <capped/pool.h>

namespace Bruce {

  /* The only purpose of this class is to avoid a circular dependency between
     TMsg and TMsgStateTracker. */
  class TMsgCreator final {
    NO_CONSTRUCTION(TMsgCreator);

    public:
    /* Create a message with the given topic and body.  'topic_begin' points to
       the first byte of the topic, and 'topic_end' points one byte past the
       last byte of the topic.  The topic and body contents are copied into the
       blob, with memory for the body being allocated from 'pool'.  Use routing
       type of 'AnyPartition'.

       Throws TMemoryCapReached if the pool doesn't contain enough memory to
       create the message. */
    static TMsg::TPtr CreateAnyPartitionMsg(TMsg::TTimestamp timestamp,
        const void *topic_begin, const void *topic_end, const void *key,
        size_t key_size, const void *value, size_t value_size,
        bool body_truncated, Capped::TPool &pool,
        TMsgStateTracker &msg_state_tracker) {
      TMsg::TPtr msg = TMsg::CreateAnyPartitionMsg(timestamp, topic_begin,
          topic_end, key, key_size, value, value_size, body_truncated, pool);
      msg_state_tracker.MsgEnterNew();
      return std::move(msg);
    }

    static TMsg::TPtr CreatePartitionKeyMsg(int32_t partition_key,
        TMsg::TTimestamp timestamp, const void *topic_begin,
        const void *topic_end, const void *key, size_t key_size,
        const void *value, size_t value_size, bool body_truncated,
        Capped::TPool &pool, TMsgStateTracker &msg_state_tracker) {
      TMsg::TPtr msg = TMsg::CreatePartitionKeyMsg(partition_key, timestamp,
          topic_begin, topic_end, key, key_size, value, value_size,
          body_truncated, pool);
      msg_state_tracker.MsgEnterNew();
      return std::move(msg);
    }
  };  // TMsgCreator

}  // Bruce
