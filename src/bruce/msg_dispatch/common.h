/* <bruce/msg_dispatch/common.h>

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

   Common code for Kafka dispatcher implementation.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>

#include <base/opt.h>
#include <bruce/msg.h>

namespace Bruce {

  namespace MsgDispatch {

    class TShutdownOnDestroy final : public std::runtime_error {
      public:
      TShutdownOnDestroy()
          : std::runtime_error("TShutdownOnDestroy") {
      }

      virtual ~TShutdownOnDestroy() noexcept { }
    };  // TShutdownOnDestroy

    struct TMsgSet {
      /* If compression is configured for the topic of this message set, then
         this contains the total size in bytes of keys, values, and message
         header bytes for all messages in the set.  If compression is not
         configured for this topic, then this value is unused.  Its purpose is
         to prevent creation of a compressed message set whose size is large
         enough to cause Kafka to return a MessageSizeTooLarge error. */
      size_t DataSize;

      /* These are the messages in the message set. */
      std::list<TMsg::TPtr> Contents;

      TMsgSet()
          : DataSize(0) {
      }
    };  // TMsgSet

    /* All messages in map have same topic.  Key is partition and value is all
       messages with that partition. */
    using TMultiPartitionGroup = std::map<int32_t, TMsgSet>;

    /* Key is topic, value is all message sets for topic. */
    using TAllTopics = std::map<std::string, TMultiPartitionGroup>;

    using TCorrId = int32_t;  // correlation ID

    using TProduceRequest = std::pair<TCorrId, TAllTopics>;

    struct TInProgressShutdown {
      uint64_t Deadline;

      bool FastShutdown;

      TInProgressShutdown(uint64_t deadline, bool fast_shutdown)
          : Deadline(deadline),
            FastShutdown(fast_shutdown) {
      }
    };  // TInProgressShutdown

    struct TShutdownCmd {
      /* If SlowShutdownStartTime.IsKnown() then this is a slow shutdown
         command.  Otherwise it is a fast shutdown command. */
      Base::TOpt<uint64_t> OptSlowShutdownStartTime;

      TShutdownCmd() = default;

      explicit TShutdownCmd(uint64_t slow_start_shutdown_time)
          : OptSlowShutdownStartTime(slow_start_shutdown_time) {
        assert(OptSlowShutdownStartTime.IsKnown());
      }
    };  // TShutdownCmd

    void EmptyAllTopics(TAllTopics &all_topics,
        std::list<std::list<TMsg::TPtr>> &dest);

  }  // MsgDispatch

}  // Bruce
