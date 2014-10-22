/* <bruce/batch/single_topic_batcher.h>

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

   Class for batching messages for a single topic.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <list>
#include <string>

#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/batch/batcher_core.h>
#include <bruce/msg.h>

namespace Bruce {

  namespace Batch {

    class TSingleTopicBatcher final {
      NO_COPY_SEMANTICS(TSingleTopicBatcher);

      public:
      TSingleTopicBatcher() = default;

      explicit TSingleTopicBatcher(const TBatchConfig &config)
          : CoreState(config) {
      }

      TSingleTopicBatcher(TSingleTopicBatcher &&) = default;

      TSingleTopicBatcher &operator=(TSingleTopicBatcher &&) = default;

      bool IsEmpty() const {
        assert(this);
        return CoreState.IsEmpty();
      }

      const TBatchConfig &GetConfig() const {
        assert(this);
        return CoreState.GetConfig();
      }

      bool BatchingIsEnabled() const {
        assert(this);
        return CoreState.BatchingIsEnabled();
      }

      std::list<TMsg::TPtr>
      AddMsg(TMsg::TPtr &&msg, TMsg::TTimestamp now) {
        assert(this);
        std::list<TMsg::TPtr> result = DoAddMsg(std::move(msg), now);
        assert(MsgList.size() == CoreState.GetMsgCount());
        return std::move(result);
      }

      Base::TOpt<TMsg::TTimestamp> GetNextCompleteTime() const {
        assert(this);
        return CoreState.GetNextCompleteTime();
      }

      /* Empty out the batcher, and return all messages it contained. */
      std::list<TMsg::TPtr> TakeBatch() {
        assert(this);
        CoreState.ClearState();
        assert(CoreState.GetMsgCount() == 0);
        return std::move(MsgList);
      }

      private:
      std::list<TMsg::TPtr>
      DoAddMsg(TMsg::TPtr &&msg, TMsg::TTimestamp now);

      TBatcherCore CoreState;

      std::list<TMsg::TPtr> MsgList;
    };  // TCombinedTopicsBatcher

  }  // Batch

}  // Bruce
