/* <bruce/test_util/misc_util.h>

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

   Utility functions for testing.
 */

#pragma once

#include <list>
#include <memory>
#include <string>

#include <bruce/msg.h>
#include <bruce/msg_state_tracker.h>
#include <capped/pool.h>

namespace Bruce {

  namespace TestUtil {

    struct TTestMsgCreator {
      std::unique_ptr<Capped::TPool> Pool;

      TMsgStateTracker MsgStateTracker;

      TTestMsgCreator()
          : Pool(new Capped::TPool(64, 1024 * 1024,
                                   Capped::TPool::TSync::Mutexed)) {
      }

      TMsg::TPtr NewMsg(const std::string &topic, const std::string &value,
          TMsg::TTimestamp timestamp, bool set_processed = false);
    };  // TTestMsgCreator

    bool KeyEquals(const TMsg::TPtr &msg, const char *key);

    inline bool KeyEquals(const TMsg::TPtr &msg, const std::string &key) {
      return KeyEquals(msg, key.c_str());
    }

    bool ValueEquals(const TMsg::TPtr &msg, const char *value);

    inline bool ValueEquals(const TMsg::TPtr &msg, const std::string &value) {
      return ValueEquals(msg, value.c_str());
    }

    /* Prevent unnecessary log messages about destroying unprocessed
       messages. */
    inline void SetProcessed(TMsg &msg) {
      msg.SetState(TMsg::TState::Processed);
    }

    /* Prevent unnecessary log messages about destroying unprocessed
       messages. */
    inline void SetProcessed(const TMsg::TPtr &msg_ptr) {
      SetProcessed(*msg_ptr);
    }

    /* Prevent unnecessary log messages about destroying unprocessed
       messages. */
    std::list<TMsg::TPtr>
    SetProcessed(std::list<TMsg::TPtr> &&msg_list);

    /* Prevent unnecessary log messages about destroying unprocessed
       messages. */
    std::list<std::list<TMsg::TPtr>>
    SetProcessed(std::list<std::list<TMsg::TPtr>> &&msg_list_list);

  }  // TestUtil

}  // Bruce
