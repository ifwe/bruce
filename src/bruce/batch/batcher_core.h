/* <bruce/batch/batcher_core.h>

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

   Implementation class for core batching logic.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <limits>

#include <base/opt.h>
#include <bruce/batch/batch_config.h>
#include <bruce/msg.h>

namespace Bruce {

  namespace Batch {

    class TBatcherCore final {
      public:
      enum class TAction {
        LeaveMsgAndReturnBatch,
        ReturnBatchAndTakeMsg,
        TakeMsgAndReturnBatch,
        TakeMsgAndLeaveBatch
      };  // TAction

      TBatcherCore();

      explicit TBatcherCore(const TBatchConfig &config);

      TBatcherCore(const TBatcherCore &) = default;

      TBatcherCore &operator=(const TBatcherCore &) = default;

      bool BatchingIsEnabled() const {
        assert(this);
        return Bruce::Batch::BatchingIsEnabled(Config);
      }

      const TBatchConfig &GetConfig() const {
        assert(this);
        return Config;
      }

      bool IsEmpty() const {
        assert(this);
        return (MsgCount == 0);
      }

      size_t GetMsgCount() const {
        assert(this);
        return MsgCount;
      }

      size_t GetByteCount() const {
        assert(this);
        return ByteCount;
      }

      Base::TOpt<TMsg::TTimestamp> GetNextCompleteTime() const;

      TAction ProcessNewMsg(TMsg::TTimestamp now, const TMsg::TPtr &msg);

      void ClearState();

      private:
      bool TestTimeLimit(TMsg::TTimestamp now,
          TMsg::TTimestamp new_msg_timestamp =
              std::numeric_limits<TMsg::TTimestamp>::max()) const;

      bool TestMsgCount(bool adding_msg = false) const;

      bool TestByteCount(size_t bytes_to_add = 0) const;

      bool TestByteCountExceeded(size_t bytes_to_add) const;

      bool TestAllLimits(TMsg::TTimestamp now) const {
        assert(this);
        return TestTimeLimit(now) || TestMsgCount() || TestByteCount();
      }

      void UpdateState(TMsg::TTimestamp timestamp, size_t body_size);

      TBatchConfig Config;

      TMsg::TTimestamp MinTimestamp;

      size_t MsgCount;

      size_t ByteCount;
    };  // TBatcherCore

  }  // Batch

}  // Bruce
