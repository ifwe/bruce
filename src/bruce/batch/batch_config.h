/* <bruce/batch/batch_config.h>

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

   Batching configuration class.
 */

#pragma once

#include <cassert>
#include <cstddef>

namespace Bruce {

  namespace Batch {

    struct TBatchConfig {
      TBatchConfig()
          : TimeLimit(0),
            MsgCount(0),
            ByteCount(0) {
      }

      TBatchConfig(size_t time_limit, size_t msg_count, size_t byte_count)
          : TimeLimit(time_limit),
            MsgCount(msg_count),
            ByteCount(byte_count) {
      }

      TBatchConfig(const TBatchConfig &) = default;

      TBatchConfig& operator=(const TBatchConfig &) = default;

      void Clear() {
        assert(this);
        *this = TBatchConfig();
      }

      size_t TimeLimit;  // milliseconds

      size_t MsgCount;

      size_t ByteCount;
    };  // TBatchConfig

    inline bool BatchingIsEnabled(const TBatchConfig &config) {
      return config.TimeLimit || config.MsgCount || config.ByteCount;
    }

    inline bool TimeLimitIsEnabled(const TBatchConfig &config) {
      return (config.TimeLimit != 0);
    }

    inline bool MsgCountLimitIsEnabled(const TBatchConfig &config) {
      return (config.MsgCount != 0);
    }

    inline bool ByteCountLimitIsEnabled(const TBatchConfig &config) {
      return (config.ByteCount != 0);
    }

  }  // Batch

}  // Bruce
