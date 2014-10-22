/* <bruce/util/bruce_rate_limiter.h>

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

   Thin wrapper around TBackoffRateLimiter that places a lower bound on the
   value returned by ComputeDelay().
 */

#pragma once

#include <algorithm>
#include <cassert>

#include <base/backoff_rate_limiter.h>

namespace Bruce {

  namespace Util {

    class TBruceRateLimiter final {
      public:
      TBruceRateLimiter(size_t initial_delay, size_t max_double,
          size_t min_delay,
          const std::function<unsigned ()> &random_number_generator)
          : MinDelay(min_delay),
            Limiter(initial_delay, max_double, random_number_generator) {
      }

      size_t ComputeDelay() {
        assert(this);
        return std::max(MinDelay, Limiter.ComputeDelay());
      }

      void OnAction() {
        assert(this);
        return Limiter.OnAction();
      }

      private:
      const size_t MinDelay;

      Base::TBackoffRateLimiter Limiter;
    };  // TBruceRateLimiter

  }  // Util

}  // Bruce
