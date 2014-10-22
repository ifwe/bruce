/* <base/backoff_rate_limiter.h>

   ----------------------------------------------------------------------------
   Copyright 2013 if(we)

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

   Class for generating delay times for performing an action based on how long
   ago the action was last performed.  Uses random exponential backoff to
   compute wait times.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <functional>

#include <time.h>

#include <base/error_utils.h>
#include <base/random_exp_backoff.h>

namespace Base {

  /* Class for generating delay times for performing an action based on how
     long ago the action was last performed.  Uses random exponential backoff
     to compute wait times. */
  class TBackoffRateLimiter final {
    public:
    /* Parameter 'initial_delay' specifies the initial delay between actions
       that you wish to impose (plus or minus some random noise).  Parameter
       'max_double' specifies the maximum number of times the value given by
       'initial_delay' should be allowed to double for consecutive actions such
       that each action occurs within a window of at most (initial_delay <<
       (max_double + 2)) time units of the previous action.  Once a gap between
       actions occurs that exceeds the window size, the rate limiter is reset
       to its initial state, and the same doubling behavior can again take
       place.  Time units are in milliseconds. */
    TBackoffRateLimiter(size_t initial_delay, size_t max_double,
        const std::function<unsigned ()> &random_number_generator);

    /* Call this when you want to perform the action.  Returns the number of
       milliseconds (possibly 0) that you should wait before performing the
       action. */
    size_t ComputeDelay();

    /* Call this to indicate that the action is now being performed.  Updates
       the internal state of the rate limiter so it will correctly compute the
       next delay value. */
    void OnAction() {
      assert(this);
      GetCurrentTime(LastEventTime);
    }

    private:
    static void GetCurrentTime(struct timespec &result) {
      IfLt0(clock_gettime(CLOCK_MONOTONIC_RAW, &result));
    }

    bool InBackoffWindow(const struct timespec &now);

    const long BackoffWindow;

    TRandomExpBackoff DelayGenerator;

    bool FirstTime;

    struct timespec WindowStartTime;

    struct timespec LastEventTime;
  };  // TBackoffRateLimiter

}  // Base
