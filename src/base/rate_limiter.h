/* <base/rate_limiter.h>

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

   Class for limiting the rate of occurrence of an event.
 */

#pragma once

#include <functional>
#include <mutex>

#include <base/opt.h>

namespace Base {

  /* Class for limiting the rate of occurrence of an event.
     Template parameters:

         TTimePoint: a data type representing a point in time
         TDuration: a data type representing a length of time

     For instance, to limit the output rate of a log message to a maximum of
     once every 30 seconds, you might write code like this:

         using TClock = std::chrono::steady_clock;
         using TTimePoint = TClock::time_point;
         using TDuration = TTimePoint::duration;

         static TRateLimiter<TTimePoint, TDuration>
         rate_limiter(&TClock::now, std::chrono::seconds(30));

         if (rate_limiter.Test()) {
           syslog(LOG_INFO, "Blah blah");
         }
   */
  template <typename TTimePoint, typename TDuration>
  class TRateLimiter final {
    public:
    /* A function that, when called, tells you the current time. */
    using TClockFn = std::function<TTimePoint()>;

    /* Construct a rate limiter that uses 'clock_fn' as its clock and allows
       events to occur no more frequently than once every 'min_interval'. */
    TRateLimiter(const TClockFn &clock_fn, TDuration min_interval)
        : ClockFn(clock_fn),
          MinInterval(min_interval)
    {}

    /* When you want to perform the event whose rate you want to limit, first
       call this method.  If it returns true, perform the event.  Otherwise
       don't perform the event. */
    bool Test() {
      TTimePoint now = ClockFn();

      if (LastEventTime.IsUnknown()) {
        LastEventTime.MakeKnown(now);
        return true;
      }

      if ((now < *LastEventTime) || ((now - *LastEventTime) < MinInterval)) {
        return false;
      }

      *LastEventTime = now;
      return true;
    }

    private:
    /* The clock function that tells us what time it is. */
    const TClockFn ClockFn;

    /* Keeps track of the time at which Test() most recently returned true.
       Before the very first call to Test(), this is unknown. */
    typename Base::TOpt<TTimePoint> LastEventTime;

    /* This is the minimum interval to enforce. */
    TDuration MinInterval;
  };  // TRateLimiter

}  // Base
