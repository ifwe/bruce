/* <base/backoff_rate_limiter.cc>

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

   Implements <base/backoff_rate_limiter.h>.
 */

#include <base/backoff_rate_limiter.h>

#include <limits>

using namespace Base;

static size_t ComputeTimeToNow(const struct timespec &start,
    const struct timespec &now) {
  if (start.tv_sec > now.tv_sec) {
    return 0;
  }

  if (start.tv_sec == now.tv_sec) {
    return (start.tv_nsec > now.tv_nsec) ?
        0 : ((now.tv_nsec - start.tv_nsec) / 1000000);
  }

  return ((now.tv_sec - start.tv_sec) * 1000) +
         ((now.tv_nsec - start.tv_nsec) / 1000000);
}

static long ComputeBackoffWindow(size_t initial_delay, size_t max_double) {
  /* This behavior is somewhat arbitrary. */
  size_t window = initial_delay << (max_double + 2);
  assert(window >> (max_double + 2) == initial_delay);  // check for overflow
  assert(static_cast<long>(window) <= std::numeric_limits<long>::max());
  return static_cast<long>(window);
}

TBackoffRateLimiter::TBackoffRateLimiter(
    size_t initial_delay, size_t max_double,
    const std::function<unsigned ()> &random_number_generator)
    : BackoffWindow(ComputeBackoffWindow(initial_delay, max_double)),
      DelayGenerator(initial_delay, max_double, random_number_generator),
      FirstTime(true),
      WindowStartTime({ 0, 0 }),
      LastEventTime({ 0, 0 }) {
}

size_t TBackoffRateLimiter::ComputeDelay() {
  assert(this);
  struct timespec now;
  GetCurrentTime(now);

  if (InBackoffWindow(now)) {
    size_t delay = DelayGenerator.NextValue();
    size_t delta = ComputeTimeToNow(LastEventTime, now);

    if (delta < delay) {
      return delay - delta;
    }
  } else {
    DelayGenerator.Reset();
    WindowStartTime = now;
  }

  LastEventTime = now;
  return 0;
}

bool TBackoffRateLimiter::InBackoffWindow(const struct timespec &now) {
  assert(this);

  if (FirstTime) {
    FirstTime = false;
    return false;
  }

  long delta = ComputeTimeToNow(WindowStartTime, now);
  return (delta <= BackoffWindow);
}
