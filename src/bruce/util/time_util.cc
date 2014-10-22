/* <bruce/util/time_util.cc>

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

   Implements <bruce/util/time_util.h>.
 */

#include <bruce/util/time_util.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>

#include <time.h>

using namespace Base;

void Bruce::Util::SleepMilliseconds(size_t milliseconds) {
  if (milliseconds == 0) {
    return;
  }

  struct timespec delay, remaining;
  delay.tv_sec = milliseconds / 1000;
  delay.tv_nsec = (milliseconds % 1000) * 1000000;

  while (nanosleep(&delay, &remaining) < 0) {
    if (errno != EINTR) {
      IfLt0(-1);  // this will throw
    }

    delay = remaining;
  }
}

void Bruce::Util::SleepMicroseconds(size_t microseconds) {
  if (microseconds == 0) {
    return;
  }

  struct timespec delay, remaining;
  delay.tv_sec = microseconds / 1000000;
  delay.tv_nsec = (microseconds % 1000000) * 1000;

  while (nanosleep(&delay, &remaining) < 0) {
    if (errno != EINTR) {
      IfLt0(-1);  // this will throw
    }

    delay = remaining;
  }
}

uint64_t Bruce::Util::GetEpochSeconds() {
  struct timespec t;
  IfLt0(clock_gettime(CLOCK_REALTIME, &t));
  return static_cast<uint64_t>(t.tv_sec);
}

uint64_t Bruce::Util::GetEpochMilliseconds() {
  struct timespec t;
  IfLt0(clock_gettime(CLOCK_REALTIME, &t));
  return (static_cast<uint64_t>(t.tv_sec) * 1000) + (t.tv_nsec / 1000000);
}

uint64_t Bruce::Util::GetMonotonicRawMilliseconds() {
  struct timespec t;
  IfLt0(clock_gettime(CLOCK_MONOTONIC_RAW, &t));
  return (static_cast<uint64_t>(t.tv_sec) * 1000) + (t.tv_nsec / 1000000);
}
