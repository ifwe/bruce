/* <base/timer_fd.cc>

   ----------------------------------------------------------------------------
   Copyright 2010-2013 if(we)

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

   Implements <base/timer_fd.h>.
 */

#include <base/timer_fd.h>

#include <algorithm>

#include <base/error_utils.h>
#include <base/zero.h>

using namespace std;
using namespace Base;

TTimerFd::TTimerFd(size_t milliseconds)
    : Fd(timerfd_create(CLOCK_MONOTONIC, 0)) {
  itimerspec its;
  Zero(its);
  //seconds = max(seconds, static_cast<uint32_t>(1));
  uint32_t seconds = milliseconds / 1000UL;
  int64_t nanoseconds = (milliseconds % 1000UL) * 1000000L;
  its.it_interval.tv_sec = seconds;
  its.it_interval.tv_nsec = nanoseconds;
  its.it_value.tv_sec = seconds;
  its.it_value.tv_nsec = nanoseconds;
  IfLt0(timerfd_settime(Fd, 0, &its, nullptr));
}

uint64_t TTimerFd::Pop() {
  assert(this);
  uint64_t count;
  IfLt0(read(Fd, &count, sizeof(count)));
  return count;
}
