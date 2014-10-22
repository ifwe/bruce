/* <base/timer_fd.h>

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

   A wrapper around the Linux timerfd functions.
 */

#pragma once

#include <cassert>
#include <cstdint>

#include <sys/timerfd.h>

#include <base/fd.h>
#include <base/no_copy_semantics.h>

namespace Base {

  class TTimerFd {
    NO_COPY_SEMANTICS(TTimerFd);

    public:
    TTimerFd(size_t milliseconds);

    const TFd &GetFd() const {
      assert(this);
      return Fd;
    }

    uint64_t Pop();

    private:
    TFd Fd;
  };  // TTimerFd

}  // Base
