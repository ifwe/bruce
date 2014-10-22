/* <base/event_semaphore.h>

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

   Event semaphore class.
 */

#pragma once

#include <cstdint>
#include <cassert>

#include <base/fd.h>
#include <base/no_copy_semantics.h>

namespace Base {

  class TEventSemaphore {
    NO_COPY_SEMANTICS(TEventSemaphore);

    public:
    TEventSemaphore(uint64_t initial_count = 0, bool nonblocking = false);

    const TFd &GetFd() const {
      assert(this);
      return Fd;
    }

    /* Reinitialize the semaphore with the given initial count.  Calling this
       method is guaranteed to preserve the semaphore's integer file descriptor
       number. */
    void Reset(uint64_t initial_count = 0);

    /* If the nonblocking option was passed to the constructor, this returns
       true if the pop was successful, or false if the pop failed because the
       semaphore had a count of 0 immediately before the call.  It the
       nonblocking option was not passed to the constructor, this always
       returns true. */
    bool Pop();

    void Push(uint64_t count = 1);

    private:
    TFd Fd;
  };  // TEventSemaphore

}  // Base

