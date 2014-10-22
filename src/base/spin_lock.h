/* <base/spin_lock.h>

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

   A very low-overhead mutex.
 */

#pragma once

#include <cassert>
#include <atomic>

#include <sched.h>

#include <base/no_copy_semantics.h>

namespace Base {

  class TSpinLock {
    NO_COPY_SEMANTICS(TSpinLock);
    public:

    class TLock {
      NO_COPY_SEMANTICS(TLock);
      public:

      TLock(const TSpinLock &spin_lock) : SpinLock(spin_lock) {
        while (spin_lock.Lock.test_and_set(std::memory_order_acquire));
      }

      ~TLock() {
        assert(this);
        SpinLock.Lock.clear(std::memory_order_release);
      }

      private:
      const TSpinLock &SpinLock;
    };  // TSpinLock

    class TSoftLock {
      NO_COPY_SEMANTICS(TSoftLock);
      public:
      TSoftLock(const TSpinLock &spin_lock) : SpinLock(spin_lock) {
        size_t tries = 0UL;
        while (spin_lock.Lock.test_and_set(std::memory_order_acquire)) {
          if (++tries == YieldCount) {
            tries = 0UL;
            #if 0
            timespec wait{0, 10000};
            nanosleep(&wait, nullptr);
            #endif
            sched_yield();
          }
        }
      }

      ~TSoftLock() {
        assert(this);
        SpinLock.Lock.clear(std::memory_order_release);
      }

      private:
      const TSpinLock &SpinLock;

      static const size_t YieldCount = 20UL;
    };  // TSoftLock

    /* TODO */
    TSpinLock() : Lock(ATOMIC_FLAG_INIT) {}

    private:
    /* TODO */
    mutable std::atomic_flag Lock;
  };  // TSpinLock

}  // Base
