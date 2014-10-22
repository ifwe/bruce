/* <server/counter.h>

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

   A counter useful for providing health reports.
 */

#pragma once

#include <cassert>
#include <cstdint>
#include <ctime>

#include <base/code_location.h>
#include <base/no_copy_semantics.h>
#include <server/blocking_asset.h>
#include <server/shared_lock.h>

/* A macro to simplify declaring counters. */
#define SERVER_COUNTER(name) static ::Server::TCounter name(HERE, #name);

namespace Server {

  /* A counter useful for providing health reports.

     Declare counters in your static data segment, then increment them during
     server operations.  You can later query the counters to discover how many
     times particular operations have been done.

     For example, this counter will keep track of the number of connections a
     listening socket accepts:

        static TCounter Connections(HERE, "Connections");

        void MainLoop(int sock) {
          for (;;) {
            int new_sock = accept(sock, 0, 0);
            Connections.Increment();
            ... handle the connection...
            close(new_sock);
          }
        }

     You may also use the COUNTER() macro to simplify the above declaration:

        SERVER_COUNTER(Connections);

     When you want to query your counters, you must first call Sample().  This
     will copy all of the counters' values, adding them to the sampled values.
     You may then query these sampled values while the counters continue to
     count.

     For example:

        void DumpCounters(ostream &strm) {
          TCounter::Sample();
          for (const TCounter *counter = TCounter::GetFirstCounter(); counter;
               counter = counter->GetNextCounter()) {
            strm << counter->GetCodeLocation()
                 << " '" << counter->GetName() << "' "
                 << counter->GetCount()
                 << endl;
          }
        }

     You may also call Reset(), which resets all the sampled values to zero.
     Doing this periodically helps to avoid overflows; however, the counters
     are unsigned 32-bit numbers, so don't sweat it.

     You may also call GetSampleTime() to get the time at which the counters
     were last sampled, and GetResetTime() to get the time at which the
     counters were last reset.  These times are drawn from the system clock.

     The counters in the program are kept in a singly-linked list formed during
     pre-main initialization.  The GetFirstCounter() and GetNextCounter()
     functions allow you to access this list.  The counters appear in no
     particular order. */
  class TCounter {
    NO_COPY_SEMANTICS(TCounter);

    public:
    /* Construct with all counts zero.  The given name should point to a string
       in the data segment, as we do not copy it.  */
    TCounter(const Base::TCodeLocation &code_location, const char *name);

    /* The code location at which the counter was declared. */
    const Base::TCodeLocation &GetCodeLocation() const {
      assert(this);
      return CodeLocation;
    }

    /* The count as of the last time the counters were sampled. */
    uint32_t GetCount() const {
      assert(this);
      return SampledCount;
    }

    /* The name of this counter.  This should be unique within its module.
       Never null. */
    const char *GetName() const {
      assert(this);
      return Name;
    }

    /* The next counter in the program, if any. */
    const TCounter *GetNextCounter() const {
      assert(this);
      return NextCounter;
    }

    /* Increment the counter.  This will not change the current frozen value,
       but will be reflected in the next frozen value. */
    void Increment(uint32_t delta = 1) {
      assert(this);
      TSharedLock<TBlockingAsset> lock(Asset);
      __sync_add_and_fetch(&UnsampledCount, delta);
    }

    /* The time of the most recent reset of the counters.
       This is initially set at program start-up, and is changed each time
       you call Sample(true). */
    static time_t GetResetTime() {
      return ResetTime;
    }

    /* The time of the most recent sample of the counters.
       This is initially set at program start-up, and is changed each time
       you call Sample(). */
    static time_t GetSampleTime() {
      return SampleTime;
    }

    /* Set the sampled values back to zero.
       Calling this function will update ResetTime. We return the reset time
       directly to eliminate any chance of getting another thread's reset
       time. */
    static time_t Reset();

    /* Add to the sampled values for each counter.
       Calling this function will update SampleTime. */
    static void Sample();

    /* The first counter in the program, if any. */
    static const TCounter *GetFirstCounter() {
      return FirstCounter;
    }

    private:
    /* See accessor. */
    Base::TCodeLocation CodeLocation;

    /* See accessor. */
    const char *Name;

    /* The currently incrementing count.  There is no direct access to this
       variable; instead, the Sample() function adds this variable to
       SampledCount, then sets this variable back to zero. */
    uint32_t UnsampledCount;

    /* See accessor. */
    uint32_t SampledCount;

    /* See accessor. */
    TCounter *NextCounter;

    /* We use this asset to coordinate the incrementing and freezing functions.
       When any counter is incrementing, it acquires this asset in shared mode.
       This allows multiple counters to be incrementing simultaneously with a
       minimum of synchronization overhead.  During a freeze operation, this
       asset is acquired exclusively, temporarily blocking all increments while
       the frozen values are copied.  This allows us to get a consistent
       snapshot of all counters at a single moment in time. */
    static TBlockingAsset Asset;

    /* See accessor. */
    static TCounter *FirstCounter;

    /* See accessors. */
    static time_t SampleTime, ResetTime;
  };  // TCounter

}  // Server
