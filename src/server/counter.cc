/* <server/counter.cc>

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

   Implements <server/counter.h>.
 */

#include <server/counter.h>

#include <server/exclusive_lock.h>

using namespace Base;
using namespace Server;

TCounter::TCounter(const TCodeLocation &code_location, const char *name)
    : CodeLocation(code_location),
      Name(name),
      UnsampledCount(0),
      SampledCount(0) {
  assert(name);
  NextCounter = FirstCounter;
  FirstCounter = this;
}

time_t TCounter::Reset() {
  TExclusiveLock<TBlockingAsset> lock(Asset);
  ResetTime = time(0);

  for (TCounter *counter = FirstCounter;
       counter;
       counter = counter->NextCounter) {
    counter->SampledCount = 0;
  }

  return ResetTime;
}

void TCounter::Sample() {
  TExclusiveLock<TBlockingAsset> lock(Asset);
  SampleTime = time(0);

  for (TCounter *counter = FirstCounter;
       counter;
       counter = counter->NextCounter) {
    counter->SampledCount += counter->UnsampledCount;
    counter->UnsampledCount = 0;
  }
}

TBlockingAsset TCounter::Asset;

TCounter *TCounter::FirstCounter = 0;

time_t TCounter::SampleTime = time(0), TCounter::ResetTime = SampleTime;
