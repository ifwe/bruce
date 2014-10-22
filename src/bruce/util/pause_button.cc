/* <bruce/util/pause_button.cc>

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

   Implements <bruce/util/pause_button.h>.
 */

#include <bruce/util/pause_button.h>

#include <server/counter.h>

using namespace Bruce;
using namespace Bruce::Util;

SERVER_COUNTER(PauseStarted);

TPauseButton::TPauseButton()
    : PauseActivated(false) {
}

void TPauseButton::Push() {
  assert(this);
  std::lock_guard<std::mutex> lock(Mutex);

  if (!PauseActivated) {
    Button.Push();
    PauseActivated = true;
    PauseStarted.Increment();
  }
}

void TPauseButton::Reset() {
  assert(this);
  Button.Reset();
  PauseActivated = false;
}
