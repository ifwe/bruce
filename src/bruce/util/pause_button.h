/* <bruce/util/pause_button.h>

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

   Used by the broker dispatcher to signal a pause event.
 */

#pragma once

#include <cassert>
#include <mutex>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace Util {

    class TPauseButton final {
      NO_COPY_SEMANTICS(TPauseButton);

      public:
      TPauseButton();

      const Base::TFd &GetFd() const {
        assert(this);
        return Button.GetFd();
      }

      /* Multiple threads can call Push() concurrently. */
      void Push();

      /* This method does not support concurrent calls. */
      void Reset();

      private:
      std::mutex Mutex;

      Base::TEventSemaphore Button;

      bool PauseActivated;
    };  // TPauseButton

  }  // Util

}  // Bruce
