/* <signal/handler_installer.h>

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

   RAII for installing a signal handler.
 */

#pragma once

#include <cassert>

#include <signal.h>

#include <base/error_utils.h>
#include <base/no_copy_semantics.h>
#include <base/zero.h>

namespace Signal {

  /* RAII for installing a signal handler. */
  class THandlerInstaller {
    NO_COPY_SEMANTICS(THandlerInstaller);

    public:
    /* Set the mask to the given set. */
    THandlerInstaller(int sig, void (*handler)(int) = DoNothing)
        : SignalNumber(sig) {
      struct sigaction new_act;
      Base::Zero(new_act);
      new_act.sa_handler = handler;
      Base::IfLt0(sigaction(sig, &new_act, &OldAct));
    }

    /* Restore the old action. */
    ~THandlerInstaller() {
      assert(this);
      sigaction(SignalNumber, &OldAct, nullptr);
    }

    /* The signal we handle. */
    int GetSignalNumber() const {
      assert(this);
      return SignalNumber;
    }

    private:
    /* The do-nothing handler. */
    static void DoNothing(int sig);

    /* See accessor. */
    int SignalNumber;

    /* The action which we will restore. */
    struct sigaction OldAct;
  };  // THandlerInstaller

}  // Signal
