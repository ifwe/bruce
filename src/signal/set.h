/* <signal/set.h>

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

   A set of signals.
 */

#pragma once

#include <cassert>
#include <cstring>
#include <initializer_list>

#include <signal.h>

#include <base/error_utils.h>
#include <base/no_default_case.h>

namespace Signal {

  /* A set of signals. */
  class TSet {
    public:

    /* How to construct a new set. */
    enum TOp0 {
      /* Construct an empty set. */
      Empty,

      /* Construct a full set. */
      Full,

      /* Construct a set with the calling thread's mask in it. */
      Mask
    };  // TOp0

    /* How to construct a new set from an initializer list. */
    enum TOp1 {
      /* Include only the signals in the list. */
      Include,

      /* Include all signals except the ones in the list. */
      Exclude
    };  // TOp1

    /* See TOp0. */
    TSet(TOp0 op = Empty) {
      switch (op) {
        case Empty: {
          Base::IfLt0(sigemptyset(&OsObj));
          break;
        }
        case Full: {
          Base::IfLt0(sigfillset(&OsObj));
          break;
        }
        case Mask: {
          Base::IfNe0(pthread_sigmask(0, nullptr, &OsObj));
          break;
        }
        NO_DEFAULT_CASE;
      }
    }

    /* See TOp1.*/
    TSet(TOp1 op, std::initializer_list<int> sigs) {
      switch (op) {
        case Include: {
          Base::IfLt0(sigemptyset(&OsObj));
          for (int sig: sigs) {
            Base::IfLt0(sigaddset(&OsObj, sig));
          }
          break;
        }
        case Exclude: {
          Base::IfLt0(sigfillset(&OsObj));
          for (int sig: sigs) {
            Base::IfLt0(sigdelset(&OsObj, sig));
          }
          break;
        }
        NO_DEFAULT_CASE;
      }
    }

    /* Copy constructor. */
    TSet(const TSet &that) {
      assert(&that);
      memcpy(&OsObj, &that.OsObj, sizeof(OsObj));
    }

    /* Assignment operator. */
    TSet &operator=(const TSet &that) {
      assert(&that);
      memcpy(&OsObj, &that.OsObj, sizeof(OsObj));
      return *this;
    }

    /* Add the signal to the set. */
    TSet &operator+=(int sig) {
      assert(this);
      Base::IfLt0(sigaddset(&OsObj, sig));
      return *this;
    }

    /* Remove the signal from the set. */
    TSet &operator-=(int sig) {
      assert(this);
      Base::IfLt0(sigdelset(&OsObj, sig));
      return *this;
    }

    /* Construct a new set with the signal added. */
    TSet operator+(int sig) {
      assert(this);
      return TSet(*this) += sig;
    }

    /* Construct a new set with the signal removed. */
    TSet operator-(int sig) {
      assert(this);
      return TSet(*this) -= sig;
    }

    /* True iff. the signal is in the set. */
    bool operator[](int sig) const {
      assert(this);
      int result;
      Base::IfLt0(result = sigismember(&OsObj, sig));
      return result != 0;
    }

    /* Access the OS object. */
    const sigset_t &operator*() const {
      assert(this);
      return OsObj;
    }

    /* Access the OS object. */
    const sigset_t *Get() const {
      assert(this);
      return &OsObj;
    }

    private:
    /* The OS representation of the set. */
    sigset_t OsObj;
  };  // TSet

}  // Signal
