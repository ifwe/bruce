/* <base/safe_global.h>

   ----------------------------------------------------------------------------
   Copyright 2013 if(we)

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

   A global variable that initializes only when called for.
 */

#pragma once

#include <cassert>
#include <functional>

#include <base/assert_true.h>
#include <base/no_copy_semantics.h>
#include <base/spin_lock.h>

namespace Base {

  template <typename TVal>
  class TSafeGlobal {
    NO_COPY_SEMANTICS(TSafeGlobal);
    public:

    typedef std::function<TVal *()> TFactory;

    TSafeGlobal(const TFactory &factory)
        : Factory(factory), Val(0), Constructing(false) {}

    ~TSafeGlobal() {
      assert(this);
      delete Val;
    }

    const TVal &operator*() const {
      assert(this);
      Freshen();
      return *AssertTrue(Val);
    }

    TVal &operator*() {
      assert(this);
      Freshen();
      return *AssertTrue(Val);
    }

    const TVal *operator->() const {
      assert(this);
      Freshen();
      return AssertTrue(Val);
    }

    TVal *operator->() {
      assert(this);
      Freshen();
      return AssertTrue(Val);
    }

    const TVal *GetObj() const {
      assert(this);
      Freshen();
      return AssertTrue(Val);
    }

    TVal *GetObj() {
      assert(this);
      Freshen();
      return AssertTrue(Val);
    }

    private:
    void Freshen() const {
      assert(this);
      TSpinLock::TLock lock(SpinLock);
      if (!Val) {
        assert(!Constructing);
        Constructing = true;
        Val = Factory();
        assert(Val);
        Constructing = false;
      }
    }

    TSpinLock SpinLock;

    TFactory Factory;

    mutable TVal *Val;

    mutable bool Constructing;
  };  // TSafeGlobal

}  // Base
