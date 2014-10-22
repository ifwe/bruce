/* <base/assert_true.h>

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

   Defines a function for performing pass-through assertions.  Useful in
   ctor-initializer lines and other places where you want to check a
   value while accessing it.
 */

#pragma once

#include <cassert>

namespace Base {

  /* Returns the value given, after asserting that the value, when cast to
     bool, equals true.  This means non-null pointers and non-zero numbers will
     pass through, but null pointers and zeros will not. */
  template <typename TVal>
  const TVal &AssertTrue(const TVal &val) {
    assert(val);
    return val;
  }

}  // Base

