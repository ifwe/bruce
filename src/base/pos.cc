/* <base/pos.cc>

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

   Implements <base/pos.h>.
 */

#include <base/pos.h>

using namespace Base;

const TSafeGlobal<TPos> TPos::Start(NewStart), TPos::Limit(NewLimit);

const TSafeGlobal<TPos> TPos::ReverseStart(NewReverseStart),
    TPos::ReverseLimit(NewReverseLimit);

