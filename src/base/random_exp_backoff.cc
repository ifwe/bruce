/* <base/random_exp_backoff.cc>

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

   Implements <base/random_exp_backoff.h>
 */

#include <base/random_exp_backoff.h>

using namespace Base;

size_t TRandomExpBackoff::NextValue() {
  assert(this);
  size_t half_base = BaseCount / 2;
  size_t min_result = half_base;
  size_t max_result = BaseCount + half_base;
  assert(max_result >= min_result);
  size_t range = max_result - min_result + 1;
  size_t result = min_result + (RandomNumberGenerator() % range);
  assert((result >= min_result) && (result <= max_result));

  if (DoubleTimesLeft) {
    --DoubleTimesLeft;
    size_t new_base_count = BaseCount * 2;

    /* Assert on overflow. */
    assert(new_base_count > BaseCount);
    assert((new_base_count + (new_base_count / 2) > new_base_count));

    BaseCount = new_base_count;
  }

  return result;
}
