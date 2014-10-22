/* <base/random_exp_backoff.h>

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

   This generates values that can be used for random exponential backoff.  The
   intended use is for things such as delay generation for resending messages
   after communication failures.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <functional>

namespace Base {

  class TRandomExpBackoff final {
    public:
    /* See description of method NextValue() for explanation of parameters
       'initial_count' and 'max_double'.  'random_number_generator' is a user-
       supplied function that generates random unsigned integers. */
    TRandomExpBackoff(size_t initial_count, size_t max_double,
        const std::function<unsigned ()> &random_number_generator)
        : RandomNumberGenerator(random_number_generator),
          InitialCount(initial_count),
          MaxDouble(max_double),
          BaseCount(initial_count),
          DoubleTimesLeft(max_double)
    {}

    size_t GetInitialCount() const {
      assert(this);
      return InitialCount;
    }

    size_t GetMaxDouble() const {
      assert(this);
      return MaxDouble;
    }

    size_t GetCurrentBaseCount() const {
      assert(this);
      return BaseCount;
    }

    size_t GetCurrentDoubleTimesLeft() const {
      assert(this);
      return DoubleTimesLeft;
    }

    /* A call to this method generates a random value between 0.5 and 1.5 times
       the current "base value", doubles the base value unless it has already
       been doubled the number of times given by the 'max_double' constructor
       parameter, and then returns the generated value.  The base value starts
       out as the 'initial_count' value passed to the constructor.  Note that
       strict monotonically increasing values are not guaranteed.  For
       instance, let's say the base value is N on one call, and a random value
       of (1.5 * N) is generated.  On the next call, the base value will be
       (2 * N), and a random value of (0.5 * (2 * N)) might be generated, which
       is less than the previous value. */
    size_t NextValue();

    /* This reinitializes the object to its initially constructed state,
       allowing it to generate a new sequence of values. */
    void Reset() {
      assert(this);

      BaseCount = InitialCount;
      DoubleTimesLeft = MaxDouble;
    }

    /* This overload of Reset() functions as above, but lets new initial_count
       and max_double values be chosen. */
    void Reset(size_t initial_count, size_t max_double) {
      InitialCount = initial_count;
      MaxDouble = max_double;
      Reset();
    }

    private:
    const std::function<unsigned ()> RandomNumberGenerator;

    size_t InitialCount;

    size_t MaxDouble;

    size_t BaseCount;

    size_t DoubleTimesLeft;
  };  // TRandomExpBackoff

}  // Base
