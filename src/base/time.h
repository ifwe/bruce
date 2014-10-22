/* <base/time.h>

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

   Provide a time class that wraps struct timespec and provides operators.
 */

#pragma once

#include <time.h>

#include <base/no_copy_semantics.h>

namespace Base {

  class TTime {
    public:
    TTime() {
      Time.tv_sec = 0;
      Time.tv_nsec = 0;
    }

    TTime(time_t sec, long nsec) {
      Time.tv_sec = sec;
      Time.tv_nsec = nsec;
    }

    void Now(clockid_t clk_id = CLOCK_REALTIME) {
      clock_gettime(clk_id, &Time);
    }

    size_t Remaining(clockid_t clk_id = CLOCK_REALTIME) const {
      TTime t;
      t.Now(clk_id);
      if (*this <= t) return 0;
      TTime diff = *this - t;
      return diff.Sec() * 1000 + diff.Nsec() / 1000000;
    }

    size_t RemainingMicroseconds(clockid_t clk_id = CLOCK_REALTIME) const {
      TTime t;
      t.Now(clk_id);

      if (*this <= t) {
        return 0;
      }

      TTime diff = *this - t;
      return diff.Sec() * 1000000 + diff.Nsec() / 1000;
    }

    time_t Sec() const {
      return Time.tv_sec;
    }

    long Nsec() const {
      return Time.tv_nsec;
    }

    TTime &operator=(const TTime &rhs);

    bool operator==(const TTime &rhs) const;

    bool operator!=(const TTime &rhs) const;

    bool operator<(const TTime &rhs) const;

    bool operator>(const TTime &rhs) const;

    bool operator<=(const TTime &rhs) const;

    bool operator>=(const TTime &rhs) const;

    TTime &operator+=(const TTime &rhs);

    TTime &operator-=(const TTime &rhs);

    const TTime operator+(const TTime &rhs) const;

    const TTime operator-(const TTime &rhs) const;

    TTime &operator+=(size_t msec);

    TTime &operator-=(size_t msec);

    TTime &AddMicroseconds(size_t usec);

    TTime &SubtractMicroseconds(size_t usec);

    private:
    struct timespec Time;
  };  // TTime

}  // Base
