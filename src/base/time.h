/* <base/time.h>

   ----------------------------------------------------------------------------
   Copyright 2010-2013 Tagged

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

  /* TODO */
  class TTime {
    public:

    /* TODO */
    TTime() {
      Time.tv_sec = 0;
      Time.tv_nsec = 0;
    }

    /* TODO */
    TTime(time_t sec, long nsec) {
      Time.tv_sec = sec;
      Time.tv_nsec = nsec;
    }

    /* TODO */
    void Now(clockid_t clk_id = CLOCK_REALTIME) {
      clock_gettime(clk_id, &Time);
    }

    /* TODO */
    size_t Remaining(clockid_t clk_id = CLOCK_REALTIME) const {
      TTime t;
      t.Now(clk_id);
      if (*this <= t) return 0;
      TTime diff = *this - t;
      return diff.Sec() * 1000 + diff.Nsec() / 1000000;
    }

    /* TODO */
    size_t RemainingMicroseconds(clockid_t clk_id = CLOCK_REALTIME) const {
      TTime t;
      t.Now(clk_id);
      if (*this <= t) return 0;
      TTime diff = *this - t;
      return diff.Sec() * 1000000 + diff.Nsec() / 1000;
    }

    /* TODO */
    time_t Sec() const {
      return Time.tv_sec;
    }

    /* TODO */
    long Nsec() const {
      return Time.tv_nsec;
    }

    /* TODO */
    TTime &operator=(const TTime &rhs);

    /* TODO */
    bool operator==(const TTime &rhs) const;

    /* TODO */
    bool operator!=(const TTime &rhs) const;

    /* TODO */
    bool operator<(const TTime &rhs) const;

    /* TODO */
    bool operator>(const TTime &rhs) const;

    /* TODO */
    bool operator<=(const TTime &rhs) const;

    /* TODO */
    bool operator>=(const TTime &rhs) const;

    /* TODO */
    TTime &operator+=(const TTime &rhs);

    /* TODO */
    TTime &operator-=(const TTime &rhs);

    /* TODO */
    const TTime operator+(const TTime &rhs) const;

    /* TODO */
    const TTime operator-(const TTime &rhs) const;

    /* TODO */
    TTime &operator+=(size_t msec);

    /* TODO */
    TTime &operator-=(size_t msec);

    /* TODO */
    TTime &AddMicroseconds(size_t usec);

    /* TODO */
    TTime &SubtractMicroseconds(size_t usec);

    private:

    /* TODO */
    struct timespec Time;

  };

}
