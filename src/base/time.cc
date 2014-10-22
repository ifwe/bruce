/* <base/time.cc>

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

   Implements <base/time.h>.
 */

#include <base/time.h>

using namespace Base;

TTime &TTime::operator=(const TTime &rhs) {
  if (this == &rhs) {
    return *this;
  }

  Time = rhs.Time;
  return *this;
}

bool TTime::operator==(const TTime &rhs) const {
  return (Time.tv_sec == rhs.Time.tv_sec) &&
         (Time.tv_nsec == rhs.Time.tv_nsec);
}

bool TTime::operator!=(const TTime &rhs) const {
  return (Time.tv_sec != rhs.Time.tv_sec) ||
         (Time.tv_nsec != rhs.Time.tv_nsec);
}

bool TTime::operator<(const TTime &rhs) const {
  return (Time.tv_sec < rhs.Time.tv_sec) ||
      ((Time.tv_sec == rhs.Time.tv_sec) && (Time.tv_nsec < rhs.Time.tv_nsec));
}

bool TTime::operator>(const TTime &rhs) const {
  return (Time.tv_sec > rhs.Time.tv_sec) ||
      ((Time.tv_sec == rhs.Time.tv_sec) && (Time.tv_nsec > rhs.Time.tv_nsec));
}

bool TTime::operator<=(const TTime &rhs) const {
  return (*this < rhs) || (*this == rhs);
}

bool TTime::operator>=(const TTime &rhs) const {
  return (*this > rhs) || (*this == rhs);
}

TTime &TTime::operator+=(const TTime &rhs) {
  Time.tv_sec += rhs.Time.tv_sec;
  Time.tv_nsec += rhs.Time.tv_nsec;

  if (Time.tv_nsec >= 1000000000L) {
    ++Time.tv_sec;
    Time.tv_nsec -= 1000000000L;
  }

  return *this;
}

TTime &TTime::operator-=(const TTime &rhs) {
  Time.tv_sec -= rhs.Time.tv_sec;
  Time.tv_nsec -= rhs.Time.tv_nsec;

  if (Time.tv_nsec < 0L) {
    --Time.tv_sec;
    Time.tv_nsec += 1000000000L;
  }

  return *this;
}

const TTime TTime::operator+(const TTime &rhs) const {
  return TTime(*this) += rhs;
}

const TTime TTime::operator-(const TTime &rhs) const {
  return TTime(*this) -= rhs;
}

TTime &TTime::operator+=(size_t msec) {
  return *this += TTime(msec / 1000, (msec % 1000) * 1000000);
}

TTime &TTime::operator-=(size_t msec) {
  return *this -= TTime(msec / 1000, (msec % 1000) * 1000000);
}

TTime &TTime::AddMicroseconds(size_t usec) {
  return *this += TTime(usec / 1000000, (usec % 1000000) * 1000);
}

TTime &TTime::SubtractMicroseconds(size_t usec) {
  return *this -= TTime(usec / 1000000, (usec % 1000000) * 1000);
}
