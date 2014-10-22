/* <base/pos.h>

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

   A position in a sequence, relative to ether the start or limit of the
   sequence.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <ostream>

#include <base/no_default_case.h>
#include <base/safe_global.h>

namespace Base {

  /* A value type for specifing a position in a sequence, relative to either
     the start or limit of the sequence.  Our position is represented as a
     signed offset into the sequence, allowing us to refer to positions that
     are outside of the sequence proper. */
  class TPos {
    public:
    /* We offer two different directions in which to interpret offsets. */
    enum TDir {
      /* Our direction is 'forward', meaning from the start and toward the end
         of the sequence.  Our offset will therefore be interpreted thus:

             -1        the first element before the start of the sequence
                       (ReverseLimit)
              0        the first element of the sequence (Start)
              1        the second element of the sequence
             ...
             size - 2  the next-to-last element of the sequence
             size - 1  the last element of the sequence (ReverseStart)
             size      the first element beyond the end of the sequence (Limit)

         This is by far the more common way of thinking about a sequence and
         is, therefore, the default direction for a TPos. */
      Forward,

      /* Our direction is 'reverse', meaning from the end and toward the start
         of the sequence.  Our offset will therefore be interpreted thus:

             -1        the first element beyond the end of the sequence (Limit)
              0        the last element of the sequence (ReverseStart)
              1        the next-to-last element of the sequence
             ...
             size - 2  the second element of the sequence
             size - 1  the first element of the sequence (Start)
             size      the first element before the start of the sequence
                       (ReverseLimit)

         This point of view is less common than the other so, when you want it,
         you must request it specifically. */
      Reverse
    };

    /* Initializing constructor, fully defaultable. */
    TPos(ptrdiff_t offset = 0, TDir dir = Forward)
        : Offset(offset), Dir(dir) {
    }

    /* Returns true iff. this pos is the same as that one. */
    bool operator==(const TPos &that) const {
      assert(this);
      assert(&that);
      return Offset == that.Offset && Dir == that.Dir;
    }

    /* Returns true iff. this pos is not the same as that one. */
    bool operator!=(const TPos &that) const {
      assert(this);
      assert(&that);
      return Offset != that.Offset || Dir != that.Dir;
    }

    /* Compute the absolute offset represented by this TPos when projected onto
       a sequence of the given size.  The absolute offset is always relative to
       the start of the sequence. */
    ptrdiff_t GetAbsOffset(size_t size) const {
      assert(this);
      ptrdiff_t result;

      switch (Dir) {
        case Forward: {
          result = Offset;
          break;
        }
        case Reverse: {
          result = size - Offset - 1;
          break;
        }
        NO_DEFAULT_CASE;
      }

      return result;
    }

    /* Return to the default constructed state. */
    TPos &Reset() {
      assert(this);
      Offset = 0;
      Dir = Forward;
      return *this;
    }

    /* Swap this pos with that one. */
    TPos &Swap(TPos &that) {
      assert(this);
      std::swap(Offset, that.Offset);
      std::swap(Dir, that.Dir);
      return *this;
    }

    /* Our offset, relative to our direction.  This member is public because we
       enforce no rules about it. */
    ptrdiff_t Offset;

    /* Our direction.  This member is public because we enforce no rules about
       it. */
    TDir Dir;

    /* Get the start position for the given direction of iteration. */
    static const TPos &GetStart(TDir dir) {
      const TSafeGlobal<TPos> *result;

      switch (dir) {
        case Forward: {
          result = &Start;
          break;
        }
        case Reverse: {
          result = &ReverseStart;
          break;
        }
        NO_DEFAULT_CASE;
      }

      return **result;
    }

    /* Get the limit position for the given direction of iteration. */
    static const TPos &GetLimit(TDir dir) {
      const TSafeGlobal<TPos> *result;

      switch (dir) {
        case Forward: {
          result = &Limit;
          break;
        }
        case Reverse: {
          result = &ReverseLimit;
          break;
        }
        NO_DEFAULT_CASE;
      }

      return **result;
    }

    /* A pair of constants referring to the start and limit of a sequence when
       iterating forward.  Forward iteration is so much more common than the
       reverse that we drop the 'Forward' prefix for these names. */
    static const TSafeGlobal<TPos> Start, Limit;

    /* A pair of constants referring to the start and limit of a sequence when
       iterating backward. */
    static const TSafeGlobal<TPos> ReverseStart, ReverseLimit;

    private:
    /* Factory for Start global. */
    static TPos *NewStart() {
      return new TPos(0, Forward);
    }

    /* Factory for Limit global. */
    static TPos *NewLimit() {
      return new TPos(-1, Reverse);
    }

    /* Factory for ReverseStart global. */
    static TPos *NewReverseStart() {
      return new TPos(0, Reverse);
    }

    /* Factory for ReverseLimit global. */
    static TPos *NewReverseLimit() {
      return new TPos(-1, Forward);
    }
  };  // TPos

  /* Standard stream inserter for Base::TPos. */
  inline std::ostream &operator<<(std::ostream &strm, const TPos &that) {
    char dir_char;

    switch (that.Dir) {
      case Base::TPos::Forward: {
        dir_char = 'F';
        break;
      }
      case Base::TPos::Reverse: {
        dir_char = 'R';
        break;
      }
      NO_DEFAULT_CASE;
    }

    return strm << that.Offset << dir_char;
  }

}  // Base

namespace std {

  /* A standard swapper for Base::TPos. */
  inline void swap(Base::TPos &lhs, Base::TPos &rhs) {
    lhs.Swap(rhs);
  }

}  // std
