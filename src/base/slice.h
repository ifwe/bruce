/* <base/slice.h>

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

   A value type for specifying how to slice a sequence.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <ostream>

#include <base/pos.h>
#include <base/safe_global.h>

namespace Base {

  /* A value type for specifying how to slice a sequence.  The slice extends
     from its start position up to (but not including) its limit.  The
     endpoints are instances of TPos, which means you can specify them relative
     to the start or end of the sequence.

     Because TPos can construct from an integer, you can generally specify
     slices like this: TSlice(0, 10).  (This would be the slice extending from
     element #0 up to but not including element #10; in other words, the first
     10 elements in the sequence.)  This should match the intuition you
     developed when working with arrays.

     You can also use the constants defined in TPos to define slices.  For
     example, TSlice(10, TPos::Limit) means 'the slice starting at element #10
     and continuing through the rest of the sequence'.  Notice that, due to the
     defaults provided in the TSlice constructor, you can say the same thing by
     just saying TSlice(10).

     You can also construct explicit instances of TPos to get specific slices.
     For example, TSlice(TPos(10, TPos::Reverse), TPos(7, TPos::Reverse)) means
     'the slice starting 10 elements back from the end and continuing for the
     next 3 elements'. */
  class TSlice {
    public:
    /* Initializing constructor, fully defaultable.  Notice that the default is
       the 'everything' slice. */
    TSlice(const TPos &start = *TPos::Start, const TPos &limit = *TPos::Limit)
        : Start(start), Limit(limit) {
    }

    /* Returns true iff. this slice is the same as that one. */
    bool operator==(const TSlice &that) const {
      assert(this);
      assert(&that);
      return Start == that.Start && Limit == that.Limit;
    }

    /* Returns true iff. this slice is not the same as that one. */
    bool operator!=(const TSlice &that) const {
      assert(this);
      assert(&that);
      return Start != that.Start || Limit != that.Limit;
    }

    /* Converts a slice based on a non-char sequence into a slice of a char
       sequence. */
    TSlice AsByteSlice(size_t seq_size, size_t elem_size) const {
      assert(this);
      size_t start, limit;
      GetAbsPair(seq_size / elem_size, start, limit);
      start *= elem_size;
      limit *= elem_size;
      return TSlice(start, limit);
    }

    /* Returns true iff. this slice is valid when applied to a sequence of the
       given size. */
    bool CanGetAbsPair(size_t size) const {
      assert(this);
      size_t dummy1, dummy2;
      return TryGetAbsPair(size, dummy1, dummy2);
    }

    /* Returns (via out-parameters) the absolute offsets into a sequence of the
       given size specified by this slice.  The size must be large enough to
       accomodate the slice. */
    void GetAbsPair(size_t size, size_t &start, size_t &limit) const {
      assert(this);
      bool success = TryGetAbsPair(size, start, limit);
      assert(success);
    }

    /* Returns true if the slice amounts to a single pos; that is, if the start
       and limit are the same relative position.  This doesn't detect the case
       where two different relative positions map to the same absolute
       position.  You'll need to use same variation on GetAbsPair() for that.
     */
    bool IsPos() const {
      assert(this);
      return Start == Limit;
    }

    /* Tries to apply this slice to a sequence of the given size.  If the size
       is large enough to accomodate us, then this function sets the two
       out-parameters to the correct absolute offsets and returns true;
       otherwise, this function leaves the out-parameters alone and the returns
       false. */
    bool TryGetAbsPair(size_t size, size_t &start, size_t &limit) const {
      assert(this);
      assert(&start);
      assert(&limit);
      ptrdiff_t start_offset = Start.GetAbsOffset(size),
                limit_offset = Limit.GetAbsOffset(size);
      bool success =
        ((start_offset >= 0) && (start_offset <= static_cast<int>(size)) &&
         (limit_offset >= start_offset) &&
         (limit_offset <= static_cast<int>(size)));

      if (success) {
        start = start_offset;
        limit = limit_offset;
      }

      return success;
    }

    /* The default (everything) slice. */
    TSlice &Reset() {
      assert(this);
      Start = *TPos::Start;
      Limit = *TPos::Limit;
      return *this;
    }

    /* Swap this slice with that one. */
    TSlice &Swap(TSlice &that) {
      assert(this);
      std::swap(Start, that.Start);
      std::swap(Limit, that.Limit);
      return *this;
    }

    /* The start and limit (relative) positions of the slice.  These are
       directly accessible and freely modifiable. */
    TPos Start, Limit;

    /* We define three global slices for convenience.  Each represents some
       sort of degenerate case.

         All: the slice which includes entire sequence;
         AtLimit: the slice of the single position at the limit of the
             sequence; and
         AtStart: the slice of the single position at the start of the
             sequence. */
    static const TSafeGlobal<TSlice> All, AtLimit, AtStart;

    private:
    /* The factory for the All global. */
    static TSlice *NewAll() {
      return new TSlice;
    }

    /* The factory for the AtLimit global. */
    static TSlice *NewAtLimit() {
      return new TSlice(*TPos::Limit, *TPos::Limit);
    }

    /* The factory for the AtStart global. */
    static TSlice *NewAtStart() {
      return new TSlice(*TPos::Start, *TPos::Start);
    }
  };  // TSlice

  /* Standard stream inserter for Base::TSlice.  NOTE: This should go away when
     we switch to our own I/O classes. */
  inline std::ostream &operator<<(std::ostream &strm, const TSlice &that) {
    return strm << '[' << that.Start << ", " << that.Limit << ')';
  }
}

namespace std {

  /* A standard swapper for Base::TSlice. */
  inline void swap(Base::TSlice &lhs, Base::TSlice &rhs) {
    lhs.Swap(rhs);
  }

}  // std
