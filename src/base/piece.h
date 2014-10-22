/* <base/piece.h>

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

   This module defines:

      * TPiece<>, a way of pointing to a contiguous sequence of values in
        memory;

      * Copy() and ShallowCopy(), overloaded functions for copying data between
        pieces;

      * AsPiece(), overloaded functions for constructing a TPiece<> spanning
        the values in a container or C-style string;

      * AsFixedPiece(), a templatized function for constructing a TPiece<>
        spanning a fixed array;

      * AsTypedPiece(), overloaded functions for converting a blob into a typed
        sequence;

      * Assign(), overloaded functions for copying a piece into a dynamic
        container;

      * AsBlob(), overloaded functions for constructing a TPiece<> spanning an
        object; and

      * AsMutable(), for casting away the constness of TVal.

   NOTE: The notion of pieces is foundational to this code base.  The implicit
   behaviors and overloads defined here help to define a style of coding.  You
   should study this module carefully and practice using it consistently.

   In particular, when you are writing a function which will operate over a
   sequence of values but which will not change the number of values (that is,
   it will not construct new ones or delete existing ones), prefer to take a
   constant reference to a TPiece<>.  The most common example of this pattern
   is a function which takes a string argument.  Rather than define your
   function to take the argument as a const char* or a const &, define it to
   take a const TPiece<const char>&.  AsPiece() overloads exist to convert from
   const char* and const std::string& to TPiece<const char>, and these
   conversions are very cheap.  The result is your function will be more
   general and more easily callable if you use TPiece<>.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <unordered_set>
#include <vector>

#include <base/error.h>
#include <base/not_found_error.h>
#include <base/safe_global.h>
#include <base/slice.h>
#include <base/stl_utils.h>

namespace Base {

  /* Most of the value types we define will have a GetHash() function, but
     standard library types and built-ins will not.  TPiece<> has a GetHash()
     function which combines the hashes of all the elements in the piece.  In
     order to do this, TPiece<TVal> relies on GetHelper<TVal>().  If you define
     a new type and you want to be able to hash pieces consisting of that type,
     then provide a GetHash() function or a specialization of
     GetHashHelper<>().  Specializations of GetHashHelper<>() for standard
     library and built-in types appear at the end of this file. */
  template <typename TVal>
  inline size_t GetHashHelper(const TVal &val) {
    assert(&val);
    return val.GetHash();
  }

  /* An instance of TPiece<TVal> points to a contiguous in-memory sequence of
     TVal instances.  It has value-type semantics and is the size of two
     pointers.  It is suitable to be returned by value, if doing so would aid
     composability.

     Each TPiece<TVal> is defined by a pair of TVal* pointers, Start and Limit.
     The piece starts at the TVal pointed to by Start and continues up to but
     does NOT include the TVal pointed to by Limit.  The size of the piece
     (that is, the number of TVal instances it spans) is given by Limit -
     Start.  TPiece<> prevents Limit from ever being less than Start, but it
     does allow the two to be equal.  If they are equal, then the piece is
     empty; that is, it's size is zero.

     The default constructor for TPiece<> results in an empty piece (with both
     pointers null).  You may always revert a piece fo this state by calling
     Reset().  You may also copy construct and assign from:

        * any compatible TPiece<>,

        * static array of compatible values,

        * a single compatible value,

        * a pair of compatible start/limit pointers, or

        * a compatiable start pointer and a size.

     In all of these cases, the term 'compatible' means a type which is
     pointer-compatible with the type on which the TPiece<> is specialized.
     For example, const char is pointer-compatiable with char (but not the
     other way around).

     TPiece<TVal> is NOT a container in the conventional sense because it
     doesn't control the lifespans of the TVal instances at which it points.
     When you copy an instance of TPiece<TVal>, you are only copying its Start
     and Limit pointers.  The objects at which they point are not copied.  The
     TVal instances remains exactly where they are and both TPiece<TVal>
     instances point at them.

     There are getters and setters for Start and Limit and a getter for Size.
     You cannot set Size directly, but you can adjust the Start or Limit to
     result in the Size you want.  (See AdjustStartToSize() and
     AdjustLimitToSize().)  You can also apply arbitrary deltas to Start and
     Limit.  (See AdjustStart() and AdjustLimit().)

     TPiece<> implicitly casts to bool. The result of such a cast will be true
     iff. the piece is non-empty.

     TPiece<> also defines two overloads of operator[], one for TPos and one
     for TSlice.  The former allows you to pick a single value from the
     sequence, the latter allows you to define a new piece as a subset of an
     existing piece.  CanSlice() and TrySlice() allow you determine if a
     particular slice is possible.  You can also call Constrain() to reduce a
     TPiece<> to some subset of itself.

     A pair TPiece<> instances may be compared (with equality and inequality
     operators) to each other on a lexicographic basis.  The types on which the
     pair are specialized must be compatible (as defined above) and must
     support strict weak ordering based on operator<.  You may also call
     Compare() to get a result similar to that of strcmp().

     The Contains() function determines whether TPiece<> contains a particular
     address or other TPiece<>. */
  template <typename TVal>
  class TPiece {
    public:

    /* Construct an empty piece. */
    TPiece() : Start(0), Limit(0) {
    }

    /* Construct as a copy of a piece of compatible type. */
    template <typename TCompatVal>
    TPiece(const TPiece<TCompatVal> &that) {
      assert(&that);
      Start = that.GetStart();
      Limit = that.GetLimit();
    }

    /* Construct so as to span a single value. */
    TPiece(TVal &that) {
      Start = &that;
      Limit = Start + 1;
    }

    /* Construct a piece from the given start and up to but not including the
       given limit. */
    template <typename TCompatVal>
    TPiece(TCompatVal *start, TCompatVal *limit) : Start(start), Limit(limit) {
      assert(start <= limit);
    }

    /* Construct a piece from the given start and of the given size.  A size of
       zero is ok here, as is a null pointer. */
    template <typename TCompatVal>
    TPiece(TCompatVal *start, size_t size) {
      Start = start;
      Limit = start + size;
    }

    /* Assign as a copy of a piece of compatible type. */
    template <typename TCompatVal>
    TPiece &operator=(const TPiece<TCompatVal> &that) {
      assert(this);
      assert(&that);
      Start = that.GetStart();
      Limit = that.GetLimit();
      return *this;
    }

    /* Assign so as to span a single value. */
    TPiece &operator=(TVal &that) {
      assert(this);
      Start = &that;
      Limit = Start + 1;
      return *this;
    }

    /* Return true iff. the piece is non-empty. */
    operator bool() const {
      assert(this);
      return (Start != Limit);
    }

    /* Return the given element of the piece. */
    TVal &operator[](const TPos &pos) const {
      assert(this);
      size_t size = GetSize();
      ptrdiff_t offset = pos.GetAbsOffset(size);
      assert(offset >= 0 && static_cast<size_t>(offset) < size);
      return Start[offset];
    }

    /* Return a slice of the piece. */
    TPiece operator[](const TSlice &slice) const {
      assert(this);
      assert(&slice);
      size_t start, limit;
      slice.GetAbsPair(GetSize(), start, limit);
      assert(start <= limit);
      return TPiece(Start + start, Start + limit);
    }

    /* Compare this piece with that one on a lexicographical basis and return
       true iff. they are equal. */
    template <typename TCompatVal>
    bool operator==(const TPiece<TCompatVal> &that) const {
      assert(this);
      assert(&that);
      TVal *cursor = Start;
      TCompatVal *that_cursor = that.GetStart();

      for (;
           cursor < Limit && that_cursor < that.GetLimit();
           ++cursor, ++that_cursor) {
        if (!(*cursor == *that_cursor)) {
          return false;
        }
      }

      return (cursor == Limit) && (that_cursor == that.GetLimit());
    }

    /* Compare this piece with that one on a lexicographical basis and return
       true iff. they are not equal. */
    template <typename TCompatVal>
    bool operator!=(const TPiece<TCompatVal> &that) const {
      assert(this);
      assert(&that);
      TVal *cursor = Start;
      TCompatVal *that_cursor = that.GetStart();

      for (;
           cursor < Limit && that_cursor < that.GetLimit();
           ++cursor, ++that_cursor) {
        if (*cursor != *that_cursor) {
          return true;
        }
      }

      return (cursor != Limit) || (that_cursor != that.GetLimit());
    }

    /* Compare this piece with that one on a lexicographical basis and return
       true iff. this one is less than that one. */
    template <typename TCompatVal>
    bool operator<(const TPiece<TCompatVal> &that) const {
      assert(this);
      return (Compare(that) < 0);
    }

    /* Compare this piece with that one on a lexicographical basis and return
       true iff. this one is less than or equal to that one. */
    template <typename TCompatVal>
    bool operator<=(const TPiece<TCompatVal> &that) const {
      assert(this);
      return (Compare(that) <= 0);
    }

    /* Compare this piece with that one on a lexicographical basis and return
       true iff. this one is greater than that one. */
    template <typename TCompatVal>
    bool operator>(const TPiece<TCompatVal> &that) const {
      assert(this);
      return (Compare(that) > 0);
    }

    /* Compare this piece with that one on a lexicographical basis and return
       true iff. this one is greater than or equal to that one. */
    template <typename TCompatVal>
    bool operator>=(const TPiece<TCompatVal> &that) const {
      assert(this);
      return (Compare(that) >= 0);
    }

    /* Apply a delta to the limit of the piece.  The result must not place the
       limit before the start. */
    TPiece &AdjustLimit(ptrdiff_t delta) {
      assert(this);
      Limit += delta;
      assert(Start <= Limit);
      return *this;
    }

    /* Adjust the limit of the piece such that the piece ends up with the given
       size.  The start remains unchanged. */
    TPiece &AdjustLimitToSize(size_t size) {
      assert(this);
      Limit = Start + size;
      return *this;
    }

    /* Apply a delta to the start of the piece.  The result must not place the
       limit before the start. */
    TPiece &AdjustStart(ptrdiff_t delta) {
      assert(this);
      Start += delta;
      assert(Start <= Limit);
      return *this;
    }

    /* Adjust the start of the piece such that the piece ends up with the given
       size.  The limit remains unchanged. */
    TPiece &AdjustStartToSize(size_t size) {
      assert(this);
      Start = Limit - size;
      return *this;
    }

    /* Assign the piece to span from the given start up to but not including
       the given limit.  The limit must not be less than the start, but they
       can be equal.  One or both can be null. */
    template <typename TCompatVal>
    TPiece &Assign(TCompatVal *start, TCompatVal *limit) {
      assert(this);
      assert(start <= limit);
      Start = start;
      Limit = limit;
      return *this;
    }

    /* Assign the piece to have the given start and be of the given size.  A
       size of zero is ok here, as is a null pointer. */
    template <typename TCompatVal>
    TPiece &Assign(TCompatVal *start, size_t size) {
      assert(this);
      Start = start;
      Limit = start + size;
      return *this;
    }

    /* Returns true if the given slice can be made. */
    bool CanSlice(const TSlice &slice) const {
      assert(this);
      assert(&slice);
      return slice.CanGetAbsPair(GetSize());
    }

    /* Compare this piece with that one on a lexicographical basis.  Returns -1
       if this piece sorts before that one, +1 if that piece sorts before this
       one, or zero if the two pieces sort identically.  TVal and TCompatVal
       must support strict-weak ordering relative to each other based on
       operator<. */
    template <typename TCompatVal>
    int Compare(const TPiece<TCompatVal> &that) const {
      assert(this);
      assert(&that);
      TVal *cursor = Start;
      TCompatVal *that_cursor = that.GetStart();

      for (;
           cursor < Limit && that_cursor < that.GetLimit();
           ++cursor, ++that_cursor) {
        if (*cursor < *that_cursor) {
          return -1;
        }

        if (*that_cursor < *cursor) {
          return 1;
        }
      }

      return (cursor < Limit) ? 1 : (that_cursor < that.GetLimit()) ? -1 : 0;
    }

    /* Redefines the piece to just the given slice. */
    TPiece &Constrain(const TSlice &slice) {
      assert(this);
      assert(&slice);
      size_t start, limit;
      slice.GetAbsPair(GetSize(), start, limit);
      Limit = Start + limit;
      Start += start;
      assert(Start <= Limit);
      return *this;
    }

    /* Return true if the given pointer falls within the piece.  The starting
       pointer is considered to be within the piece, but the limiting pointer
       is not. */
    template <typename TCompatVal>
    bool Contains(TCompatVal *ptr) {
      assert(this);
      return (ptr >= Start) && (ptr < Limit);
    }

    /* Return true if the other piece falls within (or is congruent to) this
       one. */
    template <typename TCompatVal>
    bool Contains(const TPiece<TCompatVal> &that) {
      assert(this);
      assert(&that);
      return (Start <= that.Start) && (that.Limit <= Limit);
    }

    /* Return the first element in the piece.  The piece must not be empty.
       (Compare with GetRest().) */
    const TVal &GetHead() const {
      assert(this);
      assert(*this);
      return *Start;
    }

    /* Return the limiting pointer of the piece.  This can be null. */
    TVal *GetLimit() const {
      assert(this);
      return Limit;
    }

    /* Return a new piece which spans all but the first element of this piece.
       This piece must not be empty.  (Compare with GetHead().) */
    TPiece GetRest() const {
      assert(this);
      assert(*this);
      return TPiece(Start + 1, Limit);
    }

    /* Return the number of elements in the piece.  This can be zero. */
    size_t GetSize() const {
      assert(this);
      return Limit - Start;
    }

    /* Return the starting pointer of the piece. */
    TVal *GetStart() const {
      assert(this);
      return Start;
    }

    /* Compute a hash of the contents of the piece.  (TVal must be hashable.)
     */
    size_t GetHash() const {
      assert(this);
      size_t result = 0;

      for (TVal *csr = Start; csr < Limit; ++csr) {
        result = RotatedRight(result, 5) ^ GetHashHelper(*csr);
      }

      return result;
    }

    const TVal &GetTail() const {
      assert(this);
      assert(*this);
      return *(Limit - 1);
    }

    /* Reset to the default-constructed (empty) state. */
    TPiece &Reset() {
      assert(this);
      Start = 0;
      Limit = 0;
      return *this;
    }

    /* Set the limiting pointer of the piece.  The new limit must not be less
       than the existing start. */
    TPiece &SetLimit(TVal *limit) {
      assert(this);
      assert(Start <= limit);
      Limit = limit;
      return *this;
    }

    /* Set the starting pointer of the piece.  The existing limit must not be
       less than the new start. */
    TPiece &SetStart(TVal *start) {
      assert(this);
      assert(start <= Limit);
      Start = start;
      return *this;
    }

    /* Exchange the states of this piece with that one.  This is a guranteed
       no-throw. */
    TPiece &Swap(TPiece &that) {
      assert(this);
      assert(&that);
      std::swap(Start, that.Start);
      std::swap(Limit, that.Limit);
      return *this;
    }

    /* Return a pointer to the instance of val, if possible. */
    template <typename TCompatVal>
    TVal *Find(const TCompatVal &val) const {
      assert(this);
      assert(&val);

      for (auto cur = Start; cur < Limit; ++cur) {
        if (*cur == val) {
          return cur;
        }
      }

      throw TNotFoundError(HERE,
          "Base::TPiece::Find() requires that target element be present");
    }

    /* Return a pointer to an instance of an element in val_set, if possible.
     */
    template <typename TCompatVal>
    TVal *Find(const std::unordered_set<TCompatVal> &val_set) const {
      assert(this);
      assert(&val_set);

      for (auto cur = Start; cur < Limit; ++cur) {
        if (Base::Contains(val_set, *cur)) {
          return cur;
        }
      }

      throw TNotFoundError(HERE,
          "Base::TPiece::Find() requires that target elements be present");
    }

    /* Return a slice of the piece (by out-param), if possible. */
    bool TrySlice(const TSlice &slice, TPiece &out) const {
      assert(this);
      assert(&slice);
      assert(&out);
      size_t start, limit;
      bool success = slice.TryGetAbsPair(GetSize(), start, limit);

      if (success) {
        out.Assign(Start + start, Start + limit);
      }

      return success;
    }

    /* An instance of an empty piece. */
    static const TSafeGlobal<TPiece> Empty;

    private:
    /* The pointers to the start and limit of the piece.  Limit will never be
       less than Start, but they could be equal.  One or both could be null. */
    TVal *Start, *Limit;

    /* Factory for the global Empty. */
    static TPiece *NewEmpty() {
      return new TPiece;
    }
  };

  /* Implementation of global Empty. */
  template <typename TVal>
  const TSafeGlobal<TPiece<TVal> > TPiece<TVal>::Empty(NewEmpty);

  /* ShallowCopy() copies the elements from a source piece into a destination
     piece, overwriting the elements already present in the destination piece.
     If you do not specify a number of elements to copy, then ShallowCopy()
     will copy the number of elements in the source or destination, whichever
     is lesser.  The copy is done with memcpy(), so the types on which the two
     pieces are specialized must be of identical size and must not require deep
     copying behavior. */

  /* Shallow-copy with explicit size. */
  template <typename TDestVal, typename TSrcVal>
  const TPiece<TDestVal> &ShallowCopy(const TPiece<TDestVal> &dest,
      const TPiece<TSrcVal> &src, size_t size) {
    assert(&dest);
    assert(&src);
    assert(dest.GetSize() >= size);
    assert(src.GetSize() >= size);
    assert(sizeof(TDestVal) == sizeof(TSrcVal));
    memcpy(dest.GetStart(), src.GetStart(), size * sizeof(TDestVal));
    return dest;
  }

  /* Shallow-copy with implicit size. */
  template <typename TDestVal, typename TSrcVal>
  const TPiece<TDestVal> &ShallowCopy(const TPiece<TDestVal> &dest,
      const TPiece<TSrcVal> &src) {
    assert(&dest);
    assert(&src);
    return ShallowCopy(dest, src, std::min(dest.GetSize(), src.GetSize()));
  }

  /* Copy() copies the elements from a source piece into a destination piece,
     overwriting the elements already present in the destination piece.  If you
     do not specify a number of elements to copy, then Copy() will copy the
     number of elements in the source or destination, whichever is lesser.  The
     copy is done with the assignment operator of the type on which the
     destination piece is specialized. */

  /* Copy with explicit size. */
  template <typename TDestVal, typename TSrcVal>
  const TPiece<TDestVal> &Copy(const TPiece<TDestVal> &dest,
      const TPiece<TSrcVal> &src, size_t size) {
    assert(&dest);
    assert(&src);
    assert(dest.GetSize() >= size);
    assert(src.GetSize() >= size);
    TSrcVal *src_val = src.GetStart();
    TDestVal *dest_val = dest.GetStart();

    for (size_t i = 0; i < size; ++i) {
      *dest_val++ = *src_val++;
    }

    return dest;
  }

  /* Copy with implicit size. */
  template <typename TDestVal, typename TSrcVal>
  const TPiece<TDestVal> &Copy(const TPiece<TDestVal> &dest,
      const TPiece<TSrcVal> &src) {
    assert(&dest);
    assert(&src);
    return Copy(dest, src, std::min(dest.GetSize(), src.GetSize()));
  }

  /* The following explicit specializatons redefine Copy() to be the same as
     ShallowCopy() for the built-in types. */

  /* Explicit specialization for char. */
  template <typename TVal>
  const TPiece<char> &Copy(const TPiece<char> &dest, const TPiece<TVal> &src,
      size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for wchar_t. */
  template <typename TVal>
  const TPiece<wchar_t> &Copy(const TPiece<wchar_t> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for float. */
  template <typename TVal>
  const TPiece<float> &Copy(const TPiece<float> &dest, const TPiece<TVal> &src,
      size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for double. */
  template <typename TVal>
  const TPiece<double> &Copy(const TPiece<double> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for bool. */
  template <typename TVal>
  const TPiece<bool> &Copy(const TPiece<bool> &dest, const TPiece<TVal> &src,
      size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for int8_t. */
  template <typename TVal>
  const TPiece<int8_t> &Copy(const TPiece<int8_t> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for int16_t. */
  template <typename TVal>
  const TPiece<int16_t> &Copy(const TPiece<int16_t> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for int32_t. */
  template <typename TVal>
  const TPiece<int32_t> &Copy(const TPiece<int32_t> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for int64_t. */
  template <typename TVal>
  const TPiece<int64_t> &Copy(const TPiece<int64_t> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for uint8_t. */
  template <typename TVal>
  const TPiece<uint8_t> &Copy(const TPiece<uint8_t> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for uint16_t. */
  template <typename TVal>
  const TPiece<uint16_t> &Copy(const TPiece<uint16_t> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for uint32_t. */
  template <typename TVal>
  const TPiece<uint32_t> &Copy(const TPiece<uint32_t> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* Explicit specialization for uint64_t. */
  template <typename TVal>
  const TPiece<uint64_t> &Copy(const TPiece<uint64_t> &dest,
      const TPiece<TVal> &src, size_t size) {
    return ShallowCopy(dest, src, size);
  }

  /* AsPiece() constructs a TPiece<> spanning the values in a standard
     container or C-style string. */

  /* Constructs a TPiece<const char> spanning a constant C-style string. */
  inline TPiece<const char> AsPiece(const char *that) {
    assert(that);
    return TPiece<const char>(that, strlen(that));
  }

  /* Constructs a TPiece<char> spanning a C-style string. */
  inline TPiece<char> AsPiece(char *that) {
    assert(that);
    return TPiece<char>(that, strlen(that));
  }

  /* Constructs a TPiece<const wchar_t> spanning a constant C-style wide
     string. */
  inline TPiece<const wchar_t> AsPiece(const wchar_t *that) {
    assert(that);
    return TPiece<const wchar_t>(that, wcslen(that));
  }

  /* Constructs a TPiece<wchar_t> spanning a C-style wide string. */
  inline TPiece<wchar_t> AsPiece(wchar_t *that) {
    assert(that);
    return TPiece<wchar_t>(that, wcslen(that));
  }

  /* Constructs a TPiece<const TVal> spanning a constant standard vector of the
     same type. */
  template <typename TVal, typename TAlloc>
  TPiece<const TVal> AsPiece(const std::vector<TVal, TAlloc> &that) {
    assert(&that);
    return TPiece<const TVal>(&that[0], that.size());
  }

  /* Constructs a TPiece<TVal> spanning a standard vector of the same type. */
  template <typename TVal, typename TAlloc>
  TPiece<TVal> AsPiece(std::vector<TVal, TAlloc> &that) {
    assert(&that);
    return TPiece<TVal>(&that[0], that.size());
  }

  /* Constructs a TPiece<const TVal> spanning a constant standard basic_string
     of the same type. */
  template <typename TVal, typename TTraits, typename TAlloc>
  TPiece<const TVal> AsPiece(
      const std::basic_string<TVal, TTraits, TAlloc> &that) {
    assert(&that);
    return TPiece<const TVal>(that.data(), that.size());
  }

  /* Constructs a TPiece<TVal> spanning a standard basic_string of the same
     type. */
  template <typename TVal, typename TTraits, typename TAlloc>
  TPiece<TVal> AsPiece(std::basic_string<TVal, TTraits, TAlloc> &that) {
    assert(&that);
    return TPiece<TVal>(const_cast<TVal *>(that.data()), that.size());
  }

  /* Construct a TPiece spanning a fixed array. */
  template <typename TVal, size_t Size>
  TPiece<TVal> AsFixedPiece(TVal (&that)[Size]) {
    return TPiece<TVal>(that, that + Size);
  }

  /* Contruct a TPiece of an explicit type from a char blob. */
  template <typename TVal>
  TPiece<TVal> AsTypedPiece(const TPiece<char> &that) {
    return TPiece<TVal>(reinterpret_cast<TVal*>(that.GetStart()),
        reinterpret_cast<TVal*>(that.GetLimit()));
  }

  /* Contruct a TPiece of an explicit type from a read-only char blob. */
  template <typename TVal>
  TPiece<TVal> AsTypedPiece(const TPiece<const char> &that) {
    return TPiece<TVal>(reinterpret_cast<TVal*>(that.GetStart()),
        reinterpret_cast<TVal*>(that.GetLimit()));
  }

  /* Assign() copies the elements of a piece into a dynamic container of
     compatible type.  The container will be resized to the size of the
     piece. */

  /* Copy from a piece into a standard vector. */
  template <typename TVal, typename TAlloc, typename TCompatVal>
  std::vector<TVal, TAlloc> &Assign(std::vector<TVal, TAlloc> &dest,
      const TPiece<TCompatVal> &src) {
    dest.assign(src.GetStart(), src.GetLimit());
    return dest;
  }

  /* Copy from a piece into a standard basic_string. */
  template <typename TVal, typename TTraits, typename TAlloc,
            typename TCompatVal>
  std::basic_string<TVal, TTraits, TAlloc> &
  Assign(std::basic_string<TVal, TTraits, TAlloc> &dest,
      const TPiece<TCompatVal> &src) {
    return dest.assign(src.GetStart(), src.GetSize());
  }

  /* Constructs a TPiece<const unsigned char> spanning a constant C-style
     string. */
  inline TPiece<const unsigned char> AsUnsignedPiece(const char *that) {
    assert(that);
    return TPiece<const unsigned char>(
        reinterpret_cast<const unsigned char *>(that), strlen(that));
  }

  /* AsBlob() constructs a TPiece<> spanning an object, essentially throwing
     away the object's type and allowing you to deal with it as a sequence of
     bytes. */

  /* Construct a TPiece<const char> spanning a constant object. */
  template <typename TObj>
  TPiece<const char> AsBlob(const TObj &obj) {
    return TPiece<const char>(
        reinterpret_cast<const char *>(&obj), sizeof(obj));
  }

  /* Construct a TPiece<char> spanning an object. */
  template <typename TObj>
  TPiece<char> AsBlob(TObj &obj) {
    return TPiece<char>(reinterpret_cast<char *>(&obj), sizeof(obj));
  }

  /* AsMutable() constructs a TPiece<> which is no longer const from one that
     is.  Caveat emptor. */
  template <typename TVal>
  TPiece<TVal> AsMutable(const TVal &that) {
    return TPiece<TVal>(const_cast<TVal *>(that.GetStart()), that.GetSize());
  }

  /* Converts a piece to a C string. CALLER NEEDS TO DESTROY THE STRING. */
  unsigned char *BuildCStr(const TPiece<const unsigned char> &piece);
  char *BuildCStr(const TPiece<const char> &piece);

  /* Special hasher for char. */
  template <>
  inline size_t GetHashHelper(const char &val) {
    assert(&val);
    return val;
  }

  /* Special hasher for unsigned char. */
  template <>
  inline size_t GetHashHelper(const unsigned char &val) {
    assert(&val);
    return val;
  }

  /* Special hasher for std::string. */
  template <>
  inline size_t GetHashHelper(const std::string &val) {
    assert(&val);
    return AsPiece(val).GetHash();
  }

  std::ostream &operator<<(std::ostream &strm,
      const Base::TPiece<const char> &piece);

}  // Base

namespace std {

  template <typename TVal>
  struct hash<Base::TPiece<TVal>> {
    size_t operator()(Base::TPiece<TVal> val) const {
      return val.GetHash();
    }

    typedef size_t result_type;
    typedef Base::TPiece<TVal> argument_type;
  };  // hash

  /* A standard swapper for Base::TPiece<>. */
  template <typename TVal>
  void swap(Base::TPiece<TVal> &lhs, Base::TPiece<TVal> &rhs) {
    lhs.Swap(rhs);
  }

}  // std
