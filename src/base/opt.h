/* <base/opt.h>

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

   An optional value.
 */

#pragma once

#include <cassert>
#include <istream>
#include <ostream>
#include <utility>

#include <base/safe_global.h>
#include <io/binary_input_stream.h>
#include <io/binary_output_stream.h>

namespace Base {

  /* An optional value; that is, a value which may or may not be known.  This
     is a value type, and TVal must also be a value type.  The domain of TVal
     is augmented with the additional state of being unknown.

     The interface to TOpt<> looks like that of a smart-pointer.  The implicit
     cast to bool and deference operators are overloaded.  You can therefore do
     things like this:

        void OtherFunc(const TFoo &foo);

        void ThisFunc(const TOpt<TFoo> &opt_foo) {
          if (opt_foo) {
            OtherFunc(*opt_foo);
          }
        }

     Note that deferencing an unknown TOpt<> is illegal.  You can, however,
     call MakeKnown() to force an unknown TOpt<TVal> to construct a TVal if it
     doesn't already have one.

     The storage for the instance of TVal is allocated within the TOpt<TVal>
     instance, but remains uninitialized until the TVal is referred to.  This
     allows you to pass instances of TOpt<> around without worrying about extra
     heap allocations.  If TVal has light-weight copying semantics (such as a
     COW scheme), then it plausible to pass instances of TOpt<TVal> by value.

     A constant of the type TOpt<TVal> and with the unknown value is defined
     for you as a convenience.  Refer to it as TOpt<TVal>::Unknown.

     There is a std stream inserter for this type. */
  template <typename TVal>
  class TOpt {
    public:
    /* Default-construct as an unknown. */
    TOpt()
        : Val(nullptr) {
    }

    /* Move contructor.  We get the donor's value (if any) and the donor
       becomes unknown. */
    TOpt(TOpt &&that) {
      assert(&that);

      if (that.Val) {
        Val = new (Storage) TVal(std::move(*that.Val));
        that.Reset();
      } else {
        Val = nullptr;
      }
    }

    /* Copy constructor. */
    TOpt(const TOpt &that) {
      assert(&that);
      Val = that.Val ? new (Storage) TVal(*that.Val) : nullptr;
    }

    /* Construct by moving the given value into our internal storage.
       What remains of the donor is up to TVal. */
    TOpt(TVal &&that) {
      assert(&that);
      Val = new (Storage) TVal(std::move(that));
    }

    /* Construct with a copy of the given value. */
    TOpt(const TVal &that) {
      assert(&that);
      Val = new (Storage) TVal(that);
    }

    /* If we're known, destroy our value as we go. */
    ~TOpt() {
      assert(this);
      Reset();
    }

    /* Swaperator. */
    TOpt &operator=(TOpt &&that) {
      assert(this);
      assert(&that);

      if (this != &that) {
        if (Val && that.Val) {
          std::swap(*Val, *that.Val);
        } else if (that.Val) {
          Val = new (Storage) TVal(std::move(*that.Val));
          that.Reset();
        } else if (Val) {
          that.Val = new (that.Storage) TVal(std::move(*Val));
          Reset();
        }
      }

      return *this;
    }

    /* Assignment operator. */
    TOpt &operator=(const TOpt &that) {
      assert(this);
      return (this != &that) ? *this = TOpt(that) : *this;
    }

    /* If we're already known, swap our value with the given one;
       otherwise, move-construct the value into our storage. */
    TOpt &operator=(TVal &&that) {
      assert(this);
      assert(&that);

      if (Val != &that) {
        if (Val) {
          std::swap(*Val, that);
        } else {
          Val = new (Storage) TVal(std::move(that));
        }
      }

      return *this;
    }

    /* Assume a copy of the given value.  If we weren't known before, we will
       be now. */
    TOpt &operator=(const TVal &that) {
      assert(this);
      return (Val != &that) ? *this = TOpt(that) : *this;
    }

    /* True iff. we're known. */
    operator bool() const {
      assert(this);
      return Val != nullptr;
    }

    /* Our value.  We must already be known. */
    const TVal &operator*() const {
      assert(this);
      assert(Val);
      return *Val;
    }

    /* Our value.  We must already be known. */
    TVal &operator*() {
      assert(this);
      assert(Val);
      return *Val;
    }

    /* Our value.  We must already be known. */
    const TVal *operator->() const {
      assert(this);
      assert(Val);
      return Val;
    }

    /* Our value.  We must already be known. */
    TVal *operator->() {
      assert(this);
      assert(Val);
      return Val;
    }

    /* A pointer to our value.  We must already be known. */
    const TVal *Get() const {
      assert(this);
      assert(Val);
      return Val;
    }

    /* A pointer to our value.  We must already be known. */
    TVal *Get() {
      assert(this);
      assert(Val);
      return Val;
    }

    /* True iff. we're known. */
    bool IsKnown() const {
      assert(this);
      return Val != nullptr;
    }

    /* True iff. we're not known. */
    bool IsUnknown() const {
      assert(this);
      return Val == nullptr;
    }

    /* If we're already known, do nothing; otherwise, construct a new value
       using the given args.  Return a refernce to our (possibly new) value. */
    template <typename... TArgs>
    TVal &MakeKnown(TArgs &&... args) {
      assert(this);

      if (!Val) {
        Val = new (Storage) TVal(std::forward<TArgs>(args)...);
      }

      return *Val;
    }

    /* Stream in. */
    void Read(Io::TBinaryInputStream &strm) {
      assert(this);
      assert(&strm);
      bool is_known;
      strm >> is_known;

      if (is_known) {
        strm >> MakeKnown();
      } else {
        Reset();
      }
    }

    /* Forward our value to the out-parameter and resume the unknown state.
       We must already be known. */
    void Release(TVal &val) {
      assert(this);
      assert(&val);
      assert(Val);
      val = std::forward<TVal>(*Val);
      Val = nullptr;
    }

    /* Reset to the unknown state. */
    TOpt &Reset() {
      assert(this);

      if (Val) {
        Val->~TVal();
        Val = 0;
      }

      return *this;
    }

    /* A pointer to our value.  If we're not known, return null. */
    const TVal *TryGet() const {
      assert(this);
      return Val;
    }

    /* A pointer to our value.  If we're not known, return null. */
    TVal *TryGet() {
      assert(this);
      return Val;
    }

    /* Stream out. */
    void Write(Io::TBinaryOutputStream &strm) const {
      assert(this);
      assert(&strm);
      bool is_known = *this;
      strm << is_known;

      if (is_known) {
        strm << **this;
      }
    }

    /* A constant instance in the unknown state.  Useful to have around. */
    static const TSafeGlobal<TOpt> Unknown;

    private:
    /* The storage space used to hold our known value, if any.  We use in-place
       new operators and explicit destruction to make values come and go from
       this storage space. */
    char Storage[sizeof(TVal)] __attribute__((aligned(__BIGGEST_ALIGNMENT__)));

    /* A pointer to our current value.  If this is null, then our value is
       unknown.  If it is non-null, then it points to Storage. */
    TVal *Val;
  };  // TOpt

  /* See declaration. */
  template <typename TVal>
  const TSafeGlobal<TOpt<TVal>> TOpt<TVal>::Unknown(
      [] {
        return new TOpt<TVal>();
      });

  /* A std stream inserter for Base::TOpt<>.  If the TOpt<> is unknown, then
     this function inserts nothing. */
  template <typename TVal>
  std::ostream &operator<<(std::ostream &strm, const Base::TOpt<TVal> &that) {
    assert(&strm);
    assert(&that);

    if (that) {
      strm << *that;
    }

    return strm;
  }

  /* A std stream extractor for Base::TOpt<>. */
  template <typename TVal>
  std::istream &operator>>(std::istream &strm, Base::TOpt<TVal> &that) {
    assert(&strm);
    assert(&that);

    if (!strm.eof()) {
      strm >> that.MakeKnown();
    } else {
      that.Reset();
    }

    return strm;
  }

  /* Binary stream inserter for Base::TOpt<>. */
  template <typename TVal>
  Io::TBinaryOutputStream &operator<<(Io::TBinaryOutputStream &strm,
      const Base::TOpt<TVal> &that) {
    assert(&that);
    that.Write(strm);
    return strm;
  }

  /* Binary stream extractor for Base::TOpt<>. */
  template <typename TVal>
  Io::TBinaryInputStream &operator>>(Io::TBinaryInputStream &strm,
      Base::TOpt<TVal> &that) {
    assert(&that);
    that.Read(strm);
    return strm;
  }

}  // Base
