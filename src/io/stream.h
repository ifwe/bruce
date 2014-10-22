/* <io/stream.h>

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

   A I/O stream and its formatters.

   The main idea here is that a stream, weather in-bound or out-bound, will
   store a set of formatting options.  The type of options will depend on the
   type of the stream.  A textual stream might store rules for punctuation,
   capitalization, and number represenataion, where a binary stream might store
   rules for byte ordering and field delimiting.

   The TFormat template paramater used in this module is meant to represent the
   set of formatting options which the stream requires.  The TFormat type must
   default-construct and each stream will aggregate a default-constructed
   instance of the type.  The user may then gain access to this object by
   calling GetFormat() on the stream.

   The user may also construct an instance of TFormatter<TFormat>.  This object
   also aggregates an instance of TFormat, which will be copy-constructed from
   the one currently in use in the stream.  The TFormatter<> object offers
   access to its TFormat as a smart-pointer would, by overloads of the
   deferencing operators.  The stream will use the TFormat instance in the
   TFormatter<> until the TFormatter<> goes out of scope, at which time the
   stream will resume use of the previous TFormat.

   If the user creates another TFormatter<>, it will be stacked on top of the
   last one.  This allows the user to create a 'local' set of formatting rules,
   then automatically resume the previous ones when he's done reading or
   writing.

   There is also a SetFormat() factory which allows the user to inject a change
   to his formatting options in the middle of an insertion expression.  The
   change will be stored in the TFormat instance currently in force, whether
   that is the instance stored on the stream or the instance stored in the
   top-most TFormatter<>.

   For example, here's a function which writes does some writing.  It creates a
   local formatter so that it can change some formatting options without
   altering the stream itself.  (This is an example of good citizenship.)  It
   also uses SetFormat() to make some 'on the fly' changes to its formatting
   options.  When the function exits, its formatter goes out of scope and takes
   all the local formatting changes with it, leaving the stream in the same
   state (formatting-wise) as it was when it enetered.

      void MyWriter(TSomeStream *strm) {
        TFormatter<TSomeFormat> formatter(strm);
        formatter.DecimalPlaces = 2;
        strm << 98.6 << SetFormat(&TSomeFormat::DecimalPlaces, 0) << 98.6;
      }
 */

#pragma once

#include <algorithm>
#include <cassert>

#include <base/assert_true.h>
#include <base/no_copy_semantics.h>

namespace Io {

  /* This forward declaration is needed because TStream<TFormat> makes
     reference to it. */
  template <typename TFormat>
  class TFormatter;

  /* A I/O stream which maintains a stack of formatters. */
  template <typename TFormat>
  class TStream {
    NO_COPY_SEMANTICS(TStream);

    public:
    /* Our current formatting options. */
    const TFormat &GetFormat() const;

    /* Our current formatting options. */
    TFormat &GetFormat();

    protected:
    /* Start with the default format. */
    TStream()
        : NewestFormatter(0) {
    }

    /* It is an error to destroy a stream while it still has formatters. */
    virtual ~TStream() {
      assert(!NewestFormatter);
    }

    private:
    /* The formatter currently in effect, if any. */
    TFormatter<TFormat> *NewestFormatter;

    /* The formatting args we use when we have no formatters. */
    TFormat Format;

    /* Formatters help maintain our stack. */
    friend class TFormatter<TFormat>;
  };  // TStream<TFormat>

  /* A formatter for an I/O stream. */
  template <typename TFormat>
  class TFormatter {
    NO_COPY_SEMANTICS(TFormatter);

    public:
    /* Begin using new formatting options. */
    TFormatter(TStream<TFormat> *strm)
        : Format(Base::AssertTrue(strm)->GetFormat()) {
      Strm = strm;
      OlderFormatter = strm->NewestFormatter;
      strm->NewestFormatter = this;
    }

    /* Resume use of the previous formatting options.  It is not legal to
       destroy a formatter other than the most recent one. */
    ~TFormatter() {
      assert(this);
      /* Formatters must be destroyed in the opposite order in which they were
         created.  If this assertion fails, it means we're being destroyed out
         of order. */
      assert(Strm->NewestFormatter == this);
      Strm->NewestFormatter = OlderFormatter;
    }

    /* Our formatting options. */
    const TFormat &operator*() const {
      assert(this);
      return Format;
    }

    /* Our formatting options. */
    TFormat &operator*() {
      assert(this);
      return Format;
    }

    /* Our formatting options. */
    const TFormat *operator->() const {
      assert(this);
      return &Format;
    }

    /* Our formatting options. */
    TFormat *operator->() {
      assert(this);
      return &Format;
    }

    private:
    /* See accessors. */
    TFormat Format;

    /* The stream we are formatting. */
    TStream<TFormat> *Strm;

    /* The next formatter, if any, in our stream's stack of formatters. */
    TFormatter *OlderFormatter;
  };  // TFormatter<TFormat>

  /* See declaration. */
  template <typename TFormat>
  const TFormat &TStream<TFormat>::GetFormat() const {
    assert(this);
    return NewestFormatter ? **NewestFormatter : Format;
  }

  /* See declaration. */
  template <typename TFormat>
  TFormat &TStream<TFormat>::GetFormat() {
    assert(this);
    return NewestFormatter ? **NewestFormatter : Format;
  }

  /* A helper object, used for setting formatting options in the middle of an
     insertion expression. */
  template <typename TFormat, typename TParam, typename TArg>
  class TSetFormat {
    public:
    /* The type of the format field we're setting. */
    typedef TParam (TFormat::*TPtr);

    /* Cache the pointer to the field and the argument to assign to it. */
    TSetFormat(TPtr ptr, TArg &&arg)
        : Ptr(ptr), Arg(std::move(arg)) {
    }

    /* Assign the argument to the field. */
    void Write(TStream<TFormat> &strm) const {
      assert(this);
      assert(&strm);
      strm.GetFormat().*Ptr = Arg;
    }

    private:
    /* The format field we're setting. */
    TPtr Ptr;

    /* The argument to store in the format field. */
    TArg Arg;
  };  // TSetFormat<TFormat, TParam>

  /* A type-inferring factory for TSetFormat. */
  template <typename TFormat, typename TParam, typename TArg>
  TSetFormat<TFormat, TParam, TArg> SetFormat(TParam (TFormat::*ptr),
      TArg &&arg) {
    return TSetFormat<TFormat, TParam, TArg>(ptr, std::move(arg));
  }

  /* Inserter for Io::TSetFormat. */
  template <typename TFormat, typename TParam, typename TArg>
  TStream<TFormat> &operator<<(TStream<TFormat> &strm,
      const TSetFormat<TFormat, TParam, TArg> &manip) {
    assert(&manip);
    manip.Write(strm);
    return strm;
  }

}  // Io
