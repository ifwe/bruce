/* <base/thrower.h>

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

   Exception-related utilities.
 */

#pragma once

#include <cassert>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <base/code_location.h>

/* Use this macro to define a new error class, like this:

   DEFINE(TSomethingBad, std::runtime_error, "something bad happened"); */
#define DEFINE_ERROR(error_t, base_t, desc)  \
  class error_t final : public base_t { \
    public:\
    error_t(const char *msg) \
        : base_t(msg) { \
    } \
\
    static const char *GetDesc() { \
      return desc; \
    } \
  };

/* Use this macro to throw an error, like this:

   THROW_ERROR(TSomethingBad) << "more info" << Base::EndOfPart
       << "yet more info"; */
#define THROW_ERROR(error_t)  (::Base::TThrower<error_t>(HERE))

/* Use this macro to throw a non-specific error, like this:

   THROW << "the details"; */
#define THROW  (::Base::TThrower< ::Base::TNonSpecificRuntimeError>(HERE))

namespace Base {

  /* A do-nothing singleton.  Insert this object onto a thrower to mark the end
     of a single piece of information. */
  extern const class TEndOfPart final {
  } EndOfPart;

  /* The text we insert between parts of an error message. */
  extern const char *PartDelimiter;

  /* Construct a temporary instance of this object, passing in the current code
     location.  While the object exists, you may stream additional error
     information onto it.  When the object goes out of scope, it will throw the
     error. */
  template <typename TError>
  class TThrower final {
    public:

    /* Begin the error message with the code location and the description
       provided by TError (if any). */
    TThrower(const TCodeLocation &code_location)
        : AtEndOfPart(false) {
      std::move(*this) << code_location << EndOfPart;
      const char *desc = TError::GetDesc();

      if (desc) {
        std::move(*this) << desc << EndOfPart;
      }
    }

    /* Boom goes the dynamite. */
    ~TThrower() noexcept(false) __attribute__((noreturn)) {
      assert(this);
      throw TError(Strm.str().c_str());
    }

    /* Append the value to the error message we are building.
       If we're currently positioned at the end of a message part, insert a
       delimiter before the value. */
    template <typename TVal>
    void Write(const TVal &val) {
      assert(this);

      if (AtEndOfPart) {
        Strm << PartDelimiter;
        AtEndOfPart = false;
      }

      Strm << val;
    }

    /* Append the end-of-part marker to the message.
       This doesn't actually add anything to the message, it just marks the
       position as being the end of a part. */
    void Write(const TEndOfPart &) {
      assert(this);
      AtEndOfPart = true;
    }

    private:
    /* True when we have just written the end of a part. */
    bool AtEndOfPart;

    /* Collects the error message as we build it. */
    std::ostringstream Strm;
  };  // TThrower<TError>

  /* The error thrown by THROW() macro. */
  class TNonSpecificRuntimeError final : public std::runtime_error {
    public:
    /* Do-little. */
    TNonSpecificRuntimeError(const char *msg)
        : std::runtime_error(msg) {
    }

    /* No descriptive message. */
    static const char *GetDesc() {
      return nullptr;
    }
  };  // TNonSpecificRuntimeError

  /* Inserts a value onto a thrower. */
  template <typename TError, typename TVal>
  Base::TThrower<TError> &&operator<<(Base::TThrower<TError> &&thrower,
      const TVal &val) {
    assert(&thrower);
    thrower.Write(val);
    return std::move(thrower);
  }

}  // Base
