/* <base/code_location.h>

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

   Defines a class to represent a location (file and line) within the
   body of source code.  This is useful for reporting errors and for
   logging.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <ostream>

/* Use this macro to represent the current location in the code.
   For example, if a function takes a code location, like this:

     void SomeFunc(const TCodeLocation &code_location, ...);

   You can call it like this:

     SomeFunc(HERE, ...); */
#define HERE ::Base::TCodeLocation(__FILE__, __LINE__)

/* Works like a HERE but allows you to use C string concatenation to build an
   error message. */
#define S2(x) #x
#define S(x) S2(x)
#define HERE_STRING "[" S(__FILE__) ":" S(__LINE__) "]"

namespace Base {

  /* Represents a location (file and line) within the body of source code.
     This is a value type. */
  class TCodeLocation {
    public:
    /* The default is a blank file at line 1. */
    TCodeLocation()
        : File(""), LineNumber(1) {
    }

    /* Represents the given file and line.  'file' must not be null and
       'line_number' must be > 0. */
    TCodeLocation(const char *file, unsigned line_number)
        : File(file), LineNumber(line_number) {
      assert(file);
      assert(line_number);
    }

    /* Returns the file.  Never returns null. */
    const char *GetFile() const;

    /* Returns the line number.  Always returns > 0. */
    unsigned GetLineNumber() const {
      assert(this);
      return LineNumber;
    }

    /* Stream out a human-readable version of our state. */
    void Write(std::ostream &strm) const;

    /* The root of the source tree from which we are built.
       This is injected by the build system as the macro SRC_ROOT. */
    static const char *SrcRoot;

    /* The length of the SrcLen, above, in bytes. */
    static size_t SrcRootLen;

    private:

    /* Never null. */
    const char *File;

    /* Always > 0. */
    unsigned LineNumber;

  };  // TCodeLocation

  /* Standard stream inserter for Base::TCodeLocation. */
  inline std::ostream &operator<<(std::ostream &strm,
      const Base::TCodeLocation &that) {
    assert(&that);
    that.Write(strm);
    return strm;
  }

}  // Base
