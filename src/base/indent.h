/* <base/indent.h>

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

   Utility class for indentation.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <ostream>
#include <string>

namespace Base {

  class TIndent final {
    public:
    static const char DEFAULT_INDENT_CHAR = ' ';

    static const size_t DEFAULT_INDENT_COUNT = 2;

    enum class StartAt {
      Zero,
      Indented
    };  // StartAt

    TIndent(std::string &indent_str, StartAt start_at = StartAt::Indented,
            size_t count = DEFAULT_INDENT_COUNT,
            char indent_char = DEFAULT_INDENT_CHAR)
        : IndentStr(indent_str),
          InitialIndentSize(indent_str.size()),
          Count(count),
          IndentChar(indent_char) {
      if (start_at == StartAt::Indented) {
        IndentStr.append(Count, IndentChar);
      }
    }

    TIndent(TIndent &indent)
        : TIndent(indent.IndentStr, StartAt::Indented, indent.Count,
                  indent.IndentChar) {
    }

    TIndent(TIndent &indent, size_t count, char indent_char)
        : TIndent(indent.IndentStr, StartAt::Indented, count, indent_char) {
    }

    TIndent& operator=(const TIndent &) = delete;

    ~TIndent() {
      assert(this);
      IndentStr.resize(InitialIndentSize);
    }

    void AddOnce(size_t count = DEFAULT_INDENT_COUNT,
                 char indent_char = DEFAULT_INDENT_CHAR) {
      assert(this);
      IndentStr.append(count, indent_char);
    }

    void AddOnce(const char *extra_indent) {
      assert(this);
      IndentStr += extra_indent;
    }

    void AddOnce(const std::string &extra_indent) {
      assert(this);
      AddOnce(extra_indent.c_str());
    }

    const std::string &Get() const {
      assert(this);
      return IndentStr;
    }

    private:
    std::string &IndentStr;
    size_t InitialIndentSize;
    const size_t Count;
    const char IndentChar;
  };  // TIndent

  inline std::ostream &operator<<(std::ostream &strm, const TIndent &that) {
    strm << that.Get();
    return strm;
  }

}  // Base
