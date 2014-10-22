/* <bruce/util/misc_util.h>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 if(we)

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

   Utility functions.
 */

#pragma once

#include <cstddef>
#include <string>

namespace Bruce {

  namespace Util {

    void InitSyslog(const char *prog_name, int max_level, bool log_echo);

    /* Result of call to TestUnixDgSize() below. */
    enum class TUnixDgSizeTestResult {
      /* Test passed with default value for SO_SNDBUF. */
      Pass,

      /* Test passed after setting SO_SNDBUF to size of test datagram. */
      PassWithLargeSendbuf,

      /* Test failed. */
      Fail
    };

    /* Attempt to send and receive a UNIX domain datagram of 'size' bytes.
       Return true on success or false on failure.  Throw on fatal system
       error.  */
    TUnixDgSizeTestResult TestUnixDgSize(size_t size);

    size_t FirstNonWhitespaceIndex(const std::string &s,
                                   size_t start_index);

    void TrimWhitespace(std::string &s);

    /* Return true if strings match by case-insensitive comparison.  Otherwise
       return false. */
    bool StringsMatchNoCase(const char *s1, const char *s2);

    inline bool StringsMatchNoCase(const std::string &s1,
        const std::string &s2) {
      return StringsMatchNoCase(s1.c_str(), s2.c_str());
    }

    inline bool StringsMatchNoCase(const std::string &s1, const char *s2) {
      return StringsMatchNoCase(s1.c_str(), s2);
    }

    inline bool StringsMatchNoCase(const char *s1, const std::string &s2) {
      return StringsMatchNoCase(s1, s2.c_str());
    }

  }  // Util

}  // Bruce
