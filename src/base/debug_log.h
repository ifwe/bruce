/* <base/debug_log.h>

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

   Debug logging macro.
 */

#pragma once

#include <syslog.h>

#ifndef NDEBUG
  /* If you're wondering what the token-paste operator is doing in this macro,
     read this: http://gcc.gnu.org/onlinedocs/cpp/Variadic-Macros.html */
  #define DEBUG_LOG(msg, ...) syslog(LOG_DEBUG, msg, ##__VA_ARGS__);
#else
  #define DEBUG_LOG(msg, ...)
#endif

