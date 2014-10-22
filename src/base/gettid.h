/* <base/gettid.h>

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

   A function that returns the ID of the calling thread.
 */

#pragma once

#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace Base {

  /* Linux system call wrapper returns the ID of the calling thread.
     Why isn't this in /usr/include? */
  inline pid_t Gettid() noexcept {
    return syscall(SYS_gettid);
  }

}
