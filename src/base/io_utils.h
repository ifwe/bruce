/* <base/io_utils.h>

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

   I/O utilities.
 */

#pragma once

#include <stdexcept>

namespace Base {

  /* The 'AtMost' versions of read and write are basically just wrappers around
     the OS functions.  They will transfer as much data as possible, up to the
     given max, and return the number of bytes they tranferred.  They will
     raise std::system_error if anything goes wrong or if they are interrupted.
   */

  /* Read at most 'max_size' bytes into 'buf', throwing appropriately. */
  size_t ReadAtMost(int fd, void *buf, size_t max_size);

  /* Same as above, but with timeout in milliseconds.  Throws std::system_error
     with code of ETIMEDOUT on timeout.  A negative timeout value means
     "infinite timeout". */
  size_t ReadAtMost(int fd, void *buf, size_t max_size, int timeout_ms);

  /* Write at most 'max_size' bytes from 'buf', throwing appropriately.
     If the fd is a socket, we will do this operation with send() instead of
     write(), to suppress SIGPIPE. */
  size_t WriteAtMost(int fd, const void *buf, size_t max_size);

  /* Same as above, but with timeout in milliseconds.  Throws std::system_error
     with code of ETIMEDOUT on timeout.  A negative timeout value means
     "infinite timeout". */
  size_t WriteAtMost(int fd, const void *buf, size_t max_size, int timeout_ms);

  /* The 'TryExactly' versions of read and write will call the OS functions
     repeatedly until the full number of bytes is transferred.  If the first OS
     call results in zero bytes being transferred (indicating, for example, the
     the other end of the pipe is closed), the function will stop trying to
     transfer and return false.  If any subseqeuent transfer returns zero
     bytes, the function will throw a std::runtime_error indicating that the
     transfer ended unexpectedly.  If the full number of bytes are tranferred
     successfully, the function returns true. */

  /* An instance of this class is thrown by the 'TryExactly' functions when a
     transfer was started successfully but could not finish. */
  class TUnexpectedEnd : public std::runtime_error {
    public:
    /* Do-little. */
    TUnexpectedEnd()
        : std::runtime_error("unexpected end") {
    }
  };  // TUnexpectedEnd

  /* Try to read exactly 'size' bytes into 'buf'.
     Retry until we get enough bytes, then return true.
     If we get a zero-length return, return false.
     Throw appropriately. */
  bool TryReadExactly(int fd, void *buf, size_t size);

  /* Same as above, but with timeout in milliseconds.  Throws std::system_error
     with code of ETIMEDOUT if entire read is not completed within the timeout
     period.  A negative timeout value means "infinite timeout". */
  bool TryReadExactly(int fd, void *buf, size_t size, int timeout_ms);

  /* Try to write exactly 'size' bytes from 'buf'.
     Retry until we put enough bytes, then return true.
     If we get a zero-length return, return false.
     Throw appropriately. */
  bool TryWriteExactly(int fd, const void *buf, size_t size);

  /* Same as above, but with timeout in milliseconds.  Throws std::system_error
     with code of ETIMEDOUT if entire write is not completed within the timeout
     period.  A negative timeout value means "infinite timeout". */
  bool TryWriteExactly(int fd, const void *buf, size_t size, int timeout_ms);

  /* The 'Exactly' versions of read and write work like the 'TryExactly'
     versions (see above), except that they do not tolerate a failure to start.
     If the transfer could start, they a throw std::runtime_error indicating
     so. */

  /* An instance of this class is thrown by the 'Exactly' functions when a
     transfer could not start. */
  class TCouldNotStart : public std::runtime_error {
    public:
    /* Do-little. */
    TCouldNotStart()
        : std::runtime_error("could not start") {
    }
  };  // TCouldNotStart

  /* Read exactly 'size' bytes into 'buf', throwing appropriately. */
  inline void ReadExactly(int fd, void *buf, size_t size) {
    if (!TryReadExactly(fd, buf, size)) {
      throw TCouldNotStart();
    }
  }

  /* Same as above, but with timeout in milliseconds.  Throws std::system_error
     with code of ETIMEDOUT if entire read is not completed within the timeout
     period.  A negative timeout value means "infinite timeout". */
  inline void ReadExactly(int fd, void *buf, size_t size, int timeout_ms) {
    if (!TryReadExactly(fd, buf, size, timeout_ms)) {
      throw TCouldNotStart();
    }
  }

  /* Write exactly 'size' bytes into 'buf', throwing appropriately. */
  inline void WriteExactly(int fd, const void *buf, size_t size) {
    if (!TryWriteExactly(fd, buf, size)) {
      throw TCouldNotStart();
    }
  }

  /* Same as above, but with timeout in milliseconds.  Throws std::system_error
     with code of ETIMEDOUT if entire write is not completed within the timeout
     period.  A negative timeout value means "infinite timeout". */
  inline void WriteExactly(int fd, const void *buf, size_t size,
      int timeout_ms) {
    if (!TryWriteExactly(fd, buf, size, timeout_ms)) {
      throw TCouldNotStart();
    }
  }

  /* Sets the given fd to close-on-exec. */
  void SetCloseOnExec(int fd);

  /* Sets the given fd to non-blocking I/O. */
  void SetNonBlocking(int fd);

}  // Base
