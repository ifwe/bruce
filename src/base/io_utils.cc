/* <base/io_utils.cc>

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

   Implements <base/io_utils.h>.
 */

#include <base/io_utils.h>

#include <cassert>
#include <cerrno>

#include <poll.h>
#include <unistd.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

#include <base/error_utils.h>
#include <base/time.h>

using namespace std;

size_t Base::ReadAtMost(int fd, void *buf, size_t max_size) {
  return IfLt0(read(fd, buf, max_size));
}

size_t Base::ReadAtMost(int fd, void *buf, size_t max_size, int timeout_ms) {
  if (timeout_ms >= 0) {
    struct pollfd event;
    event.fd = fd;
    event.events = POLLIN;
    event.revents = 0;
    int ret = IfLt0(poll(&event, 1, timeout_ms));

    if (ret == 0) {
      ThrowSystemError(ETIMEDOUT);
    }
  }

  return ReadAtMost(fd, buf, max_size);
}

size_t Base::WriteAtMost(int fd, const void *buf, size_t max_size) {
  struct stat stat;
  IfLt0(fstat(fd, &stat));
  return IfLt0(S_ISSOCK(stat.st_mode) ?
      send(fd, buf, max_size, MSG_NOSIGNAL) : write(fd, buf, max_size));
}

size_t Base::WriteAtMost(int fd, const void *buf, size_t max_size,
    int timeout_ms) {
  if (timeout_ms >= 0) {
    struct pollfd event;
    event.fd = fd;
    event.events = POLLOUT;
    event.revents = 0;
    int ret = IfLt0(poll(&event, 1, timeout_ms));

    if (ret == 0) {
      ThrowSystemError(ETIMEDOUT);
    }
  }

  return WriteAtMost(fd, buf, max_size);
}

bool Base::TryReadExactly(int fd, void *buf, size_t size) {
  char *csr = static_cast<char *>(buf);
  char *end = csr + size;

  while (csr < end) {
    size_t actual_size = ReadAtMost(fd, csr, end - csr);

    if (!actual_size) {
      if (csr > buf) {
        throw TUnexpectedEnd();
      }

      return false;
    }

    csr += actual_size;
  }

  return true;
}

bool Base::TryReadExactly(int fd, void *buf, size_t size, int timeout_ms) {
  if (timeout_ms < 0) {
    return TryReadExactly(fd, buf, size);
  }

  if (size == 0) {
    return true;
  }

  char *csr = static_cast<char *>(buf);
  char *end = csr + size;
  const clockid_t CLOCK_TYPE = CLOCK_MONOTONIC_RAW;
  TTime deadline;
  deadline.Now(CLOCK_TYPE);
  deadline += timeout_ms;
  int time_left = timeout_ms;

  for (; ; ) {
    size_t actual_size = ReadAtMost(fd, csr, end - csr, time_left);

    if (!actual_size) {
      if (csr > buf) {
        throw TUnexpectedEnd();
      }

      return false;
    }

    csr += actual_size;

    if (csr >= end) {
      assert(csr == end);
      break;
    }

    time_left = deadline.Remaining(CLOCK_TYPE);
  }

  return true;
}

bool Base::TryWriteExactly(int fd, const void *buf,
    size_t size) {
  const char *csr = static_cast<const char *>(buf);
  const char *end = csr + size;

  while (csr < end) {
    size_t actual_size = WriteAtMost(fd, csr, end - csr);

    if (!actual_size) {
      if (csr > buf) {
        throw TUnexpectedEnd();
      }

      return false;
    }

    csr += actual_size;
  }

  return true;
}

bool Base::TryWriteExactly(int fd, const void *buf, size_t size,
    int timeout_ms) {
  if (timeout_ms < 0) {
    return TryWriteExactly(fd, buf, size);
  }

  if (size == 0) {
    return true;
  }

  const char *csr = static_cast<const char *>(buf);
  const char *end = csr + size;
  const clockid_t CLOCK_TYPE = CLOCK_MONOTONIC_RAW;
  TTime deadline;
  deadline.Now(CLOCK_TYPE);
  deadline += timeout_ms;
  int time_left = timeout_ms;

  for (; ; ) {
    size_t actual_size = WriteAtMost(fd, csr, end - csr, time_left);

    if (!actual_size) {
      if (csr > buf) {
        throw TUnexpectedEnd();
      }

      return false;
    }

    csr += actual_size;

    if (csr >= end) {
      assert(csr == end);
      break;
    }

    time_left = deadline.Remaining(CLOCK_TYPE);
  }

  return true;
}

void Base::SetCloseOnExec(int fd) {
  int flags;
  IfLt0(flags = fcntl(fd, F_GETFD, 0));
  IfLt0(fcntl(fd, F_SETFD, flags | FD_CLOEXEC));
}

void Base::SetNonBlocking(int fd) {
  int flags;
  IfLt0(flags = fcntl(fd, F_GETFL, 0));
  IfLt0(fcntl(fd, F_SETFL, flags | O_NONBLOCK));
}
