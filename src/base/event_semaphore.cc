/* <base/event_semaphore.cc>

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

   Implements <base/event_semaphore.h>.
 */

#include <base/event_semaphore.h>

#include <cerrno>

#include <fcntl.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <base/error_utils.h>
#include <base/io_utils.h>

using namespace Base;

TEventSemaphore::TEventSemaphore(uint64_t initial_count, bool nonblocking)
    : Fd(IfLt0(eventfd(initial_count, EFD_SEMAPHORE))) {
  if (nonblocking) {
    SetNonBlocking(Fd);
  }
}

void TEventSemaphore::Reset(uint64_t initial_count) {
  assert(this);
  int flags = IfLt0(fcntl(Fd, F_GETFL, 0));
  TFd new_fd = IfLt0(eventfd(initial_count, EFD_SEMAPHORE));

  /* Xfer old flags to new FD, including nonblocking option if previously
     specified. */
  IfLt0(fcntl(new_fd, F_SETFL, flags));

  /* Save setting of "close on exec" flag. */
  flags = IfLt0(fcntl(Fd, F_GETFD, 0));

  /* dup() the new FD into the old one.  This prevents the FD number from
     changing, which clients may find helpful.  'new_fd' gets closed by its
     destructor on return. */
  for (; ; ) {
    int dup_fd = dup2(new_fd, Fd);

    if (dup_fd >= 0) {
      assert(dup_fd == Fd);
      close(new_fd);

      /* Preserve setting of close on exec flag. */
      IfLt0(fcntl(dup_fd, F_SETFD, flags));
      break;
    }

    if (errno != EINTR) {
      IfLt0(dup_fd);  // this will throw
    }
  }
}

bool TEventSemaphore::Pop() {
  assert(this);
  uint64_t dummy;
  ssize_t ret = read(Fd, &dummy, sizeof(dummy));

  if (ret < 0) {
    if (errno == EAGAIN) {
      /* The nonblocking option was passed to the constructor, and the
         semaphore was unavailable when we tried to do the pop. */
      return false;
    }

    IfLt0(ret);  // this will throw
  }

  return true;
}

void TEventSemaphore::Push(uint64_t count) {
  assert(this);
  IfLt0(eventfd_write(Fd, count));
}
