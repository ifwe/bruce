/* <bruce/mock_kafka_server/mock_kafka_worker.cc>

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

   Implements <bruce/mock_kafka_server/mock_kafka_worker.h>.
 */

#include <bruce/mock_kafka_server/mock_kafka_worker.h>

#include <array>
#include <cerrno>

#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MockKafkaServer;

TMockKafkaWorker::TIoResult
TMockKafkaWorker::TryReadExactlyOrShutdown(int fd, void *buf,
    size_t size) {
  assert(this);
  std::array<struct pollfd, 2> events;
  struct pollfd &fd_event = events[0];
  struct pollfd &shutdown_request_event = events[1];
  fd_event.fd = fd;
  fd_event.events = POLLIN;
  shutdown_request_event.fd = GetShutdownRequestFd();
  shutdown_request_event.events = POLLIN;

  uint8_t *pos = reinterpret_cast<uint8_t *>(buf);
  size_t bytes_left = size;

  while (bytes_left) {
    fd_event.revents = 0;
    shutdown_request_event.revents = 0;
    int ret = poll(&events[0], events.size(), -1);

    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }

      IfLt0(ret);  // this will throw
    }

    if (shutdown_request_event.revents) {
      return TIoResult::GotShutdownRequest;
    }

    assert(fd_event.revents);
    ssize_t nbytes = read(fd, pos, bytes_left);

    if (nbytes < 0) {
      switch (errno) {
        case EINTR: {
          continue;
        }
        case ECONNRESET:
        case EIO: {
          return (bytes_left == size) ?
              TIoResult::EmptyReadUnexpectedEnd : TIoResult::UnexpectedEnd;
        }
        default: {
          IfLt0(nbytes);  // this will throw
        }
      }
    }

    if (nbytes == 0) {
      return (bytes_left == size) ?
             TIoResult::Disconnected : TIoResult::UnexpectedEnd;
    }

    pos += nbytes;
    bytes_left -= nbytes;
  }

  return TIoResult::Success;
}

TMockKafkaWorker::TIoResult
TMockKafkaWorker::TryWriteExactlyOrShutdown(int fd, const void *buf,
    size_t size) {
  assert(this);
  struct stat stat_buf;
  IfLt0(fstat(fd, &stat_buf));
  bool is_socket = S_ISSOCK(stat_buf.st_mode);

  std::array<struct pollfd, 2> events;
  struct pollfd &fd_event = events[0];
  struct pollfd &shutdown_request_event = events[1];
  fd_event.fd = fd;
  fd_event.events = POLLOUT;
  shutdown_request_event.fd = GetShutdownRequestFd();
  shutdown_request_event.events = POLLIN;

  const uint8_t *pos = reinterpret_cast<const uint8_t *>(buf);
  size_t bytes_left = size;

  while (bytes_left) {
    fd_event.revents = 0;
    shutdown_request_event.revents = 0;
    int ret = poll(&events[0], events.size(), -1);

    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }

      IfLt0(ret);  // this will throw
    }

    if (shutdown_request_event.revents) {
      return TIoResult::GotShutdownRequest;
    }

    assert(fd_event.revents);
    ssize_t nbytes = is_socket ?
        send(fd, pos, bytes_left, MSG_NOSIGNAL) : write(fd, pos, bytes_left);

    if (nbytes < 0) {
      switch (errno) {
        case EINTR: {
          continue;
        }
        case ECONNRESET:
        case EPIPE: {
          return TIoResult::UnexpectedEnd;
        }
        default: {
          IfLt0(nbytes);  // this will throw
        }
      }
    }

    if (nbytes == 0) {
      return (bytes_left == size) ?
             TIoResult::Disconnected : TIoResult::UnexpectedEnd;
    }

    pos += nbytes;
    bytes_left -= nbytes;
  }

  return TIoResult::Success;
}
