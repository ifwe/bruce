/* <bruce/util/misc_util.cc>

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

   Implements <bruce/util/misc_util.h>.
 */

#include <bruce/util/misc_util.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <vector>

#include <sys/types.h>
#include <sys/socket.h>
#include <syslog.h>

#include <base/basename.h>
#include <base/error_utils.h>
#include <base/fd.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Util;

void Bruce::Util::InitSyslog(const char *prog_name, int max_level,
    bool log_echo) {
  /* This is static in case syslog retains the passed in string pointer rather
     than making a copy of the string. */
  static const std::string prog_basename = Basename(prog_name);

  openlog(prog_basename.c_str(), LOG_PID | (log_echo ? LOG_PERROR : 0),
          LOG_USER);
  setlogmask(LOG_UPTO(max_level));
  syslog(LOG_NOTICE, "Log started");
}

static bool RunTest(std::vector<uint8_t> &buf,
    const TFd fd_pair[]) {
  std::fill(buf.begin(), buf.end(), 0xff);

  for (; ; ) {
    ssize_t ret = send(fd_pair[0], &buf[0], buf.size(), 0);

    if (ret < 0) {
      switch (errno) {
        case EINTR:
          continue;
        case EMSGSIZE:
          return false;
        default:
          IfLt0(ret);  // this will throw
      }
    }

    break;
  }

  std::fill(buf.begin(), buf.end(), 0);
  ssize_t ret = 0;

  for (; ; ) {
    ret = recv(fd_pair[1], &buf[0], buf.size(), 0);

    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }

      IfLt0(ret);  // this will throw
    }

    break;
  }

  if (static_cast<size_t>(ret) != buf.size()) {
    return false;
  }

  for (size_t i = 0; i < buf.size(); ++i) {
    if (buf[i] != 0xff) {
      return false;
    }
  }

  return true;
}

TUnixDgSizeTestResult Bruce::Util::TestUnixDgSize(size_t size) {
  if (size > (16 * 1024 * 1024)) {
    /* Reject unreasonably large values. */
    return TUnixDgSizeTestResult::Fail;
  }

  /* We must be able to read datagrams 1 byte larger than the requested size.
     This allows us to detect and reject attempts by clients to send messages
     that are too large, rather than silently passing them along truncated.

     TODO: remove this old stull once legacy message format goes away
   */
  ++size;

  std::vector<uint8_t> buf(size);
  TFd fd_pair[2];

  {
    int tmp_fd_pair[2];
    int err = socketpair(AF_LOCAL, SOCK_DGRAM, 0, tmp_fd_pair);
    IfLt0(err);
    fd_pair[0] = tmp_fd_pair[0];
    fd_pair[1] = tmp_fd_pair[1];
  }

  if (RunTest(buf, fd_pair)) {
    return TUnixDgSizeTestResult::Pass;
  }

  int opt = size;
  int ret = setsockopt(fd_pair[0], SOL_SOCKET, SO_SNDBUF, &opt, sizeof(opt));

  if (ret < 0) {
    if (errno == EINVAL) {
      return TUnixDgSizeTestResult::Fail;
    }

    IfLt0(ret);  // this will throw
  }

  return RunTest(buf, fd_pair) ?
      TUnixDgSizeTestResult::PassWithLargeSendbuf :
      TUnixDgSizeTestResult::Fail;
}

size_t Bruce::Util::FirstNonWhitespaceIndex(const std::string &s,
    size_t start_index) {
  if (start_index >= s.size()) {
    return s.size();
  }

  size_t i = start_index;

  for (; (i < s.size()) && std::isspace(s[i]); ++i);

  return i;
}

void Bruce::Util::TrimWhitespace(std::string &s) {
  /* First remove trailing whitespace, including any newline characters. */

  size_t i = s.size();

  while (i > 0) {
    size_t j = i - 1;

    if (!std::isspace(s[j])) {
      break;
    }

    i = j;
  }

  s.resize(i);

  /* Now remove leading whitespace. */
  s.erase(0, FirstNonWhitespaceIndex(s, 0));
}

bool Bruce::Util::StringsMatchNoCase(const char *s1, const char *s2) {
  assert(s1);
  assert(s2);

  for (; ; ) {
    char c1 = *s1;
    char c2 = *s2;

    if (std::toupper(c1) != std::toupper(c2)) {
      return false;
    }

    if (c1 == '\0') {
      break;
    }

    ++s1;
    ++s2;
  }

  return true;
}
