/* <server/daemonize.cc>

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

   Implements <server/daemonize.h>.
 */

#include <server/daemonize.h>

#include <initializer_list>

#include <signal.h>
#include <execinfo.h>
#include <fcntl.h>
#include <unistd.h>
#include <syslog.h>
#include <sys/stat.h>

#include <base/os_error.h>
#include <base/error_utils.h>

using namespace std;
using namespace Base;

/* DefendAgainstSignals installs this signal handler
   for SIGINT, SIGTERM, and SIGHUP. */
static void HandleSignal(int sig) {
  switch(sig) {
    case SIGINT: {
      syslog(LOG_NOTICE, "[SIGINT]");
      break;
    }
    case SIGTERM: {
      syslog(LOG_NOTICE, "[SIGTERM]");
      break;
    }
    case SIGHUP: {
      syslog(LOG_NOTICE, "[SIGHUP]");
      break;
    }
    default: {
      syslog(LOG_ERR, "[unexpected signal][%d]", sig);
    }
  }
}

/* Install the given hander over a list of signals. */
static void InstallSignalHandlers(initializer_list<int> signals,
    void (*handler)(int)) {
  for (auto iter = signals.begin(); iter != signals.end(); ++iter) {
    signal(*iter, handler);
  }
}

void Server::BacktraceToLog() {
  static const int max_frame_count = 100;
  void *frames[max_frame_count];
  int frame_count = backtrace(frames, max_frame_count);
  char **symbols = backtrace_symbols(frames, frame_count);

  if (symbols) {
    for (int frame_idx = 0; frame_idx < frame_count; ++frame_idx) {
      syslog(LOG_ERR, "[backtrace][frame %d of %d][%s]", frame_idx + 1,
             frame_count, symbols[frame_idx]);
    }

    free(symbols);
  } else {
      syslog(LOG_ERR, "[backtrace][failed to get %d frames]", frame_count);
  }
}

pid_t Server::Daemonize() {
  pid_t result;

  /* Check our parent's PID to see if we're already a daemon. */
  if (getppid() != 1) {
    /* We're not already a daemon. */
    TOsError::IfLt0(HERE, result = fork());
    /* We've forked.  If we're the daemon child... */

    if (!result) {
      /* Obtain a new process group. */
      setsid();
      /* Close stdin, stdout, and stderr. */
      close(0);
      close(1);
      close(2);
      /* Reroute stdin and stdout to dev/null. */
      int dev_null = open("/dev/null", O_RDWR);
      IfLt0(dup(dev_null));
      IfLt0(dup(dev_null));
      /* Set newly created file permissions. */
      umask(0);
      /* Move to the root dir. */
      IfLt0(chdir("/"));
      /* Keep signals away. */
      DefendAgainstSignals();
    }
  } else {
    /* We're already a daemon. */
    result = 0;
  }

  return result;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
void Server::DefendAgainstSignals() {
  InstallSignalHandlers({ SIGCHLD, SIGTSTP, SIGTTOU, SIGTTIN }, SIG_IGN);
  InstallSignalHandlers({ SIGINT, SIGTERM, SIGHUP }, HandleSignal);
}
#pragma GCC diagnostic pop
