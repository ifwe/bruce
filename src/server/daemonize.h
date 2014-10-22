/* <server/daemonize.h>

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

   Spawn a daemon.
 */

#pragma once

#include <functional>
#include <sys/types.h>

namespace Server {

  /* Send a trace of the stack to the log. */
  void BacktraceToLog();

  /* The calling process will fork and the child process will become a daemon.

     In particular, the child process will:
        * become session leader,
        * change to the root directory,
        * clear out its file mode creation mask,
        * close its stdin, out, and err handles,
        * redirect its stdin/out to dev/null,
        * ignore child and TTY signals, and
        * catch hang-up, kill, and interrupt signals.

     The function returns the id of the daemon process to the parent process
     and zero to the daemon process.  If the calling process is already a
     daemon, the function does nothing and returns zero.

     It is the responsibility of the caller to detect multiple invocations of
     the daemon on the same machine.  (This often happens when second and
     subsequent invocation of the daemon try to bind to a port that's already
     allocated.)  It is usually necessary to report the id of the daemon
     process to the user by some means. */
  pid_t Daemonize();

  /* Does as above, but it launches func() as a daemond process, catching
     exceptions that it throws and printing them out nicely to syslog, rather
     then forcing the caller to implement the logic. */
  pid_t Daemonize(std::function<void()> func);

  /* The calling process will begin ignoring child and TTY signals and will
     catch hang-up, kill, and interrupt signals.  The process can then dead-end
     into a pause() call, waiting to be signaled that it is time to continue.
   */
  void DefendAgainstSignals();

}  // Server
