/* <bruce/input_thread.cc>

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

   Implements <bruce/input_thread.h>.
 */

#include <bruce/input_thread.h>

#include <algorithm>
#include <array>
#include <exception>

#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include <base/error_utils.h>
#include <base/gettid.h>
#include <bruce/input_dg/input_dg_util.h>
#include <bruce/util/time_util.h>
#include <server/counter.h>
#include <socket/address.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Util;
using namespace Capped;
using namespace Socket;

SERVER_COUNTER(InputThreadForwardMsg);
SERVER_COUNTER(InputThreadGotOkMsg);

TInputThread::TInputThread(const TConfig &config, TPool &pool,
    TMsgStateTracker &msg_state_tracker, TAnomalyTracker &anomaly_tracker,
    TRouterThreadApi &router_thread)
    : Config(config),
      Destroying(false),
      ShutdownStatus(TShutdownStatus::Normal),
      Pool(pool),
      MsgStateTracker(msg_state_tracker),
      AnomalyTracker(anomaly_tracker),
      InputSocket(SOCK_DGRAM, 0),

      /* Add 1 to our buffer size so we can detect messages that are too large
         and log them as discards, rather then silently passing them along
         truncated. */
      InputBuf(config.MaxInputMsgSize + 1),

      RouterThread(router_thread),
      MsgReceivedCount(0) {
}

TInputThread::~TInputThread() noexcept {
  /* This will shut down the thread if something unexpected happens.  Setting
     the 'Destroying' flag tells the thread to shut down immediately when it
     gets the shutdown request. */
  Destroying = true;
  ShutdownOnDestroy();
}

void TInputThread::Run() {
  assert(this);
  int tid = static_cast<int>(Gettid());
  syslog(LOG_NOTICE, "Input thread %d started", tid);

  try {
    DoRun();
  } catch (const std::exception &x) {
    syslog(LOG_ERR, "Fatal error in input thread %d: %s", tid, x.what());
    _exit(EXIT_FAILURE);
  } catch (...) {
    syslog(LOG_ERR, "Fatal unknown error in input thread %d", tid);
    _exit(EXIT_FAILURE);
  }

  syslog(LOG_NOTICE, "Input thread %d finished %s", tid,
      (ShutdownStatus == TShutdownStatus::Normal) ? "normally" : "on error");
}

void TInputThread::DoRun() {
  assert(this);
  ShutdownStatus = TShutdownStatus::Error;
  syslog(LOG_NOTICE, "Starting router thread");

  /* Don't wait for the router thread to finish its initialization, since that
     depends on Kafka being in a healthy state.  If the router thread runs into
     Kafka problems, it will retry until it finishes initialization.  Meanwhile
     we must monitor the UNIX domain socket so web processes can run normally.
     Forwarded messages will be queued for the router thread until it finishes
     its initialization or we run out of buffer space and start logging
     discards. */
  RouterThread.Start();

  syslog(LOG_NOTICE, "Started router thread, opening UNIX domain socket");
  OpenUnixSocket();

  /* Let the thread that started us know that we finished our initialization
     successfully. */
  InitFinishedSem.Push();

  syslog(LOG_NOTICE,
         "Input thread finished initialization, forwarding messages");

  if (ForwardMessages()) {
    ShutdownStatus = TShutdownStatus::Normal;
  }
}

void TInputThread::OpenUnixSocket() {
  assert(this);
  TAddress input_socket_address;
  input_socket_address.SetFamily(AF_LOCAL);
  input_socket_address.SetPath(Config.ReceiveSocketName.c_str());
  Bind(InputSocket, input_socket_address);
}

bool TInputThread::ShutDownRouterThread() {
  assert(this);
  syslog(LOG_NOTICE, "Input thread sending shutdown request to router thread");
  RouterThread.RequestShutdown();
  syslog(LOG_NOTICE, "Input thread waiting for router thread to shut down");
  RouterThread.Join();
  bool ok = (RouterThread.GetShutdownStatus() ==
             TRouterThreadApi::TShutdownStatus::Normal);
  syslog(LOG_NOTICE,
         "Input thread got %s termination status from router thread",
         (ok ? "normal" : "error"));
  return ok;
}

TMsg::TPtr TInputThread::ReadOneMsg() {
  assert(this);
  char * const msg_begin = reinterpret_cast<char *>(&InputBuf[0]);
  ssize_t result = IfLt0(recv(InputSocket, msg_begin, InputBuf.size(), 0));
  TMsg::TPtr msg = InputDg::BuildMsgFromDg(msg_begin, result, Config, Pool,
      AnomalyTracker, MsgStateTracker);

  if (msg) {
    InputThreadGotOkMsg.Increment();
  }

  return std::move(msg);
}

bool TInputThread::ForwardMessages() {
  assert(this);
  bool ok_status = false;
  std::array<struct pollfd, 3> events;
  struct pollfd &shutdown_wait_event = events[0];
  struct pollfd &shutdown_request_event = events[1];
  struct pollfd &input_socket_event = events[2];
  shutdown_wait_event.fd = RouterThread.GetShutdownWaitFd();
  shutdown_wait_event.events = POLLIN;
  shutdown_request_event.fd = GetShutdownRequestFd();
  shutdown_request_event.events = POLLIN;
  input_socket_event.fd = InputSocket.GetFd();
  input_socket_event.events = POLLIN;
  TRouterThreadApi::TMsgChannel &queue = RouterThread.GetMsgChannel();
  TMsg::TPtr msg;

  for (; ; ) {
    for (auto &item : events) {
      item.revents = 0;
    }

    int ret = IfLt0(poll(&events[0], events.size(), -1));
    assert(ret > 0);

    if (shutdown_wait_event.revents) {
      /* Router thread encountered fatal error. */
      syslog(LOG_ERR,
             "Input thread shutting down due to fatal router thread error");
      InputSocket.Reset();
      RouterThread.Join();
      assert(RouterThread.GetShutdownStatus() ==
             TRouterThreadApi::TShutdownStatus::Error);
      break;
    }

    if (shutdown_request_event.revents) {
      /* Note: In the case where 'Destroying' is true, the router thread's
         destructor will shut it down. */
      if (!Destroying) {
        syslog(LOG_NOTICE, "Input thread got shutdown request, closing UNIX "
               "domain socket");
        /* We received a shutdown request from the thread that created us.
           Close the input socket and perform an orderly shutdown of the router
           thread. */
        InputSocket.Reset();
        ok_status = ShutDownRouterThread();
      }

      break;
    }

    assert(input_socket_event.revents);
    assert(!msg);
    msg = ReadOneMsg();
    ++MsgReceivedCount;  // for testing

    if (msg) {
      /* Forward message to router thread. */
      queue.Put(std::move(msg));
      InputThreadForwardMsg.Increment();
    }
  }

  return ok_status;
}
