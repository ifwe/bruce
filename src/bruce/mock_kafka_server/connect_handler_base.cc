/* <bruce/mock_kafka_server/connect_handler_base.cc>

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

   Implements <bruce/mock_kafka_server/connect_handler_base.h>.
 */

#include <bruce/mock_kafka_server/connect_handler_base.h>

#include <cassert>

#include <syslog.h>

#include <bruce/util/worker_thread.h>
#include <signal/masker.h>
#include <signal/set.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MockKafkaServer;
using namespace Bruce::Util;
using namespace Signal;

void TConnectHandlerBase::OnShutdown() {
  assert(this);
  Unregister();
}

void TConnectHandlerBase::RunThread(TMockKafkaWorker *w) {
  assert(this);

  class t_cleanup final {
    NO_COPY_SEMANTICS(t_cleanup);

    public:
    t_cleanup(
        std::unordered_map<int, TSharedState::TPerConnectionState> &state_map,
        int key)
        : StateMap(state_map),
          Key(key),
          Active(true) {
    }

    ~t_cleanup() noexcept {
      if (Active) {
        StateMap.erase(Key);
      }
    }

    void Deactivate() {
      Active = false;
    }

    private:
    std::unordered_map<int, TSharedState::TPerConnectionState> &StateMap;

    int Key;

    bool Active;
  };  // t_cleanup

  std::unique_ptr<TMockKafkaWorker> worker(w);
  int shutdown_wait_fd = worker->GetShutdownWaitFd();
  std::unique_ptr<TThreadTerminateHandler> terminate_handler(
      new TThreadTerminateHandler(std::move(
          std::bind(&TConnectHandlerBase::DeleteThreadState, this,
                    shutdown_wait_fd))));
  auto ret = Ss.PerConnectionMap.insert(
      std::make_pair(shutdown_wait_fd, TSharedState::TPerConnectionState()));
  assert(ret.second);
  t_cleanup cleanup_on_fail(Ss.PerConnectionMap, shutdown_wait_fd);
  TSharedState::TPerConnectionState &state = ret.first->second;
  state.Worker.reset(worker.release());
  state.TerminateHandler.reset(terminate_handler.release());
  state.TerminateHandler->RegisterWithDispatcher(
      *Ss.Dispatcher, shutdown_wait_fd, POLLIN);

  {
    /* Start a thread to handle the client connection.  The main thread does
       all signal handling, so worker threads spend their entire lifetimes with
       all signals blocked.  Block all signals when creating the thread, so it
       inherits the desired signal mask. */
    TMasker masker(*TSet(TSet::Full));
    state.Worker->Start();
  }

  cleanup_on_fail.Deactivate();
}

void TConnectHandlerBase::DeleteThreadState(int shutdown_wait_fd) {
  assert(this);

  auto iter = Ss.PerConnectionMap.find(shutdown_wait_fd);
  assert(iter != Ss.PerConnectionMap.end());

  try {
    iter->second.Worker->Join();
  } catch (const TWorkerThread::TThreadThrewStdException &x) {
    syslog(LOG_ERR, "%s", x.what());
  } catch (const TWorkerThread::TThreadThrewUnknownException &x) {
    syslog(LOG_ERR, "%s", x.what());
  }

  Ss.PerConnectionMap.erase(iter);
}
