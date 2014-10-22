/* <bruce/mock_kafka_server/main_thread.cc>

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

   Implements <bruce/mock_kafka_server/main_thread.h>.
 */

#include <bruce/mock_kafka_server/main_thread.h>

#include <cstdlib>
#include <exception>
#include <iostream>

#include <signal.h>

#include <base/error_utils.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MockKafkaServer;

TMainThread::~TMainThread() noexcept {
  /* This will shut down the thread if something unexpected happens. */
  ShutdownOnDestroy();
}

void TMainThread::RequestShutdown() {
  assert(this);

  /* The standalone executable version of the mock Kafka server expects to get
     SIGINT, so here we mimic that behavior. */
  IfLt0(pthread_kill(GetThread().native_handle(), SIGINT));

  TWorkerThread::RequestShutdown();
}

void TMainThread::Run() {
  assert(this);
  ShutdownStatus = TShutdownStatus::Error;

  try {
    if (Server.Init() == EXIT_SUCCESS) {
      InitFinishedSem.Push();

      if (Server.Run() == EXIT_SUCCESS) {
        ShutdownStatus = TShutdownStatus::Normal;
      } else {
        std::cerr << "mock Kafka server shutting down on error" << std::endl;
      }
    } else {
      std::cerr << "mock Kafka server initialization failed" << std::endl;
    }
  } catch (const std::exception &x) {
    std::cerr << "mock Kafka server error: " << x.what() << std::endl;
  } catch (...) {
    std::cerr << "mock Kafka server error: unexpected unknown exception"
        << std::endl;
  }
}
