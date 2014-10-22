/* <bruce/test_util/mock_router_thread.cc>

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

   Implements <bruce/test_util/mock_router_thread.h>.
 */

#include <bruce/test_util/mock_router_thread.h>

#include <cassert>

using namespace Base;
using namespace Bruce;
using namespace Bruce::TestUtil;

const TFd &TMockRouterThread::GetInitWaitFd() const {
  assert(this);
  return InitFinishedSem.GetFd();
}

TRouterThreadApi::TShutdownStatus
TMockRouterThread::GetShutdownStatus() const {
  assert(this);
  return TShutdownStatus::Normal;
}

TRouterThreadApi::TMsgChannel &TMockRouterThread::GetMsgChannel() {
  assert(this);
  return MsgChannel;
}

size_t TMockRouterThread::GetAckCount() const {
  assert(this);
  return 0;
}

TEventSemaphore &TMockRouterThread::GetMetadataUpdateRequestSem() {
  assert(this);
  return MetadataUpdateRequestSem;
}

void TMockRouterThread::Start() {
  assert(this);
  InitFinishedSem.Push();
}

bool TMockRouterThread::IsStarted() const {
  assert(this);
  return true;
}

void TMockRouterThread::RequestShutdown() {
  assert(this);
  ShutdownFinishedSem.Push();
}

const TFd &TMockRouterThread::GetShutdownWaitFd() const {
  assert(this);
  return ShutdownFinishedSem.GetFd();
}

void TMockRouterThread::Join() {
  assert(this);
  ShutdownFinishedSem.Pop();
}

void TMockRouterThread::Run() {
  assert(this);
}

const TFd &TMockRouterThread::GetShutdownRequestFd() {
  assert(this);
  return ShutdownRequestedSem.GetFd();
}

void TMockRouterThread::ClearShutdownRequest() {
  assert(this);
}
