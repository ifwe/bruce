/* <bruce/util/worker_thread.cc>

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

   Implements <bruce/util/worker_thread.h>.
 */

#include <bruce/util/worker_thread.h>

#include <functional>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Util;

TWorkerThread::TWorkerThread()
    : ThreadThrewUnknownException(false) {
}

TWorkerThread::~TWorkerThread() noexcept {
  assert(this);

  /* This should have already been called by a subclass destructor.  Calling it
     here a second time is harmless and acts as a safeguard, just in case some
     subclass omits calling it. */
  ShutdownOnDestroy();
}

void TWorkerThread::Start() {
  assert(this);

  if (OptThread.IsKnown()) {
    THROW_ERROR(TThreadAlreadyStarted);
  }

  assert(!ShutdownRequestedSem.GetFd().IsReadable());
  assert(!ShutdownFinishedSem.GetFd().IsReadable());
  assert(!ThreadThrewUnknownException);
  assert(OptThrownByThread.IsUnknown());

  /* Start the thread running. */
  OptThread.MakeKnown(std::bind(&TWorkerThread::RunAndTerminate, this));
}

bool TWorkerThread::IsStarted() const {
  assert(this);
  return OptThread.IsKnown();
}

void TWorkerThread::RequestShutdown() {
  assert(this);

  if (OptThread.IsUnknown()) {
    THROW_ERROR(TThreadAlreadyShutDown);
  }

  ShutdownRequestedSem.Push();
}

const TFd &TWorkerThread::GetShutdownWaitFd() const {
  assert(this);
  return ShutdownFinishedSem.GetFd();
}

void TWorkerThread::Join() {
  assert(this);

  if (OptThread.IsUnknown()) {
    THROW_ERROR(TCannotJoinNonexistentThread);
  }

  ShutdownFinishedSem.Pop();
  OptThread->join();
  OptThread.Reset();
  assert(!ShutdownFinishedSem.GetFd().IsReadable());
  ShutdownRequestedSem.Reset();

  if (OptThrownByThread.IsKnown()) {
    assert(!ThreadThrewUnknownException);
    std::string what_msg(OptThrownByThread->what());
    OptThrownByThread.Reset();
    throw TThreadThrewStdException(what_msg.c_str());
  }

  if (ThreadThrewUnknownException) {
    ThreadThrewUnknownException = false;
    THROW_ERROR(TThreadThrewUnknownException);
  }
}

void TWorkerThread::ShutdownOnDestroy() {
  assert(this);

  if (OptThread.IsKnown()) {
    ShutdownRequestedSem.Push();

    try {
      Join();
    } catch (...) {
      /* Ignore any uncaught exceptions from thread. */
    }
  }

  assert(!ShutdownRequestedSem.GetFd().IsReadable());
  assert(!ShutdownFinishedSem.GetFd().IsReadable());
  assert(OptThread.IsUnknown());
  assert(!ThreadThrewUnknownException);
  assert(OptThrownByThread.IsUnknown());
}

void TWorkerThread::RunAndTerminate() {
  assert(this);

  try {
    Run();
  } catch (const std::exception &x) {
    OptThrownByThread.MakeKnown(x);
  } catch (...) {
    ThreadThrewUnknownException = true;
  }

  /* Let others know that we are about to terminate. */
  ShutdownFinishedSem.Push();

  /* On return, the thread dies. */
}
