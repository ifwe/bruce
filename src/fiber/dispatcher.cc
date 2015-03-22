/* <fiber/dispatcher.cc>

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

   Implements <fiber/dispatcher.h>.
 */

#include <fiber/dispatcher.h>

#include <sys/resource.h>

#include <base/error_utils.h>
#include <signal/handler_installer.h>
#include <signal/masker.h>
#include <signal/set.h>

using namespace std;
using namespace chrono;
using namespace Base;
using namespace Fiber;
using namespace Signal;

TDispatcher::THandler::~THandler() noexcept {
  assert(this);

  /* If this fails, you probably forgot to call Unregister() in your
     destructor. */
  assert(!Dispatcher);
}

void TDispatcher::THandler::OnDeadline() {
}

void TDispatcher::THandler::OnEvent(int, short) {
}

void TDispatcher::THandler::OnShutdown() {
}

TDispatcher::THandler::THandler() noexcept {
  Init();
}

void TDispatcher::THandler::ChangeEvent(int fd, short flags) {
  assert(this);
  assert(fd >= 0);
  assert(flags);
  assert(Dispatcher);

  /* Cache the fd and/or flags for which we will wait. */
  Fd = fd;
  Flags = flags;
}

void TDispatcher::THandler::ClearDeadline() {
  assert(this);
  Deadline.Reset();
}

void TDispatcher::THandler::Register(TDispatcher *dispatcher, int fd,
    short flags) {
  assert(this);
  assert(dispatcher);
  /* If we're only changing fd and/or flags, we can skip the linked-list stuff.
   */
  if (Dispatcher != dispatcher) {
    /* If the dispatcher we're trying to register with is already full, we
       throw. */
    if (dispatcher->HandlerCount == dispatcher->MaxHandlerCount) {
      ThrowSystemError(EFAULT);
    }

    /* Unregister from our current dispatcher, if any. */
    Unregister();

    /* Link to the end of the new dispatcher's list of handlers. */
    Dispatcher = dispatcher;
    PrevHandler = dispatcher->LastHandler;
    GetPrevConj() = this;
    GetNextConj() = this;
    ++(dispatcher->HandlerCount);
  }

  /* Update the fd and/or flags for which we will wait. */
  ChangeEvent(fd, flags);
}

void TDispatcher::THandler::SetDeadline(const TTimeout &timeout) {
  assert(this);
  Deadline.Reset();
  Deadline.MakeKnown(Now() + timeout);
}

void TDispatcher::THandler::Unregister() noexcept {
  assert(this);
  /* If we're not registered with a dispatcher, then there's nothing to do;
     otherwise, perform the list-unlinking dance and reinitialize our pointers
     and related member variables to their default-constructed state. */
  if (Dispatcher) {
    GetPrevConj() = NextHandler;
    GetNextConj() = PrevHandler;
    --(Dispatcher->HandlerCount);
    Init();
  }
}

void TDispatcher::THandler::Init() noexcept {
  Dispatcher = nullptr;
  NextHandler = nullptr;
  PrevHandler = nullptr;
  Fd = -1;
  Flags = 0;
}

TDispatcher::THandler *&TDispatcher::THandler::GetNextConj() const noexcept {
  assert(this);
  assert(Dispatcher);
  return NextHandler ? NextHandler->PrevHandler : Dispatcher->LastHandler;
}

TDispatcher::THandler *&TDispatcher::THandler::GetPrevConj() const noexcept {
  assert(this);
  assert(Dispatcher);
  return PrevHandler ? PrevHandler->NextHandler : Dispatcher->FirstHandler;
}

TDispatcher::TDispatcher(size_t max_handler_count)
    : FirstHandler(nullptr), LastHandler(nullptr), HandlerCount(0),
      ShuttingDown(false), Pollers(nullptr), HandlerPtrs(nullptr) {
  try {
    MaxHandlerCount = max_handler_count ?
        max_handler_count : GetMaxEventCount();
    Pollers = new pollfd[MaxHandlerCount];
    HandlerPtrs = new THandler*[MaxHandlerCount];
  } catch (...) {
    this->~TDispatcher();
    throw;
  }
}

TDispatcher::~TDispatcher() {
  assert(this);
  assert(!FirstHandler);
  delete [] Pollers;
  delete [] HandlerPtrs;
}

bool TDispatcher::ForEachHandler(const TCb &cb) {
  assert(this);
  assert(cb);
  /* We cache each next-pointer as we go, just in case the handler is deleted.
   */
  THandler *handler = FirstHandler;

  while (handler) {
    THandler *next_handler = handler->GetNextHandler();

    if (!cb(handler)) {
      return false;
    }

    handler = next_handler;
  }

  return true;
}

void TDispatcher::Run(const TTimeout &grace_period,
    const std::vector<int> &allow_signals, int shutdown_signal_number) {
  assert(this);
  assert(&grace_period);

  /* Install a do-nothing handler for the shutdown signal, so the shutdown
     won't abort the whole process.  We'll mask out all signals while we're
     running, then unmask the shutdown signal only while we're blocked in
     Dispatch().  That way arbitrary I/O won't be affected. */
  THandlerInstaller handle_signal(shutdown_signal_number, ShutdownSigHandler);
  TMasker masker(*TSet(TSet::Full));
  GotShutdownSignal = false;
  TSet mask_set(TSet::Exclude, {});
  TSet shutdown_mask_set(TSet::Exclude, {});

  for (int sig : allow_signals) {
    mask_set -= sig;
    shutdown_mask_set -= sig;
  }

  /* A value <= 0 for shutdown_signal_number indicates that we are not using a
     shutdown signal.  In this case, some other mechanism (i.e. one of the
     monitored file descriptors) must be used to shut down the dispatcher. */
  if (shutdown_signal_number > 0) {
    mask_set -= shutdown_signal_number;
  }

  /* Dispatch normally until we run out of events or are interrupted by our
     chosen signal. */
  TOptTimeout max_timeout;

  while (!ShuttingDown && HandlerCount &&
         Dispatch(max_timeout, mask_set.Get()));

  /* Let any remaining handlers know that we're starting a shutdown. */
  ForEachHandler(
      [](THandler *handler) {
        try {
          handler->OnShutdown();
        } catch (...) {}
        return true;
      }
  );

  /* Compute shutdown deadline (which will be in the very near future, we
     hope), then resume dispatching until all handlers unregister or the
     deadline is reached.  Check for the deadline every at least every 100
     milliseconds.  Note that we are no longer interruptable. */
  auto deadline = Now() + grace_period;
  max_timeout = milliseconds(100);

  while (HandlerCount && Now() < deadline) {  // When will now be now?  Soon...
    Dispatch(max_timeout, shutdown_mask_set.Get());
  }
}

void TDispatcher::Shutdown(thread &t, int signal_number) {
  assert(this);
  assert(&t);
  ShuttingDown = true;
  pthread_kill(t.native_handle(), signal_number);
  t.join();
  ShuttingDown = false;
}

size_t TDispatcher::GetMaxEventCount() {
  rlimit limits;
  IfLt0(getrlimit(RLIMIT_NOFILE, &limits));
  return limits.rlim_cur - 1;
}

volatile bool TDispatcher::GotShutdownSignal = false;

void TDispatcher::ShutdownSigHandler(int /*signum*/) {
  GotShutdownSignal = true;
}

bool TDispatcher::Dispatch(const TOptTimeout &max_timeout,
    const sigset_t *mask_set) {
  assert(this);
  assert(&max_timeout);

  /* Walk the list of regsitered handlers and initialze the poller and handler
     pointer arrays.  Also look for the nearest deadline, if any, among the
     handlers. */
  auto *poller = Pollers;
  auto *handler_ptr = HandlerPtrs;
  TOptDeadline nearest_deadline;

  for (THandler *handler = FirstHandler;
       handler;
       handler = handler->GetNextHandler(), ++poller, ++handler_ptr) {
    /* Each poller gets initialized with the fd and events to wait for and has
       its returned events cleared. */
    assert(poller < Pollers + MaxHandlerCount);
    poller->fd = handler->GetFd();
    poller->events = handler->GetFlags();
    poller->revents = 0;

    /* Each handler pointer caches the handler associated with the poller of
       the same array index.  This way a handler can alter the linked list and
       we won't get messed up. */
    assert(handler_ptr < HandlerPtrs + MaxHandlerCount);
    *handler_ptr = handler;

    /* If this handler has a deadline, it might be the nearest deadline. */
    const auto &deadline = handler->GetDeadline();

    if (deadline && (!nearest_deadline || *deadline < *nearest_deadline)) {
      nearest_deadline = deadline;
    }
  }
  /* Sanity checking. */
  assert(static_cast<size_t>(poller - Pollers) == HandlerCount);
  assert(static_cast<size_t>(handler_ptr - HandlerPtrs) == HandlerCount);

  /* Our timeout, if any, will be based on the nearest deadline and/or the
     maximum allowed timeout. */
  TOptTimeout timeout;

  if (nearest_deadline) {
    timeout.MakeKnown(duration_cast<TTimeout>(*nearest_deadline - Now()));
  }

  if (max_timeout && timeout && *timeout > *max_timeout) {
    timeout = max_timeout;
  }

  /* If we have a timeout, convert it to the system form, which is in whole
     seconds and nanoseconds. */
  timespec ts;
  timespec *ts_ptr;

  if (timeout) {
    auto ticks = nanoseconds(*timeout).count();
    ts.tv_sec  = ticks / nano::den;
    ts.tv_nsec = ticks % nano::den;
    ts_ptr = &ts;
  } else {
    ts_ptr = nullptr;
  }

  /* Wait for one or more events to occur, or for our timeout, or to be
     interrupted by a signal.  If we're interrupted, return false
     immediately. */
  while (ppoll(Pollers, HandlerCount, ts_ptr, mask_set) < 0) {
    if (errno != EINTR) {
      ThrowSystemError(errno);
    }

    if (GotShutdownSignal) {
      return false;
    }
  }

  /* Spin through the poller and handler pointer arrays in parallel,
     dispatching events and deadlines, as appropriate.  We cache the size of
     the arrays before we start because the handlers could change HandlerCount
     as we go along. */
  auto size = HandlerCount;

  for (size_t i = 0; i < size; ++i) {
    auto &pr = Pollers[i];
    auto handler = HandlerPtrs[i];

    if (pr.revents) {
      /* This handler's event has fired. */
      try {
        handler->OnEvent(pr.fd, pr.revents);
      } catch (...) {
      }
    } else {
      const auto &deadline = handler->GetDeadline();

      if (deadline && *deadline < Now()) {
        /* This handler's deadline has passed. */
        try {
          handler->OnDeadline();
        } catch (...) {
        }
      }
    }
  }

  /* We were not interrupted, so return true. */
  return true;
}
