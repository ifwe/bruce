/* <fiber/dispatcher.h>

   ----------------------------------------------------------------------------
   Copyright 2013 if(we)

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

   Handles overlapped I/O in a single thread, dispatching I/O events to their
   registered handlers.
 */

#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <functional>
#include <thread>
#include <vector>

#include <poll.h>
#include <signal.h>

#include <base/no_copy_semantics.h>
#include <base/opt.h>

namespace Fiber{

  /* Handles overlapped I/O in a single thread, dispatching I/O events to their
     registered handlers. */
  class TDispatcher final {
    NO_COPY_SEMANTICS(TDispatcher);
    public:

    /* We specify deadlines relative to the monotonic clock. */
    using TDeadline = std::chrono::steady_clock::time_point;

    /* Deadlines are optional, so we use this type a lot. */
    using TOptDeadline = Base::TOpt<TDeadline>;

    /* The type we use when talking about timeouts and grace periods. */
    using TTimeout = std::chrono::milliseconds;

    /* Timeouts are optional, so we use this type a lot, too. */
    using TOptTimeout = Base::TOpt<TTimeout>;

    /* An I/O handler.  Inherit from this class and implement OnEvent() (and
       optionally OnShutdown()).  In your most-derived classs, have the
       constructor call Register() and the destructor call Unregister(). */
    class THandler {
      NO_COPY_SEMANTICS(THandler);

      public:
      /* For the convenience of those who inherit from us. */
      using TDeadline = TDispatcher::TDeadline;
      using TOptDeadline = TDispatcher::TOptDeadline;
      using TTimeout = TDispatcher::TTimeout;
      using TOptTimeout = TDispatcher::TOptTimeout;

      /* Make sure you call Unregister() in the destructor of your most-derived
         class. */
      virtual ~THandler() noexcept;

      /* The dispatcher with which we are registered, or null if we're not
         registered with a dispatcher. */
      TDispatcher *GetDispatcher() const noexcept {
        assert(this);
        return Dispatcher;
      }

      /* The fd for which we wait, or -1 if we're not registered with a
         dispatcher. */
      int GetFd() const noexcept {
        assert(this);
        return Fd;
      }

      /* The I/O flags for which we wait, or 0 if we're not registred with a
         dispatcher. */
      short GetFlags() const noexcept {
        assert(this);
        return Flags;
      }

      /* The handler that follows us in our dispatcher.  Null if we're the last
         handler in our dispatcher or if we're not registered with a
         dispatcher. */
      THandler *GetNextHandler() const noexcept {
        assert(this);
        return NextHandler;
      }

      /* The handler that precedes us in our dispatcher.  Null if we're the
         first handler in our dispatcher or if we're not registered with a
         dispatcher. */
      THandler *GetPrevHandler() const noexcept {
        assert(this);
        return PrevHandler;
      }

      /* The current deadline for this handler, if any. */
      const TOptDeadline &GetDeadline() const noexcept {
        assert(this);
        return Deadline;
      }

      /* Called by the dispatcher when the deadline for this handler has
         passed.  If you override this function, it's ok use it to delete this
         handler or any other handler, to chance this or any handler's event
         and/or deadline, and/or to create new handlers.  Just don't do
         anything that could block the thread or abort the process.  This
         function is never called if we are not registered with a dispatcher or
         if we have not established a deadline.  The default implementation
         does nothing. */
      virtual void OnDeadline();

      /* Called by the dispatcher when the event for which we're registered
         occurs.  If you override this function, it's ok use it to delete this
         handler or any other handler, to chance this or any handler's event
         and/or deadline, and/or to create new handlers.  Just don't do
         anything that could block the thread or abort the process.  This
         function is never called if we are not registered with a dispatcher.
         The default implementation does nothing. */
      virtual void OnEvent(int fd, short flags);

      /* Called by the dispatcher when a shutdown is initiated.  If you
         override this function, it's ok use it to delete this handler but
         DON"T DELETE ANY OTHER HANDLER.  It's to make other changes, including
         adding new handlers, but remember the goal is to allow the dispatcher
         to shutdown before the grace period expires.  In particular, don't do
         anything that could block the thread or abort the process.  This
         function is never called if we are not registered with a dispatcher.
         The default implementation does nothing. */
      virtual void OnShutdown();

      protected:
      /* Start not registered with a dispatcher.  Make sure you call Register()
         in the constructor of your most-derived class. */
      THandler() noexcept;

      /* Remain registered with the same dispatcher, but change the fd and/or
         flags for which you will wait.  You must be registered with a
         dispatcher before you call this function.  See Register() for more
         information. */
      void ChangeEvent(int fd, short flags);

      /* Remove the deadline we have previously esstablished, if any. */
      void ClearDeadline();

      /* Register this handler with the given dispatcher, indicating you will
         wait for the given fd and flags.  If the given dispatcher cannot
         accomodate any more handlers, this function throws EFAULT.  The flags
         are the same as those used in pollfd events.  You must call this
         function in the constructor of your most-derived class. */
      void Register(TDispatcher *dispatcher, int fd, short flags);

      /* Set the deadline for the given amount of time from now. */
      void SetDeadline(const TTimeout &timeout);

      /* Unregister this handler from its dispatcher.  If the handler is not
         currently registered, this function does nothing.  You must call this
         function in the destructor of your most-derived class. */
      void Unregister() noexcept;

      private:
      /* A reference to the pointer which points backward to us in our
         dispatcher's linked list of handlers.  It is an error to call this
         function when the handler is not registered with dispatcher. */
      THandler *&GetNextConj() const noexcept;

      /* A reference to the pointer whicn points forward to us in our
         dispatcher's linked list of handlers.  It is an error to call this
         function when the handler is not registered with dispatcher. */
      THandler *&GetPrevConj() const noexcept;

      /* Set the linked list pointers to null and set the fd and flags fields
         to defaults.  This function ingores and overwrites the previous state
         of the handler. */
      void Init() noexcept;

      /* See accessor. */
      TDispatcher *Dispatcher;

      /* See accessors. */
      THandler *NextHandler, *PrevHandler;

      /* See accessor. */
      int Fd;

      /* See accessor. */
      short Flags;

      /* See accessor. */
      TOptDeadline Deadline;

    };  // TDispatcher::THandler

    /* The type of callback used by ForEacHandler(), below. */
    using TCb = std::function<bool (THandler *)>;

    /* Construct a dispatcher capable of handling up to the given number of
       registered handlers.  If you pass a zero here, the dispatcher will take
       its size from GetMaxEventCount(), below. */
    explicit TDispatcher(size_t max_handler_count = 0);

    /* Frees the resources used by the dispatcher.  You must make sure all
       handlers are unregistered before destroying the dispatcher. */
    ~TDispatcher();

    /* Calls back for each registered handler.  It is explicitly ok for the
       callback to delete the handler passed to it, but it is NOT OK for the
       callback to delete other handlers. */
    bool ForEachHandler(const TCb &cb);

    /* The number of handlers currently registered with this dispatcher. */
    size_t GetHandlerCount() const {
      assert(this);
      return HandlerCount;
    }

    /* Dispatch events to registered handlers until we run out of handlers or
       we are interrupted by the given signal.  If we are interrupted, each
       remaining handler will receive the OnShutdown() event and the dispatcher
       will continue to dispatch events for the given grace period.  After
       that, even if there are handlers remaining, the function returns.  You
       should then use ForEachHandler() to do something about the stragglers
       before attempting to destroy the dispatcher.  Note, while this function
       is running, all signals other than the shutdown signal will be blocked
       completely, and the shutdown signal will be unblocked only while we're
       waiting for events.  Your I/O and shutdown handlers will execute in a
       context with all signals blocked, so you should not receive an EINTR
       errors.  The signals in allow_signals will also be unmasked when waiting
       for events, but will not cause shutdown.  A value <= 0 for
       shutdown_signal_number indicates that we are not using a shutdown
       signal.  In this case, some other mechanism (i.e. one of the monitored
       file descriptors) must be used to shut down the dispatcher.  Not using a
       shutdown signal may be useful in situations where there are multiple
       threads and each uses its own dispatcher.  In that case, if multiple
       threads specified the same shutdown signal, and the signal was delivered
       to the process (not a specific thread), it would not be clear which
       thread gets the signal. */
    void Run(const TTimeout &grace_period,
        const std::vector<int> &allow_signals,
        int shutdown_signal_number = SIGINT);

    /* Issue the given signal to the given thread (which is presumed to be in
       Run(), above) and wait for the thread to exit.  This is useful if you're
       running the dispatcher in a background thread, which is the case in the
       unit test.  However, if you're using the dispatcher to handle the I/O
       for a daemon process, you don't need to use a background thread and you
       don't need to call Shutdown().  Just call Run() directly from your main
       thread and let the operating system interrupt you.  Don't call this
       method if you passed a shutdown signal number <= 0 to the Run() method.
     */
    void Shutdown(std::thread &t, int signal_number);

    /* The maximum number of file descriptors the calling process may have open
       simultaneously. */
    static size_t GetMaxEventCount();

    /* The current time, as reported by the clock we use for deadlines. */
    static TDeadline Now() {
      return TDeadline::clock::now();
    }

    private:
    static void ShutdownSigHandler(int signum);

    /* Wait for one or more I/O events to happen, then call their
       dispatcher(s).  If max_timeout is known, wait no longer than it
       indicates; otherwise, wait based on the nearest deadline of the
       registred handlers.  If there are no deadlines to honor, wait
       indefinitely.  If there is a signal mask, switch to that mask while
       waiting, then switch back when the wait ends.  Return false iff. we were
       interrupted; otherwise, true. */
    bool Dispatch(const TOptTimeout &max_timeout, const sigset_t *mask_set);

    /* Set by signal handler when we get the shutdown signal. */
    static volatile bool GotShutdownSignal;

    /* The first and last of our registered handlers, if any. */
    THandler *FirstHandler, *LastHandler;

    /* The number of handlers in the linked list. */
    size_t HandlerCount;

    /* True only while Shutdown() is waiting for the background thread to exit.
     */
    std::atomic_bool ShuttingDown;

    /* The size, in elements, if the Pollers and HandlerPtrs arrays. */
    size_t MaxHandlerCount;

    /* Initialized by InitArrays(), above. */
    pollfd *Pollers;

    /* Initialized by InitArrays(), above. */
    THandler **HandlerPtrs;
  };  // TDispatcher

}  // Fiber
