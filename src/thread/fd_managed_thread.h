/* <thread/fd_managed_thread.h>

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

   Worker thread implementation that uses file descriptors to manage the
   lifetime of the thread.  The thread's Run() method must monitor a file
   descriptor which becomes readable on receipt of a shutdown request.
   Likewise, another file descriptor becomes readable when the thread is just
   about to shut down.  A manager thread can monitor this file descriptor to
   detect when the worker is exiting.

   An advantage of using file descriptors to manage thread lifetime is that
   they can be monitored simultaneously along with other file descriptors via
   select(), poll(), or epoll().  A disadvantage is that each thread requires
   two file descriptors, so creating a really large number may cause the
   operating system's file descriptor limit to be reached.
 */

#pragma once

#include <cassert>
#include <exception>
#include <stdexcept>
#include <string>
#include <thread>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <base/thrower.h>

namespace Thread {

  class TFdManagedThread {
    NO_COPY_SEMANTICS(TFdManagedThread);

    public:
    DEFINE_ERROR(TThreadThrewUnknownException, std::runtime_error,
        "Worker thread threw unknown exception");

    class TThreadThrewStdException final : public std::runtime_error {
      public:
      TThreadThrewStdException(const char *what_msg)
          : std::runtime_error(MakeWhatMsg(what_msg)) {
      }

      TThreadThrewStdException(const TThreadThrewStdException &) = default;

      virtual ~TThreadThrewStdException() noexcept { }

      TThreadThrewStdException &
      operator=(const TThreadThrewStdException &) = default;

      private:
      static std::string MakeWhatMsg(const char *msg);
    };  // TThreadThrewStdException

    /* The destructor will terminate the thread if necessary.  However, in most
       cases it is probably better to shut down the thread manually before
       destructor invocation. */
    virtual ~TFdManagedThread() noexcept;

    /* Launch the worker thread and return immediately while the thread runs.
       Once the thread has finished running and Join() has been called, this
       method may be called again to start a new thread. */
    void Start();

    /* Return true iff. Start() has been called and Join() has not yet been
       called. */
    bool IsStarted() const;

    /* Notify the thread to shut itself down.

       TODO: Make this nonvirtual.  Curently unit test code overrides it. */
    virtual void RequestShutdown();

    /* Return a file descriptor that becomes readable once the thread is about
       to terminate.  If desired, the caller can use a mechanism such as
       select(), poll(), or epoll() to wait for the descritpor to become
       readable.  Once the descriptor becomes readable, the Join() method must
       still be called. */
    const Base::TFd &GetShutdownWaitFd() const;

    /* After calling RequestShutdown(), call this method to wait for the thread
       to terminate.  To avoid blocking for an extended period, one may test
       the file descriptor returned by GetShutdownWaitFd(), and defer calling
       this method until the descriptor becomes readable. If the thread allowed
       an exception of type std::exception to escape from the Run() method,
       this method will throw TThreadThrewStdException _after_ the thread has
       terminated.  If the thread allowed an exception of some other type to
       escape from the Run() method, this method will throw
       TThreadThrewUnknownException _after_ the thread has terminated. */
    void Join();

    const std::thread &GetThread() const {
      assert(this);
      assert(Thread.joinable());
      return Thread;
    }

    std::thread &GetThread() {
      assert(this);
      assert(Thread.joinable());
      return Thread;
    }

    protected:
    TFdManagedThread();

    /* The thread immediately calls this method once it starts executing.
       Subclasses must provide an implementation to define thread-specific
       behavior.  Once the thread receives a shutdown notification, all it must
       do is simply return, once it has finished whatever it needs to do in
       preparation for terminating.  The implementation then handles the
       details of making the file descriptor returned by GetShutdownWaitFd()
       readable and terminating the thread.  This method should not let any
       exceptions escape from it.  However, if any exceptions do escape, a
       corresponding exception will be thrown by Join(). */
    virtual void Run() = 0;

    /* This returns a file descriptor that the thread must monitor to detect a
       shutdown request, which is indicated when the descriptor becomes
       readable. */
    const Base::TFd &GetShutdownRequestFd() const {
      assert(this);
      return ShutdownRequestedSem.GetFd();
    }

    /* When the thread detects that the FD returned by GetShutdownRequestFd()
       has become readable, calling this method will clear the request.  Then,
       if performing a graceful type of shutdown, the thread should continue to
       monitor the FD in case it becomes readable due to some emergency
       scenario causing unexpected destructor invocation. */
    void ClearShutdownRequest() {
      assert(this);
      ShutdownRequestedSem.Pop();
    }

    /* Subclasses should call this in their destructors to make sure the thread
       shuts down even if something unexpected happens. */
    void ShutdownOnDestroy();

    private:
    void RunAndTerminate();

    Base::TEventSemaphore ShutdownRequestedSem;

    Base::TEventSemaphore ShutdownFinishedSem;

    std::thread Thread;

    bool ThreadThrewUnknownException;

    Base::TOpt<std::exception> OptThrownByThread;
  };  // TFdManagedThread

}  // Thread
