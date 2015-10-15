/* <thread/managed_thread_pool.h>

   ----------------------------------------------------------------------------
   Copyright 2015 Dave Peterson <dave@dspeterson.com>

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

   Managed thread pool, where work to be done by thread is supplied by a
   callable object whose type is given as a template parameter.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <exception>
#include <functional>
#include <utility>

#include <base/no_copy_semantics.h>
#include <thread/managed_thread_pool_base.h>

namespace Thread {

  /* Managed thread pool class, where threads perform work by calling a
     callable object of type TWorkCallable.  The requirements for TWorkCallable
     are as follows:

         1.  It must be constructible as follows:

                 TWorkCallable(nullptr)

         2.  It must be possible to assign nullptr to it.  For instance, if
             TWorkCallable is a class then an assigment operator method such as
             this would satisfy the requirement:

                 TWorkCallable &TWorkCallable::operator=(nullptr_t);

             The assignment should have the effect of releasing any resources
             held within the function object, and should not throw.  Any
             exception escaping from the assignment operator will cause
             invocation of TManagedThreadPoolBase::HandleFatalError().

         3.  It must be callable, taking no parameters.  Any returned value
             will be ignored.

      Note that both std::function<void()> and function pointers of type
      void (*)() satisfy all of these requirements. */
  template <typename TWorkCallable>
  class TManagedThreadPool : public TManagedThreadPoolBase {
    NO_COPY_SEMANTICS(TManagedThreadPool);

    class TWorker;

    public:
    /* Wrapper class for thread obtained from pool. */
    class TReadyWorker final
        : public TManagedThreadPoolBase::TReadyWorkerBase {
      NO_COPY_SEMANTICS(TReadyWorker);

      public:
      /* Construct an empty wrapper (i.e. a wrapper that contains no thread).
       */
      TReadyWorker() = default;

      /* Any thread contained by donor is moved into the wrapper being
         constructed, leaving the donor empty. */
      TReadyWorker(TReadyWorker &&) = default;

      /* Releases the thread (i.e. puts it back in the pool if appropriate) if
         base class Launch() method has not been called. */
      virtual ~TReadyWorker() noexcept {
      }

      /* Move any thread contained by donor into the assignment target, leaving
         the donor empty.  Release any thread prevoiusly contained by
         assignment target. */
      TReadyWorker &operator=(TReadyWorker &&) = default;

      /* Swap internal state with 'that'. */
      void Swap(TReadyWorker &that) noexcept {
        assert(this);
        TManagedThreadPoolBase::TReadyWorkerBase::Swap(that);
      }

      /* Get the function object for the ready worker.  The caller can then
         assign a value to it before calling Launch().  Must only be called
         when wrapper is nonempty (i.e. IsLaunchable() returns true). */
      TWorkCallable &GetWorkFn() noexcept {
        assert(this);
        assert(GetWorker());
        return GetWorker()->GetWorkFn();
      }

      /* Get the pool that the contained worker belongs to.  Must only be
         called when wrapper is nonempty (i.e. IsLaunchable() returns true). */
      TManagedThreadPool &GetPool() const {
        assert(this);
        assert(GetWorker());
        return static_cast<TManagedThreadPool &>(GetWorker()->GetPool());
      }

      private:
      /* Called by TManagedThreadPool to wrap thread obtained from pool. */
      explicit TReadyWorker(TWorker *worker) noexcept
          : TManagedThreadPoolBase::TReadyWorkerBase(worker) {
      }

      /* Return pointer to contained worker, or nullptr if wrapper is empty.
         Pool maintains ownership of worker and controls its lifetime. */
      TWorker *GetWorker() const {
        return static_cast<TWorker *>(GetWorkerBase());
      }

      /* so TManagedThreadPool can call private constructor */
      friend class TManagedThreadPool;
    };  // TReadyWorker

    /* Construct thread pool with given fatal error handler and configuration.
     */
    TManagedThreadPool(const TFatalErrorHandler &fatal_error_handler,
        const TConfig &cfg)
        : TManagedThreadPoolBase(fatal_error_handler, cfg) {
    }

    /* Construct thread pool with given fatal error handler and configuration.
     */
    TManagedThreadPool(TFatalErrorHandler &&fatal_error_handler,
        const TConfig &cfg)
        : TManagedThreadPoolBase(std::move(fatal_error_handler), cfg) {
    }

    /* Construct thread pool with given fatal error handler and default
       configuration. */
    explicit TManagedThreadPool(const TFatalErrorHandler &fatal_error_handler)
        : TManagedThreadPoolBase(fatal_error_handler) {
    }

    /* Construct thread pool with given fatal error handler and default
       configuration. */
    explicit TManagedThreadPool(TFatalErrorHandler &&fatal_error_handler)
        : TManagedThreadPoolBase(std::move(fatal_error_handler)) {
    }

    /* After calling Start(), pool should not be destroyed until it has been
       properly shut down (see RequestShutdown(), GetShutdownWaitFd(), and
       WaitForShutdown()). */
    virtual ~TManagedThreadPool() noexcept {
    }

    /* Allocate worker from pool and return a wrapper object containing it.
       If pool idle list is empty, a new worker will be created and added to
       pool. */
    TReadyWorker GetReadyWorker() {
      assert(this);
      return TReadyWorker(&static_cast<TWorker &>(GetAvailableWorker()));
    }

    private:
    class TWorker;

    protected:
    /* Our base class calls this when it needs to create a new thread to add to
       the pool. */
    virtual TWorkerBase *CreateWorker(bool start) override {
      assert(this);
      return new TWorker(*this, start);
    }

    private:
    /* A worker thread.  Our base class creates these, adds them to the pool as
       needed, and destroys them when they have been idle too long (as defined
       by pool config parameters). */
    class TWorker final : public TManagedThreadPoolBase::TWorkerBase {
      NO_COPY_SEMANTICS(TWorker);

      public:
      /* If 'start' is true, then the thread is created immediately and enters
         the idle state.  Otherwise, the thread is not created immediately
         (i.e. the worker starts out empty). */
      TWorker(TManagedThreadPool &my_pool, bool start)
          : TManagedThreadPoolBase::TWorkerBase(my_pool, start),
            WorkFn(nullptr) {
      }

      virtual ~TWorker() noexcept {
      }

      /* Return function pointer or object that worker calls to perform work.
       */
      TWorkCallable &GetWorkFn() noexcept {
        assert(this);
        return WorkFn;
      }

      protected:
      /* Perform work by calling the client-defined callable object.  When
         finished, empty the function wrapper, releasing any resources held
         within. */
      virtual void DoWork() override {
        assert(this);

        /* Make sure any resources held by 'WorkFn' are released, even if the
           call to 'WorkFn' throws. */
        class t_cleanup final {
          public:
          explicit t_cleanup(TWorker &worker)
              : Worker(worker) {
          }

          ~t_cleanup() noexcept {
            Worker.ClearWorkFn();
          }

          private:
          TWorker &Worker;
        } cleanup(*this);

        WorkFn();
      }

      virtual void PrepareForPutBack() noexcept {
        assert(this);
        ClearWorkFn();
      }

      private:
      /* Release any resources held within 'WorkFn' (in case it is a function
         object) and reset it to its initial state. */
      void ClearWorkFn() noexcept {
        assert(this);

        try {
          /* Depending on the type of TWorkCallable, this may invoke an
             overloaded operator function.  The function should not throw, but
             be prepared just in case it does. */
          WorkFn = nullptr;
        } catch (const std::exception &x) {
          std::string msg(
              "Fatal error while clearing thread pool worker function: ");
          msg += x.what();
          HandleFatalError(msg.c_str());
        } catch (...) {
          HandleFatalError("Fatal error while clearing thread pool worker "
              "function: Unknown exception");
        }
      }

      /* Function pointer or callable object that worker calls to perform work.
       */
      TWorkCallable WorkFn;
    };  // TWorker
  };  // TManagedThreadPool

}  // Thread
