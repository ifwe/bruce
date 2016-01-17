/* <thread/managed_thread_pool_base.h>

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

   Thread pool base class.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <thread/fd_managed_thread.h>
#include <thread/segmented_list.h>

namespace Thread {

  /* Base class for thread pool whose size adjusts based on demand.  The pool
     maintains a list of busy threads and a list of idle threads.  Clients may
     concurrently allocate idle threads and give them work to do, although the
     specifics of the work are defined by a subclass.  Allocated threads are
     placed on the busy list, and return to the idle list when they finish
     their work.  If the idle list is empty on attempted allocation, a new
     thread is created and added to the pool.  Thus the pool places no upper
     bound on the number of threads.  Its only purpose is to reduce the
     overhead associated with creating and destroying them.

     A manager thread periodically wakes up and prunes threads that have been
     idle for a long time.  The manager is also responsible for shutting down
     the pool.  The manager divides the idle list into segments representing
     time intervals (see class TSegmentedList), and only prunes threads from
     the oldest segment.  The number of segments and the time interval length
     are configurable.  One may specify a minimum pool size, an upper bound on
     the fraction of threads pruned in a single time interval, and a limit that
     prevents a prune opreation from causing the number of idle threads to drop
     below a certain fraction of the total pool size.  These parameters may be
     dynamically adjusted while the pool operates.

     Clients are not given direct access to the pool's threads.  Rather, the
     client receives an allocated thread inside a wrapper object, which is a
     subclass of TManagedThreadPoolBase::TReadyWorkerBase.  The wrapper
     provides RAII behavior to prevent resource leakage.  It is expected to
     provide an API that allows the client to perform whatever configuration is
     required to assign work to the thread (for instance, providing it with a
     function to execute and a file descriptor representing a TCP connection to
     handle).  Once finished with configuration, the client calls the wrapper's
     Launch() method to start the thread working.  If the wrapper's destructor
     is invoked before Launch() is called, all resources contained within are
     released.  For instance, if the thread was allocated from the idle list,
     it will be returned to the idle list.

     A file descriptor is provided that becomes readable when a request to
     shut down the pool is received.  Workers that perform long-running tasks
     are expected to monitor it, and finish their work when it becomes
     readable.  If an exception escapes from a busy worker, the thread pool
     mechanism catches and reports it via an error reporting queue, and returns
     the worker to the idle list. */
  class TManagedThreadPoolBase {
    NO_COPY_SEMANTICS(TManagedThreadPoolBase);

    public:
    /* Fatal error handler.  Function should report error and terminate
       program immediately. */
    using TFatalErrorHandler = std::function<void(const char *) noexcept>;

    class TPoolNotReady final : public std::runtime_error {
      public:
      TPoolNotReady()
          : std::runtime_error("Attempt to get thread from pool that is "
                "either shutting down or not started") {
      }
    };  // TPoolNotReady

    /* For reporting exceptions thrown by client-supplied worker code. */
    enum class TWorkerErrorType {
      ThrewStdException,  // worker threw std::exception or subclass
      ThrewUnknownException  // worker threw something else
    };  // TWorkerErrorType

    /* For reporting exceptions thrown by client-supplied worker code. */
    struct TWorkerError final {
      TWorkerErrorType ErrorType;

      /* If ErrorType is TWorkerErrorType::ThrewStdException then this holds
         the what() string from the exception.  Otherwise it is empty. */
      std::string StdExceptionWhat;

      /* Thread ID of worker that produced error. */
      std::thread::id ThreadId;

      TWorkerError();

      explicit TWorkerError(const char *std_exception_what);

      TWorkerError(const TWorkerError &) = default;

      TWorkerError(TWorkerError &&) = default;

      TWorkerError &operator=(const TWorkerError &) = default;

      TWorkerError &operator=(TWorkerError &&) = default;
    };  // TWorkerError

    /* Class for specifying thread pool config parameters. */
    class TConfig final {
      public:
      TConfig();

      /* Config parameters:

             min_pool_size: Prevents pool manager from pruning threads if after
                 pruning, the pool size (active + idle) would be below this
                 limit.  Must be >= 0.

             prune_quantum_ms: The prune interval length in milliseconds.  At
                 the end of each interval, the manager thread wakes up and sees
                 if there is anything to prune.  Must be > 0.

             prune_quantum_count: The number of intervals in the pool's idle
                 list.  Each interval corresponds to a time quantum whose
                 length is 'prune_quantum_ms'.  The manager only prunes threads
                 in the oldest quantum.  Must be > 0.  See TSegmentedList for
                 details.

             max_prune_fraction: Must be >= 0 and <= 1000.  Prevents the
                 manager from performing a pruning operation that would destroy
                 more than this many thousandths of the total pool size (active
                 + idle).  For instance, a value of 500 would prevent a single
                 prune operation from destroying more than half of the worker
                 threads.  Setting this to 0 disables pruning.

             min_idle_fraction: Must be >= 0 and <= 1000.  Prevents the
                 manager from performing a pruning operation that would leave
                 fewer than this many thousandths of the total pool size idle.
                 For instance, a value of 15 would prevent a pruning operation
                 that would leave fewer than 1.5 percent of the worker threads
                 idle.
       */
      TConfig(size_t min_pool_size, size_t prune_quantum_ms,
          size_t prune_quantum_count, size_t max_prune_fraction,
          size_t min_idle_fraction);

      TConfig(const TConfig &) = default;

      TConfig &operator=(const TConfig &that) = default;

      bool operator==(const TConfig &that) const noexcept;

      bool operator!=(const TConfig &that) const noexcept {
        assert(this);
        return !(*this == that);
      }

      size_t GetMinPoolSize() const noexcept {
        assert(this);
        return MinPoolSize;
      }

      void SetMinPoolSize(size_t min_pool_size) noexcept {
        assert(this);
        MinPoolSize = min_pool_size;
      }

      size_t GetPruneQuantumMs() const noexcept {
        assert(this);
        return PruneQuantumMs;
      }

      void SetPruneQuantumMs(size_t prune_quantum_ms);

      size_t GetPruneQuantumCount() const noexcept {
        assert(this);
        return PruneQuantumCount;
      }

      void SetPruneQuantumCount(size_t prune_quantum_count);

      size_t GetMaxPruneFraction() const noexcept {
        assert(this);
        return MaxPruneFraction;
      }

      void SetMaxPruneFraction(size_t max_prune_fraction);

      size_t GetMinIdleFraction() const noexcept {
        assert(this);
        return MinIdleFraction;
      }

      void SetMinIdleFraction(size_t min_idle_fraction);

      private:
      size_t MinPoolSize;

      size_t PruneQuantumMs;

      size_t PruneQuantumCount;

      size_t MaxPruneFraction;

      size_t MinIdleFraction;
    };  // TConfig

    /* Statistics on thread pool operation. */
    struct TStats final {
      /* # of times SetConfig() is called with new config. */
      size_t SetConfigCount;

      /* # of times manager does reconfig (may be less than # of times
         SetConfig() is called). */
      size_t ReconfigCount;

      /* # of prune operations performed by manager.  A single prune operation
         prunes 0 or more (possibly many) threads. */
      size_t PruneOpCount;

      /* Total # of threads pruned. */
      size_t PrunedThreadCount;

      /* Minimum # of threads pruned in a single operation. */
      size_t MinPrunedByOp;

      /* Maximum # of threads pruned in a single operation. */
      size_t MaxPrunedByOp;

      /* # of times a worker was successfully allocated from pool. */
      size_t PoolHitCount;

      /* # of times a new worker was created because the pool had no idle
         workers. */
      size_t PoolMissCount;

      /* # of times a new worker is created.  This includes pool misses and
         threads created to initially populate pool. */
      size_t CreateWorkerCount;

      /* # of times a thread is released without being launched. */
      size_t PutBackCount;

      /* # of times a thread finishes work. */
      size_t FinishWorkCount;

      /* # of times a worker error is queued for receipt by client. */
      size_t QueueErrorCount;

      /* # of times the client is notified of a queued worker error. */
      size_t NotifyErrorCount;

      /* # of busy or idle workers. */
      size_t LiveWorkerCount;

      /* # of idle workers. */
      size_t IdleWorkerCount;

      TStats();
    };  // TStats

    protected:
    class TWorkerBase;

    public:
    /* Base class for wrapper returned when client allocates a thread from the
       pool.  Wrapper contains allocated worker thread, and provides an API
       that allows the client to give it some work to do and start it working.
       Subclasses of TManagedThreadPoolBase are expected to also subclass this
       class, and use it to wrap the return value of GetAvailableWorker() when
       allocating a thread for the client. */
    class TReadyWorkerBase {
      NO_COPY_SEMANTICS(TReadyWorkerBase);

      public:
      /* Destructor releases all acquired resources, which includes returning
         worker to idle list when appropriate. */
      virtual ~TReadyWorkerBase() noexcept {
        PutBack();
      }

      /* A true value indicates that the pool contained no available threads,
         so a new one is being created (and added to the pool) to satisfy the
         request.  A false value indicates that an available thread was
         obtained from the pool.  This may facilitate maintaining metrics on
         the pool's effectiveness. */
      bool IsNew() const {
        assert(this);
        assert(Worker);
        return !Worker->IsStarted();
      }

      /* Put the worker to work and return its thread ID.  This is meant to be
         called after the client has allocated the worker and called any
         subclass methods for giving the worker something to do. */
      std::thread::id Launch() {
        assert(this);
        assert(Worker);
        Worker->Activate();
        std::thread::id id = Worker->GetId();
        Worker = nullptr;
        return id;
      }

      /* Release the worker, which includes returning the worker to the pool
         when appropriate.  If Launch() or PutBack() has already been called,
         this is a no-op.  Once this has been called, IsLaunchable() will
         return false and Launch() can no longer be called. */
      void PutBack() noexcept {
        assert(this);

        if (Worker) {
          TWorkerBase::PutBack(Worker);
          Worker = nullptr;
        }
      }

      /* Returns true until Launch() or PutBack() has been called, or wrapper
         has been assigned or move constructed from. */
      bool IsLaunchable() noexcept {
        assert(this);
        return (Worker != nullptr);
      }

      protected:
      TReadyWorkerBase() noexcept
          : Worker(nullptr) {
      }

      /* Construct wrapper containing newly allocated thread (from call to
         GetAvailableWorker()). */
      explicit TReadyWorkerBase(TWorkerBase *worker) noexcept
          : Worker(worker) {
        assert(worker);
      }

      /* Move constructor transfers allocated thread (if any) from donor
         wrapper.  Donor is left in empty state (i.e. IsLaunchable() will
         return false). */
      TReadyWorkerBase(TReadyWorkerBase &&that) noexcept
          : Worker(that.Worker) {
        that.Worker = nullptr;
      }

      /* Move assignment operator transfers allocated thread (if any) from
         donor wrapper.  Donor is left in empty state (i.e. IsLaunchable() will
         return false). */
      TReadyWorkerBase &operator=(TReadyWorkerBase &&that) noexcept {
        if (&that != this) {
          TReadyWorkerBase tmp(std::move(that));
          Swap(tmp);  // DTOR for 'tmp' releases our old state
        }

        return *this;
      }

      /* Swap our internal state with 'that'. */
      void Swap(TReadyWorkerBase &that) noexcept {
        assert(this);
        std::swap(Worker, that.Worker);
      }

      /* Return a pointer to the worker (if any) that we contain.  Return
         nullptr if we are empty.  We retain ownership of worker. */
      TWorkerBase *GetWorkerBase() const {
        assert(this);
        return Worker;
      }

      private:
      /* Pointer to allocated worker object, or nullptr if we are empty.  If
         the object was allocated from the pool's idle list, then it is now on
         the busy list but the thread it contains is still sleeping (and will
         be awakened when Launch() is called).  If the idle list was empty when
         allocation was attempted, then the object is on the busy list, but
         doesn't yet contain an actual thread.  In this case, it will be
         populated with a new thread when Launch() is called. */
      TWorkerBase *Worker;
    };  // TReadyWorkerBase

    /* After calling Start(), pool should not be destroyed until it has been
       properly shut down (see RequestShutdown(), GetShutdownWaitFd(), and
       WaitForShutdown()). */
    virtual ~TManagedThreadPoolBase() noexcept;

    /* Return the pool's current configuration. */
    TConfig GetConfig() const noexcept {
      assert(this);

      std::lock_guard<std::mutex> lock(PoolLock);
      return Config;
    }

    /* Set the thread pool's configuration to 'cfg'.  This may be called either
       before calling Start() or while the thread pool is operating.  In the
       latter case, the pool will dynamically reconfigure. */
    void SetConfig(const TConfig &cfg);

    /* Activate the thread pool.  You must call this before allocating threads.
       Once this has been called, the thread pool must be properly shut down
       before its destructor is invoked (see RequestShutdown(),
       GetShutdownWaitFd(), and WaitForShutdown()). */
    void Start(bool populate = true);

    /* True when Start() has been called, but WaitForShutdown() has not yet
       been called. */
    bool IsStarted() const noexcept {
      assert(this);
      return Manager.IsStarted();
    }

    /* Return a file descriptor that becomes readable when one or more worker
       errors is pending, which occur when an exception escapes from client-
       supplied worker code.  The error details may be obtained by calling
       GetAllPendingErrors(). */
    const Base::TFd &GetErrorPendingFd() const noexcept {
      assert(this);
      return ErrorPendingSem.GetFd();
    }

    /* Return all pending errors, which are reported when exceptions escape
       from client-supplied worker code.  Returns an empty list if there were
       no pending errors.  See GetErrorPendingFd(). */
    std::list<TWorkerError> GetAllPendingErrors();

    /* Get Pool statistics.  Results are reset when pool Start() method is
       called. */
    TStats GetStats() const;

    /* Initiate a shutdown of the thread pool.  This must be followed by a call
       to WaitForShutdown(), which finishes the shutdown operation.
       GetShutdownWaitFd() returns a file descriptor that becomes readable when
       WaitForShutdown() can be called without blocking. */
    void RequestShutdown();

    /* Return a file descriptor that becomes readable when a shutdown of the
       pool has been initiated.  Worker threads that run for an extended period
       of time must monitor this file descriptor and finish their work if it
       becomes readable. */
    const Base::TFd &GetShutdownRequestFd() const noexcept {
      assert(this);
      return Manager.GetShutdownReqFd();
    }

    /* Return a file descriptor that becomes readable when WaitForShutdown()
       can be called without blocking.  In the case where a fatal error
       prevents the pool from continuing to operate, the file descriptor will
       become readable even if RequestShutdown() has not been called.  In this
       case, WaitForShutdown must still be called to finish cleanup of the
       pool's internal state.  While the pool is running, the returned file
       descriptor should be monitored for readability so that fatal errors may
       be detected. */
    const Base::TFd &GetShutdownWaitFd() const {
      assert(this);
      return Manager.GetShutdownWaitFd();
    }

    /* Wait for the thread pool to finish shutting down (which includes
       termination of all worker threads and the manager thread), and finish
       cleaning up the pool's internal state.  Once the pool has been started
       (by calling the Start() method), this _must_ be called before the pool's
       destructor is invoked.  Once this method has been called, the Start()
       method may be called again if desired, rather than destroying the pool.
       This should be called after either RequestShutdown() has been called or
       a fatal error has been detected (indicated by the file descriptor
       returned by GetShutdownWaitFd() becoming readable even though
       RequestShutdown() has not been called).  The pool's destructor calls
       this method (after calling RequestShutdown()) in the case where the pool
       is still operating.  This is to handle the case of unexpected destructor
       invocation due to a fatal exception.  Therefore, to prevent multiple
       threads from concurrently calling this method, this should only be
       called by the same thread that invokes the pool's destructor. */
    void WaitForShutdown();

    protected:
    class TWorkerBase;

    private:
    using TWorkerBasePtr = std::unique_ptr<TWorkerBase>;

    class TManager;

    protected:
    /* Base class representing a worker thread.  Subclasses of
       TManagedThreadPoolBase are also expected to subclass this, to provide
       client-specified worker behavior.  The client never interacts directly
       with these objects.  When the client allocates a thread from the pool,
       it gets a subclass of TReadyWorkerBase, which wraps a subclass of
       TWorkerBase. */
    class TWorkerBase {
      NO_COPY_SEMANTICS(TWorkerBase);

      public:
      /* This method can only be called if 'worker' has been allocated (i.e.
         returned by GetAvailableWorker()) but its Activate() has not been
         called.  It releases the worker.  If the worker came from the idle
         list, it goes back to the idle list.  Otherwise it gets destroyed. */
      static void PutBack(TWorkerBase *worker) noexcept;

      virtual ~TWorkerBase() noexcept;

      /* Return true if this object contains an actual thread, or false
         otherwise. */
      bool IsStarted() const noexcept {
        assert(this);
        return WorkerThread.joinable();
      }

      /* When a worker is created due to the idle list being empty, calling
         this method ensures that its 'WorkerThread' member has been assigned
         before it returns to the idle list after finishing its work, thus
         preventing a race condition. */
      void SetWaitAfterDoWork() {
        assert(this);
        WaitAfterDoWork = true;
      }

      /* Returns the thread ID.  It is assumed that the thread has been started
         (i.e. IsStarted() returns true). */
      std::thread::id GetId() const noexcept {
        assert(this);
        assert(IsStarted());
        return WorkerThread.get_id();
      }

      /* Return a reference to the pool that this thread belongs to. */
      TManagedThreadPoolBase &GetPool() const noexcept {
        assert(this);
        return MyPool;
      }

      /* Start the worker working (i.e. executing client-provided worker code).
         This method handles two cases:

             Case 1: The thread was obtained from the idle list.  In this case,
             the thread is now on the busy list but is still sleeping.  Calling
             this method wakes the thread up and starts it working.

             Case 2: The idle list was empty, so we are creating a new thread.
             In this case, we are on the busy list but don't yet contain an
             actual thread (i.e. IsStarted() returns false).  Calling this
             method creates the thread and starts it working.
       */
      void Activate();

      protected:
      /* If 'start' is true then the worker is started and immediately enters
         the idle state.  Otherwise the worker is not started. */
      TWorkerBase(TManagedThreadPoolBase &my_pool, bool start);

      /* Provided as a convenience for subclasses. */
      void HandleFatalError(const char *msg) const noexcept {
        assert(this);
        MyPool.HandleFatalError(msg);
      }

      /* Subclasses must implement this method to give the worker threads
         something to do. */
      virtual void DoWork() = 0;

      /* Called by PutBack() before thread is released.  Subclass should
         release any resources it holds here, to prevent resources from being
         held by thread while on pool idle list. */
      virtual void PrepareForPutBack() noexcept = 0;

      private:
      static void DoPutBack(TWorkerBase *worker);

      /* Tell an idle worker to terminate.  Only the manager calls this.
         Worker will initially be sleeping on 'WakeupWait'. */
      void Terminate();

      /* The manager calls this after calling Terminate(), or when processing
         worker on 'JoinList'. */
      void Join() {
        assert(this);
        WorkerThread.join();
      }

      /* Remove worker from busy list and append to 'dst'.  Caller must hold
         'PoolLock'. */
      void XferFromBusyList(std::list<TWorkerBasePtr> &dst) noexcept;

      enum class TAfterBusyAction {
        BecomeIdle,
        Terminate,
        NotifyAndTerminate
      };  // TAfterBusyAction

      /* Called by worker to remove self from busy list when finished working.
         Return value indicates what worker should do next.  Caller must hold
         'PoolLock'. */
      TAfterBusyAction LeaveBusyList() noexcept;

      void DoBusyRun();

      /* Thread executes this method when starting in the busy state. */
      void BusyRun();

      /* Thread executes this method when starting in the idle state. */
      void IdleRun();

      /* pool that thread belongs to */
      TManagedThreadPoolBase &MyPool;

      /* This gets set to true when a new worker is being created because the
         idle list was empty.  When the worker finishes working, this ensures
         that its 'WorkerThread' member has been assigned before the worker
         places itself on the idle list, thus avoiding a race condition. */
      bool WaitAfterDoWork;

      /* Worker waits here until Activate() or Terminate() is called. */

      /* This serves two purposes:

             1.  When idle, the worker sleeps here until it is given work to do
                 or chosen by the manager for pruning.

             2.  When the worker is created to satisfy a client request because
                 the idle list was empty, it may (rarely) sleep here after
                 finishing work, to avoid placing itself on the idle list
                 before its 'WorkerThread' member has been assigned, thus
                 avoiding a race condition.
       */
      std::mutex WakeupWait;

      /* The worker thread.  This is initially empty when a new thread is being
         created to satisfy a client request when the idle list was empty.  In
         this case, calling Activate() creates the thread, starts it working,
         and stores its std::thread object here.

         Note: This member may be unassigned while the thread is executing for
         the first time in the above-mentioned scenario.  After finishing its
         work, the thread will not place itself on the idle list until the
         assignment has completed. */
      std::thread WorkerThread;

      /* When the worker is on the busy list, this indicates the position.
         When not on the busy list, this is set to BusyList.end(). */
      std::list<TWorkerBasePtr>::iterator BusyListPos;

      /* This is set or cleared before the worker is awakened from sleeping on
         'WakeupWait'.  If true, the worker terminates.  Otherwise the worker
         starts working. */
      bool TerminateRequested;

      /* so TManager can call Terminate() and Join() */
      friend class TManager;

      /* so TManagedThreadPoolBase can set BusyListPos */
      friend class TManagedThreadPoolBase;
    };  // TWorkerBase

    /* Subclasses call this to construct thread pool with given fatal error
       handler and configuration. */
    TManagedThreadPoolBase(const TFatalErrorHandler &fatal_error_handler,
        const TConfig &cfg);

    /* Subclasses call this to construct thread pool with given fatal error
       handler and configuration. */
    TManagedThreadPoolBase(TFatalErrorHandler &&fatal_error_handler,
        const TConfig &cfg);

    /* Subclasses call this to construct thread pool with given fatal error
       handler.  Default configuration is used, as specifie by default
       constructor for TConfig. */
    explicit TManagedThreadPoolBase(
        const TFatalErrorHandler &fatal_error_handler);

    /* Subclasses call this to construct thread pool with given fatal error
       handler.  Default configuration is used, as specifie by default
       constructor for TConfig. */
    explicit TManagedThreadPoolBase(TFatalErrorHandler &&fatal_error_handler);

    /* Called by thread pool implementation when fatal error occurs.  Handles
       error by calling client-suplied error handler, which should report the
       error and immediately terminate the program. */
    void HandleFatalError(const char *msg) const noexcept {
      assert(this);
      FatalErrorHandler(msg);
    }

    /* Derived class implements this to create new worker (a subclass of
       TWorkerBase).  If 'start' is true then the worker is started and
       immediately enters the idle state.  Otherwise the worker is not
       started. */
    virtual TWorkerBase *CreateWorker(bool start) = 0;

    /* Returned worker will be on BusyList, but still sleeping (until client
       performs any needed configuration and calls worker's Activate() method).
       The derived class calls this when a client requests an available thread,
       and returns an RAII wrapper object containing the requested thread.  The
       wrapper destructor puts the thread back on the idle list if the thread's
       Activate() method has not been called. */
    TWorkerBase &GetAvailableWorker();

    private:
    /* Manager thread, which is responsible for pruning threads that have been
       idle for a long time, and shutting down the pool. */
    class TManager final : public TFdManagedThread {
      NO_COPY_SEMANTICS(TManager);

      public:
      explicit TManager(TManagedThreadPoolBase &my_pool);

      virtual ~TManager() noexcept;

      /* Long-running worker threads are expected to monitor this, and stop
         working when it becomes readable (indicating that the pool is shutting
         down). */
      const Base::TFd &GetShutdownReqFd() const noexcept {
        assert(this);
        return GetShutdownRequestFd();
      }

      protected:
      /* Main loop for manager thread. */
      virtual void Run() override;

      private:
      /* Handle a change in the pool configuration. */
      uint64_t HandleReconfig(uint64_t old_prune_at, uint64_t now);

      /* Called during a prune operation to compute the maximum possible number
         of threads that can be pruned, based on the pool configuration. */
      size_t GetMaxThreadsToPrune() const noexcept;

      /* Perform a pruning operation.  This is called periodically at a
         frequency specified by the pool configuration. */
      void PruneThreadPool();

      /* Handle a request to shut down the pool. */
      void HandleShutdownRequest();

      void DoRun();

      /* thread pool that manager is responsible for */
      TManagedThreadPoolBase &MyPool;

      /* Manager's private copy of pool configuration.  Updated when
         configuration changes. */
      TConfig Config;
    };  // TManager

    /* 'ready_worker' is a single item list containing a ready worker.  Add it
       to the busy list and return a reference to the newly added worker.
       Caller must hold 'PoolLock'. */
    TWorkerBase &AddToBusyList(
        std::list<TWorkerBasePtr> &ready_worker) noexcept;

    /* Client-supplied fatal error handler.  This should report error and
       immediately terminate program. */
    const TFatalErrorHandler FatalErrorHandler;

    /* list of idle workers */
    TSegmentedList<TWorkerBasePtr> IdleList;

    /* list of busy workers */
    std::list<TWorkerBasePtr> BusyList;

    /* list of workers that have terminated and need to be joined by manager */
    std::list<TWorkerBasePtr> JoinList;

    /* info on exceptions thrown from client-supplied worker logic */
    std::list<TWorkerError> WorkerErrorList;

    /* This is incremented when a new worker is created.  When a worker is
       about to die (due to prune operation or shutdown request), it decrements
       this.  If 'PoolIsReady' is false when count reaches 0, the worker
       pushes 'AllWorkersFinished' to notify manager. */
    size_t LiveWorkerCount;

    /* The manager clears when it gets a shutdown request.  If
       'LiveWorkerCount' is nonzero when the manager clears this, then the
       manager waits for 'AllWorkersFinished'.  Volatile because workers test
       this in a loop. */
    volatile bool PoolIsReady;

    /* Thread pool configuration.  Manager thread maintains its own private
       copy of this, and updates its copy whenever config changes. */
    TConfig Config;

    /* true when pool configuration has changed and manager thread has not yet
       updated its state */
    bool ReconfigPending;

    /* Pool stats.  Mutable because GetStats() sets 'LiveWorkerCount' and
       'IdleWorkerCount' in 'Stats' before returning a copy. */
    mutable TStats Stats;

    /* protects everything above */
    mutable std::mutex PoolLock;

    /* manager monitors this (see above) during pool shutdown */
    Base::TEventSemaphore AllWorkersFinished;

    /* manager thread responsible for pruning idle workers and shutting down
       pool */
    TManager Manager;

    /* becomes readable when Config has changed (to let manager thread know) */
    Base::TEventSemaphore ReconfigSem;

    /* indicates that there is pending error info waiting for client on
       'WorkerErrorList' */
    Base::TEventSemaphore ErrorPendingSem;
  };  // TManagedThreadPoolBase

}  // Thread
