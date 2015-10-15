/* <thread/managed_thread_pool_base.cc>

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

   Implements <thread/managed_thread_pool_base.h>.
 */

#include <thread/managed_thread_pool_base.h>

#include <algorithm>
#include <array>

#include <base/error_utils.h>
#include <base/time_util.h>

#include <poll.h>

using namespace Base;
using namespace Thread;

TManagedThreadPoolBase::TWorkerError::TWorkerError()
    : ErrorType(TWorkerErrorType::ThrewUnknownException),
      ThreadId(std::this_thread::get_id()) {
}

TManagedThreadPoolBase::TWorkerError::TWorkerError(
    const char *std_exception_what)
    : ErrorType(TWorkerErrorType::ThrewStdException),
      StdExceptionWhat(std_exception_what),
      ThreadId(std::this_thread::get_id()) {
}

TManagedThreadPoolBase::TConfig::TConfig()
    : MinPoolSize(0),
      PruneQuantumMs(30000),
      PruneQuantumCount(10),
      MaxPruneFraction(500),
      MinIdleFraction(20) {
}

TManagedThreadPoolBase::TConfig::TConfig(size_t min_pool_size,
    size_t prune_quantum_ms, size_t prune_quantum_count,
    size_t max_prune_fraction, size_t min_idle_fraction)
    : MinPoolSize(min_pool_size),
      PruneQuantumMs(prune_quantum_ms),
      PruneQuantumCount(prune_quantum_count),
      MaxPruneFraction(max_prune_fraction),
      MinIdleFraction(min_idle_fraction) {
  if (PruneQuantumMs == 0) {
    throw std::logic_error("PruneQuantumMs must be > 0");
  }

  if (PruneQuantumCount == 0) {
    throw std::logic_error("PruneQuantumCount must be > 0");
  }

  if (MaxPruneFraction > 1000) {
    throw std::logic_error("MaxPruneFraction must be <= 1000");
  }

  if (MinIdleFraction > 1000) {
    throw std::logic_error("MinIdleFraction must be <= 1000");
  }
}

bool TManagedThreadPoolBase::TConfig::operator==(
    const TConfig &that) const noexcept {
  assert(this);
  return (MinPoolSize == that.MinPoolSize) &&
      (PruneQuantumMs == that.PruneQuantumMs) &&
      (PruneQuantumCount == that.PruneQuantumCount) &&
      (MaxPruneFraction == that.MaxPruneFraction) &&
      (MinIdleFraction == that.MinIdleFraction);
}

void TManagedThreadPoolBase::TConfig::SetPruneQuantumMs(
    size_t prune_quantum_ms) {
  assert(this);

  if (prune_quantum_ms == 0) {
    throw std::logic_error("PruneQuantumMs must be > 0");
  }

  PruneQuantumMs = prune_quantum_ms;
}

void TManagedThreadPoolBase::TConfig::SetPruneQuantumCount(
    size_t prune_quantum_count) {
  assert(this);

  if (prune_quantum_count == 0) {
    throw std::logic_error("PruneQuantumCount must be > 0");
  }

  PruneQuantumCount = prune_quantum_count;
}

void TManagedThreadPoolBase::TConfig::SetMaxPruneFraction(
    size_t max_prune_fraction) {
  assert(this);

  if (max_prune_fraction > 1000) {
    throw std::logic_error("MaxPruneFraction must be <= 1000");
  }

  MaxPruneFraction = max_prune_fraction;
}

void TManagedThreadPoolBase::TConfig::SetMinIdleFraction(
    size_t min_idle_fraction) {
  assert(this);

  if (min_idle_fraction > 1000) {
    throw std::logic_error("MinIdleFraction must be <= 1000");
  }

  MinIdleFraction = min_idle_fraction;
}

TManagedThreadPoolBase::TStats::TStats()
    : SetConfigCount(0),
      ReconfigCount(0),
      PruneOpCount(0),
      PrunedThreadCount(0),
      MinPrunedByOp(0),
      MaxPrunedByOp(0),
      PoolHitCount(0),
      PoolMissCount(0),
      CreateWorkerCount(0),
      PutBackCount(0),
      FinishWorkCount(0),
      QueueErrorCount(0),
      NotifyErrorCount(0),
      LiveWorkerCount(0),
      IdleWorkerCount(0) {
}

TManagedThreadPoolBase::~TManagedThreadPoolBase() noexcept {
  try {
    if (IsStarted()) {
      /* This handles the case where a fatal exception is causing unexpected
         destruction of the thread pool before it has been properly shut down.
         Under normal operation, we should never get here. */
      RequestShutdown();
      WaitForShutdown();
    }
  } catch (...) {
  }
}

void TManagedThreadPoolBase::SetConfig(const TConfig &cfg) {
  assert(this);
  bool notify = false;

  {
    std::lock_guard<std::mutex> lock(PoolLock);

    if (cfg != Config) {
      ++Stats.SetConfigCount;
      Config = cfg;
      notify = !ReconfigPending;
      ReconfigPending = true;
    }
  }

  if (notify) {
    /* Tell manager thread to update pool config. */
    ReconfigSem.Push();
  }
}

void TManagedThreadPoolBase::Start(bool populate) {
  assert(this);

  if (Manager.IsStarted()) {
    throw std::logic_error("Thread pool is already started");
  }

  assert(!AllWorkersFinished.GetFd().IsReadable());

  /* Reset any remaining state from previous run. */
  ErrorPendingSem.Reset();

  size_t create_count = populate ? Config.GetMinPoolSize() : 0;
  std::list<TWorkerBasePtr> initial_workers(create_count);

  for (auto &item : initial_workers) {
    item.reset(CreateWorker(true));
  }

  std::list<TWorkerError> old_worker_errors;

  {
    std::lock_guard<std::mutex> lock(PoolLock);
    assert(IdleList.Empty());
    assert(IdleList.SegmentCount() == 1);
    assert(BusyList.empty());
    assert(LiveWorkerCount == 0);

    /* Reset any remaining state from previous run. */
    old_worker_errors = std::move(WorkerErrorList);
    Stats = TStats();

    Stats.CreateWorkerCount += create_count;
    IdleList.AddNew(initial_workers);
    LiveWorkerCount = create_count;
    PoolIsReady = true;
  }

  Manager.Start();
}

std::list<TManagedThreadPoolBase::TWorkerError>
TManagedThreadPoolBase::GetAllPendingErrors() {
  assert(this);
  std::list<TManagedThreadPoolBase::TWorkerError> result;

  {
    std::lock_guard<std::mutex> lock(PoolLock);
    result = std::move(WorkerErrorList);
  }

  if (!result.empty()) {
    ErrorPendingSem.Pop();
  }

  return std::move(result);
}

TManagedThreadPoolBase::TStats
TManagedThreadPoolBase::GetStats() const {
  assert(this);

  std::lock_guard<std::mutex> lock(PoolLock);
  Stats.LiveWorkerCount = LiveWorkerCount;
  Stats.IdleWorkerCount = IdleList.Size();
  return Stats;
}

void TManagedThreadPoolBase::RequestShutdown() {
  assert(this);

  if (!Manager.IsStarted()) {
    throw std::logic_error(
        "Cannot call RequestShutdown() on thread pool that is not started");
  }

  Manager.RequestShutdown();
}

void TManagedThreadPoolBase::WaitForShutdown() {
  assert(this);

  if (!Manager.IsStarted()) {
    throw std::logic_error(
        "Cannot call WaitForShutdown() on thread pool that is not started");
  }

  Manager.Join();

  /* At this point the manager and all workers have terminated, so we shouldn't
     need to acquire 'PoolLock'.  Acquire it anyway, just in case a possibly
     buggy client tries to access the pool while we are still shutting down.
     Defensive programming doesn't cost us anything here. */
  {
    std::lock_guard<std::mutex> lock(PoolLock);
    assert(IdleList.Empty());
    assert(BusyList.empty());
    assert(LiveWorkerCount == 0);
    assert(!PoolIsReady);
    ReconfigPending = false;
  }

  AllWorkersFinished.Reset();
  ReconfigSem.Reset();
}

void TManagedThreadPoolBase::TWorkerBase::PutBack(
    TWorkerBase *worker) noexcept {
  /* Note: This code may be invoked from a destructor, so we avoid letting
     exceptions escape. */
  assert(worker);

  /* Get pool here, since worker may no longer exist when we need to use pool
     below.  This is guaranteed not to throw. */
  TManagedThreadPoolBase &pool = worker->GetPool();

  try {
    DoPutBack(worker);
    /* At this point, the worker may no longer exist. */
  } catch (const std::exception &x) {
    std::string msg("Fatal error when releasing unused thread pool worker: ");
    msg += x.what();
    pool.HandleFatalError(msg.c_str());
  } catch (...) {
    pool.HandleFatalError("Fatal error when releasing unused thread pool "
        "worker: Unknown exception");
  }
}

TManagedThreadPoolBase::TWorkerBase::~TWorkerBase() noexcept {
  if (WorkerThread.joinable()) {
    /* should happen only on fatal error */
    WorkerThread.join();
  }
}

void TManagedThreadPoolBase::TWorkerBase::Activate() {
  assert(this);

  if (WorkerThread.joinable()) {
    /* The thread was obtained from the pool.  Start it working. */
    TerminateRequested = false;
    WakeupWait.unlock();
  } else {
    /* The pool had no available threads, so we are creating a new one (and
       adding it to the pool).  Create the thread, and start it running in the
       busy state. */
    WorkerThread = std::thread(std::bind(&TWorkerBase::BusyRun, this));
  }

  assert(WorkerThread.joinable());
}

TManagedThreadPoolBase::TWorkerBase::TWorkerBase(
    TManagedThreadPoolBase &my_pool, bool start)
    : MyPool(my_pool),
      BusyListPos(my_pool.BusyList.end()),
      TerminateRequested(false) {
  /* Lock this _before_ starting the worker, so it blocks when attempting to
     acquire the lock.  The worker sleeps on this when in the idle state. */
  WakeupWait.lock();

  if (start) {
    WorkerThread = std::thread(std::bind(&TWorkerBase::IdleRun, this));
  }
}

void TManagedThreadPoolBase::TWorkerBase::DoPutBack(TWorkerBase *worker) {
  assert(worker);

  /* Allow worker to release any resources it holds before possibly returning
     to idle list. */
  worker->PrepareForPutBack();

  /* This is either empty or contains a single item: the smart pointer that
     owns the object pointed to by 'worker'.  If it goes out of scope nonempty
     then the worker object gets destroyed. */
  std::list<TWorkerBasePtr> w_ptr;

  bool shutdown_notify = false;
  TManagedThreadPoolBase &pool = worker->GetPool();

  /* When true, we are putting back a worker object that was obtained from the
     idle list, and contains an idle thread.  When false, we are disposing of a
     worker object that was created because the pool had no idle workers.  In
     the latter case, the worker object doesn't yet contain a thread, but it is
     on the busy list and pool.LiveWorkerCount has been incremented for it. */
  bool from_pool = worker->IsStarted();

  {
    std::lock_guard<std::mutex> lock(pool.PoolLock);
    ++pool.Stats.PutBackCount;
    assert(worker->BusyListPos != pool.BusyList.end());
    worker->XferFromBusyList(w_ptr);
    assert(w_ptr.size() == 1);
    assert(pool.LiveWorkerCount);

    if (pool.PoolIsReady) {
      /* Return worker to idle list if it was obtained from there.  Otherwise
         we will destroy worker below. */
      if (from_pool) {
        pool.IdleList.AddNew(w_ptr);
        assert(w_ptr.empty());
      }
    } else {
      /* The pool is shutting down, so we will destroy the worker regardless of
         whether it came from the idle list.  If we are destroying the last
         remaining worker, we must notify the pool manager that the shutdown is
         complete. */
      shutdown_notify = (pool.LiveWorkerCount == 1);
    }

    if (!w_ptr.empty()) {
      /* We are destroying the worker, so we must decrement this. */
      --pool.LiveWorkerCount;
    }
  }

  if (from_pool && !w_ptr.empty()) {
    /* The worker we are destroying came from the idle list, and therefore
       contains a thread (in the idle state).  We must terminate the thread. */
    TWorkerBasePtr &p = w_ptr.front();
    p->Terminate();
    p->Join();
  }

  if (shutdown_notify) {
    pool.AllWorkersFinished.Push();
  }
}

void TManagedThreadPoolBase::TWorkerBase::Terminate() {
  assert(this);
  assert(WorkerThread.joinable());
  TerminateRequested = true;
  WakeupWait.unlock();
}

void TManagedThreadPoolBase::TWorkerBase::XferFromBusyList(
    std::list<TWorkerBasePtr> &dst) noexcept {
  assert(this);
  assert(BusyListPos != MyPool.BusyList.end());
  dst.splice(dst.end(), MyPool.BusyList, BusyListPos);
  BusyListPos = MyPool.BusyList.end();
}

TManagedThreadPoolBase::TWorkerBase::TAfterBusyAction
TManagedThreadPoolBase::TWorkerBase::LeaveBusyList() noexcept {
  assert(this);
  assert(BusyListPos != MyPool.BusyList.end());
  TAfterBusyAction next_action = TAfterBusyAction::BecomeIdle;
  std::list<TWorkerBasePtr> my_ptr;
  XferFromBusyList(my_ptr);
  assert(my_ptr.size() == 1);

  if (MyPool.PoolIsReady) {
    /* We are becoming idle so put self back on idle list. */
    MyPool.IdleList.AddNew(my_ptr);
  } else {
    /* Pool is shutting down, so we will terminate.  Put self on join list to
       be cleaned up by manager thread. */
    MyPool.JoinList.splice(MyPool.JoinList.end(), my_ptr);
    assert(MyPool.LiveWorkerCount);
    --MyPool.LiveWorkerCount;

    /* If we are the last remaining thread, we must notify the manager that
       shutdown is complete. */
    next_action = (MyPool.LiveWorkerCount == 0) ?
        TAfterBusyAction::NotifyAndTerminate :
        TAfterBusyAction::Terminate;
  }

  assert(my_ptr.empty());
  return next_action;
}

void TManagedThreadPoolBase::TWorkerBase::DoBusyRun() {
  assert(this);
  std::list<TWorkerError> error;
  TAfterBusyAction next_action = TAfterBusyAction::BecomeIdle;

  do {
    /* enter busy state */
    assert(BusyListPos != MyPool.BusyList.end());

    /* Note that we are accessing MyPool.PoolIsReady even though we don't hold
       MyPool.PoolLock.  In this case, it's ok.  The test is not needed for
       correctness, but helps ensure fast response to a shutdown request. */
    if (MyPool.PoolIsReady) {
      /* Perform work for client.  If client code throws, report error. */
      try {
        DoWork();
      } catch (const std::exception &x) {
        error.push_back(TWorkerError(x.what()));
      } catch (...) {
        error.push_back(TWorkerError());
      }
    }

    bool error_notify = false;

    {
      std::lock_guard<std::mutex> lock(MyPool.PoolLock);
      ++MyPool.Stats.FinishWorkCount;

      if (!error.empty()) {
        ++MyPool.Stats.QueueErrorCount;
        error_notify = MyPool.WorkerErrorList.empty();

        if (error_notify) {
          ++MyPool.Stats.NotifyErrorCount;
        }

        MyPool.WorkerErrorList.splice(MyPool.WorkerErrorList.end(), error);
      }

      /* Return to idle list unless pool is shutting down. */
      next_action = LeaveBusyList();
    }

    assert(error.empty());

    if (error_notify) {
      MyPool.ErrorPendingSem.Push();
    }

    if (next_action != TAfterBusyAction::BecomeIdle) {
      break;  // terminate because pool is shutting down
    }

    WakeupWait.lock();  // sleep in idle state
  } while (!TerminateRequested);

  if (next_action == TAfterBusyAction::NotifyAndTerminate) {
    /* We are the last remaining worker, so notify manager that shutdown is
       complete. */
    MyPool.AllWorkersFinished.Push();
  }
}

void TManagedThreadPoolBase::TWorkerBase::BusyRun() {
  assert(this);

  try {
    DoBusyRun();
  } catch (const std::exception &x) {
    std::string msg("Fatal error in thread pool worker: ");
    msg += x.what();
    MyPool.HandleFatalError(msg.c_str());
  } catch (...) {
    MyPool.HandleFatalError(
        "Fatal error in thread pool worker: Unknown exception");
  }
}

void TManagedThreadPoolBase::TWorkerBase::IdleRun() {
  assert(this);

  try {
    WakeupWait.lock();  // sleep in idle state

    if (!TerminateRequested) {
      DoBusyRun();
    }
  } catch (const std::exception &x) {
    std::string msg("Fatal error in thread pool worker: ");
    msg += x.what();
    MyPool.HandleFatalError(msg.c_str());
  } catch (...) {
    MyPool.HandleFatalError(
        "Fatal error in thread pool worker: Unknown exception");
  }
}

TManagedThreadPoolBase::TManagedThreadPoolBase(
    const TFatalErrorHandler &fatal_error_handler, const TConfig &cfg)
    : FatalErrorHandler(fatal_error_handler),
      LiveWorkerCount(0),
      PoolIsReady(false),
      Config(cfg),
      ReconfigPending(false),
      Manager(*this) {
}

TManagedThreadPoolBase::TManagedThreadPoolBase(
    TFatalErrorHandler &&fatal_error_handler, const TConfig &cfg)
    : FatalErrorHandler(std::move(fatal_error_handler)),
      LiveWorkerCount(0),
      PoolIsReady(false),
      Config(cfg),
      ReconfigPending(false),
      Manager(*this) {
}

TManagedThreadPoolBase::TManagedThreadPoolBase(
    const TFatalErrorHandler &fatal_error_handler)
    : TManagedThreadPoolBase(fatal_error_handler, TConfig()) {
}

TManagedThreadPoolBase::TManagedThreadPoolBase(
    TFatalErrorHandler &&fatal_error_handler)
    : TManagedThreadPoolBase(std::move(fatal_error_handler), TConfig()) {
}

TManagedThreadPoolBase::TWorkerBase &
TManagedThreadPoolBase::GetAvailableWorker() {
  assert(this);

  {
    std::lock_guard<std::mutex> lock(PoolLock);

    if (!PoolIsReady) {
      /* pool is shutting down or not yet started */
      throw TPoolNotReady();
    }

    std::list<TWorkerBasePtr> ready_worker = IdleList.RemoveOneNewest();

    if (!ready_worker.empty()) {
      /* We got a worker from the idle list.  Put it on the busy list and
         provide it to the client. */
      ++Stats.PoolHitCount;
      return AddToBusyList(ready_worker);
    }
  }

  /* The idle list was empty so we must create a new worker.  The worker
     initially contains no thread.  The thread is created and immediately
     enters the busy state when the client launches the worker. */
  std::list<TWorkerBasePtr> new_worker(1);
  new_worker.front().reset(CreateWorker(false));

  /* Even though the worker doesn't yet contain a thread, we still count it as
     "live" and add it to the busy list.  In the case where the pool starts
     shutting down before the client either launches the worker or releases it
     without launching, this forces the manager to wait for the client to
     commit to one action or the other before finishing the shutdown. */
  std::lock_guard<std::mutex> lock(PoolLock);
  ++LiveWorkerCount;
  ++Stats.PoolMissCount;
  ++Stats.CreateWorkerCount;
  return AddToBusyList(new_worker);
}

TManagedThreadPoolBase::TManager::TManager(TManagedThreadPoolBase &my_pool)
    : MyPool(my_pool) {
}

TManagedThreadPoolBase::TManager::~TManager() noexcept {
  ShutdownOnDestroy();
}

void TManagedThreadPoolBase::TManager::Run() {
  assert(this);

  try {
    DoRun();
  } catch (const std::exception &x) {
    std::string msg("Fatal error in thread pool manager: ");
    msg += x.what();
    MyPool.HandleFatalError(msg.c_str());
  } catch (...) {
    MyPool.HandleFatalError(
        "Fatal error in thread pool manager: Unknown exception");
  }
}

uint64_t TManagedThreadPoolBase::TManager::HandleReconfig(
    uint64_t old_prune_at, uint64_t now) {
  assert(this);
  MyPool.ReconfigSem.Pop();
  bool reset_segments = false;
  size_t old_prune_quantum_ms = Config.GetPruneQuantumMs();

  {
    std::lock_guard<std::mutex> lock(MyPool.PoolLock);
    ++MyPool.Stats.ReconfigCount;
    MyPool.ReconfigPending = false;

    if ((MyPool.Config.GetPruneQuantumMs() != Config.GetPruneQuantumMs()) ||
        (MyPool.Config.GetPruneQuantumCount() !=
            Config.GetPruneQuantumCount())) {
      MyPool.IdleList.ResetSegments();
      reset_segments = true;
    }

    /* update private copy of pool config */
    Config = MyPool.Config;
  }

  if (reset_segments) {
    return now + Config.GetPruneQuantumMs();
  }

  return (Config.GetPruneQuantumMs() < old_prune_quantum_ms) ?
      old_prune_at - (old_prune_quantum_ms - Config.GetPruneQuantumMs()) :
      old_prune_at + (Config.GetPruneQuantumMs() - old_prune_quantum_ms);
}

size_t
TManagedThreadPoolBase::TManager::GetMaxThreadsToPrune() const noexcept {
  assert(this);
  assert(MyPool.IdleList.Size() <= MyPool.LiveWorkerCount);

  if (MyPool.LiveWorkerCount <= Config.GetMinPoolSize()) {
    /* Prevent integer wraparound in calculation of 'max1' below. */
    return 0;
  }

  /* Compute max prune count imposed by Config.GetMinIdleFraction().
     Define the following:

       i = initial idle list size
       b = initial busy list size (i.e. total thread count - idle list size)
       F = min idle fraction (from config)
       x = The # of threads one would have to prune to make the final idle
           fraction exactly equal F.  In general, this will not be an integer.

     Then we have the following:

       (i - x) / (i + b - x) = F / 1000

     Solving for x, we get the following:

       x = (((1000 - F) * i) - (F * b)) / (1000 - F)

     Now define the following:

       v = (1000 - F) * i
       w = F * b

     Then the above solution can be rewritten as:

       x = (v - w) / (1000 - F)

     Below we compute x (rounded down since we have to prune an integer number
     of threads), while handling the following special cases:

       case 1: b = 0
         Since all threads are idle, we can prune all of them while satisfying
         F.

       case 2: F = 1000 and b > 0
         Here we can't prune any threads.  This case must be handled specially
         to prevent division by 0 in the above formula.

       case 3: w > v
         In this case we would have to prune a negative number of threads to
         satisfy F exactly.  In other words, we can't prune any threads.
   */

  size_t max2 = 0;  // x above

  if (MyPool.IdleList.Size() == MyPool.LiveWorkerCount) {
    max2 = MyPool.IdleList.Size();  // case 1
  } else {
    size_t d = 1000 - Config.GetMinIdleFraction();
    size_t v = d * MyPool.IdleList.Size();
    size_t w = Config.GetMinIdleFraction() *
        (MyPool.LiveWorkerCount - MyPool.IdleList.Size());

    if (w >= v) {
      /* This handles case 3.  It also handles case 2: If F is 1000 then v is
         0.  Since case 1 didn't apply, w > 0, so we return here. */
      return 0;
    }

    /* Integer division rounds our result down, which is what we want. */
    max2 = (v - w) / d;
  }

  /* Compute max prune count imposed by Config.GetMinPoolSize(). */
  size_t max1 = MyPool.LiveWorkerCount - Config.GetMinPoolSize();

  /* Compute max prune count imposed by Config.GetMaxPruneFraction(). */
  size_t max3 = MyPool.LiveWorkerCount * Config.GetMaxPruneFraction() / 1000;

  /* To satisfy all 3 criteria, we must return the minimum of the 3 max prune
     counts. */
  return std::min(std::min(max1, max2), max3);
}

void TManagedThreadPoolBase::TManager::PruneThreadPool() {
  assert(this);
  std::list<TWorkerBasePtr> pruned;

  {
    std::lock_guard<std::mutex> lock(MyPool.PoolLock);
    ++MyPool.Stats.PruneOpCount;

    if (MyPool.IdleList.SegmentCount() < Config.GetPruneQuantumCount()) {
      /* Add empty segment to front of idle list, shifting older segments back
         one position.  The oldest segment isn't yet old enough to prune. */
      MyPool.IdleList.AddNewSegment();
      return;
    }

    /* Try to prune as many threads as possible from oldest segment, according
       to pool config. */
    assert(MyPool.IdleList.SegmentCount() == Config.GetPruneQuantumCount());
    size_t initial_idle_count = MyPool.IdleList.Size();
    pruned = MyPool.IdleList.RemoveOldest(GetMaxThreadsToPrune());
    assert(MyPool.IdleList.Size() <= initial_idle_count);
    assert(MyPool.LiveWorkerCount >= initial_idle_count);
    size_t prune_count = initial_idle_count - MyPool.IdleList.Size();
    MyPool.LiveWorkerCount -= prune_count;
    MyPool.IdleList.RecycleOldestSegment();
    MyPool.Stats.PrunedThreadCount += prune_count;

    if ((MyPool.Stats.PruneOpCount == 1) ||
        (prune_count < MyPool.Stats.MinPrunedByOp)) {
      MyPool.Stats.MinPrunedByOp = prune_count;
    }

    if (prune_count > MyPool.Stats.MaxPrunedByOp) {
      MyPool.Stats.MaxPrunedByOp = prune_count;
    }
  }

  /* Tell pruned workers to terminate. */
  for (auto &worker : pruned) {
    worker->Terminate();
  }

  /* Wait for termination of pruned workers to finish. */
  for (auto &worker : pruned) {
    worker->Join();
  }
}

void TManagedThreadPoolBase::TManager::HandleShutdownRequest() {
  assert(this);
  std::list<TWorkerBasePtr> idle_workers, dead_workers;
  bool wait_for_workers = false;

  {
    /* Remove all idle and terminated workers from pool.  If any busy workers
       remain, we will wait for them to terminate.  Workers performing long-
       running tasks should monitor the pool's shutdown request semaphore, and
       terminate quickly once shutdown has started. */
    std::lock_guard<std::mutex> lock(MyPool.PoolLock);
    assert(MyPool.IdleList.Size() <= MyPool.LiveWorkerCount);
    MyPool.LiveWorkerCount -= MyPool.IdleList.Size();
    idle_workers = MyPool.IdleList.EmptyAllAndResetSegments();
    dead_workers = std::move(MyPool.JoinList);
    wait_for_workers = (MyPool.LiveWorkerCount != 0);
    MyPool.PoolIsReady = false;
  }

  /* Wake up idle workers and tell them to terminate. */
  for (auto &worker : idle_workers) {
    worker->Terminate();
  }

  dead_workers.splice(dead_workers.end(), idle_workers);

  for (auto &worker : dead_workers) {
    worker->Join();
  }

  dead_workers.clear();

  if (wait_for_workers) {
    /* Wait for last busy worker to notify us that it is terminating. */
    MyPool.AllWorkersFinished.Pop();
  }

  assert(MyPool.IdleList.Empty());
  assert(MyPool.BusyList.empty());
  assert(MyPool.LiveWorkerCount == 0);

  /* At this point all workers have terminated, so we shouldn't need to acquire
     MyPool.PoolLock.  Acquire it anyway, just in case a possibly buggy client
     tries to access the pool while we are still shutting down.  Defensive
     programming doesn't cost us anything here. */
  {
    std::lock_guard<std::mutex> lock(MyPool.PoolLock);
    dead_workers = std::move(MyPool.JoinList);
  }

  for (auto &worker : dead_workers) {
    worker->Join();
  }
}

void TManagedThreadPoolBase::TManager::DoRun() {
  assert(this);

  {
    /* make private copy of pool config */
    std::lock_guard<std::mutex> lock(MyPool.PoolLock);
    Config = MyPool.Config;
  }

  std::array<struct pollfd, 2> events;
  struct pollfd &reconfig_event = events[0];
  struct pollfd &shutdown_request_event = events[1];
  reconfig_event.fd = MyPool.ReconfigSem.GetFd();
  reconfig_event.events = POLLIN;
  shutdown_request_event.fd = GetShutdownRequestFd();
  shutdown_request_event.events = POLLIN;
  uint64_t now = GetMonotonicRawMilliseconds();
  uint64_t prune_at = now + Config.GetPruneQuantumMs();

  for (; ; ) {
    for (auto &item : events) {
      item.revents = 0;
    }

    int timeout = Config.GetMaxPruneFraction() ?
        ((prune_at < now) ? 0 : static_cast<int>(prune_at - now)) : -1;
    IfLt0(poll(&events[0], events.size(), timeout));

    if (shutdown_request_event.revents) {
      break;
    }

    now = GetMonotonicRawMilliseconds();

    if (reconfig_event.revents) {
      /* Update pool config, as requested by client. */
      prune_at = HandleReconfig(prune_at, now);
    }

    if (Config.GetMaxPruneFraction() && (now >= prune_at)) {
      /* Terminate threads that have been idle for too long, according to pool
         config. */
      PruneThreadPool();
      prune_at += Config.GetPruneQuantumMs();
      now = GetMonotonicRawMilliseconds();
    }
  }

  /* We got a shutdown request.  Before terminating, clean up all remaining
     workers. */
  HandleShutdownRequest();
}

TManagedThreadPoolBase::TWorkerBase &
TManagedThreadPoolBase::AddToBusyList(
    std::list<TWorkerBasePtr> &ready_worker) noexcept {
  assert(this);
  assert(ready_worker.size() == 1);
  BusyList.splice(BusyList.begin(), ready_worker);
  TWorkerBase &worker = *BusyList.front();
  assert(worker.BusyListPos == BusyList.end());
  worker.BusyListPos = BusyList.begin();
  return worker;
}
