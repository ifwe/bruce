/* <bruce/bruce_server.cc>

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

   Implements <bruce/bruce_server.h>.
 */

#include <bruce/bruce_server.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <memory>
#include <set>
#include <system_error>

#include <poll.h>
#include <signal.h>
#include <syslog.h>
#include <time.h>

#include <base/error_utils.h>
#include <base/no_default_case.h>
#include <bruce/batch/batch_config_builder.h>
#include <bruce/compress/compression_init.h>
#include <bruce/conf/batch_conf.h>
#include <bruce/discard_file_logger.h>
#include <bruce/kafka_proto/choose_proto.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/msg.h>
#include <bruce/util/misc_util.h>
#include <bruce/web_interface.h>
#include <capped/pool.h>
#include <server/counter.h>
#include <socket/address.h>
#include <socket/option.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;
using namespace Bruce::Compress;
using namespace Bruce::Conf;
using namespace Bruce::KafkaProto;
using namespace Bruce::Util;
using namespace Capped;
using namespace Signal;
using namespace Socket;

SERVER_COUNTER(GotShutdownSignal);

TBruceServer::TServerConfig
TBruceServer::CreateConfig(int argc, char **argv, bool &large_sendbuf_required,
    size_t pool_block_size) {
  large_sendbuf_required = false;
  std::unique_ptr<TConfig> cfg(new TConfig(argc, argv));

  switch (TestUnixDgSize(cfg->MaxInputMsgSize)) {
    case TUnixDgSizeTestResult::Pass:
      break;
    case TUnixDgSizeTestResult::PassWithLargeSendbuf:
      large_sendbuf_required = true;

      if (!cfg->AllowLargeUnixDatagrams) {
        THROW_ERROR(TMustAllowLargeDatagrams);
      }

      break;
    case TUnixDgSizeTestResult::Fail:
      THROW_ERROR(TMaxInputMsgSizeTooLarge);
      break;
    NO_DEFAULT_CASE;
  }

  if (cfg->DiscardReportInterval < 1) {
    THROW_ERROR(TBadDiscardReportInterval);
  }

  if ((cfg->RequiredAcks != -1) && (cfg->RequiredAcks <= 0)) {
    THROW_ERROR(TBadRequiredAcks);
  }

  if (cfg->ReplicationTimeout >
      static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    THROW_ERROR(TBadReplicationTimeout);
  }

  if (cfg->DebugDir.empty() || (cfg->DebugDir[0] != '/')) {
    THROW_ERROR(TBadDebugDir);
  }

  if (cfg->DiscardLogMaxFileSize == 0) {
    THROW_ERROR(TBadDiscardLogMaxFileSize);
  }

  if (!cfg->DiscardLogPath.empty() &&
      ((cfg->DiscardLogMaxFileSize * 1024) < (2 * cfg->MaxInputMsgSize))) {
    /* We enforce this requirement to ensure that the discard logfile has
       enough capacity for at least one message of the maximum possible size.
       Multiplying by 2 ensures that there is more than enough space for the
       extra information in a log entry beyond just the message content. */
    THROW_ERROR(TBadDiscardLogMaxFileSize);
  }

  std::unique_ptr<const TWireProtocol> protocol(
      ChooseProto(cfg->ProtocolVersion, cfg->RequiredAcks,
          static_cast<int32_t>(cfg->ReplicationTimeout),
          cfg->RetryOnUnknownPartition));

  if (!protocol) {
    THROW_ERROR(TUnsupportedProtocolVersion);
  }

  Conf::TConf conf = Conf::TConf::TBuilder().Build(cfg->ConfigPath.c_str());
  TGlobalBatchConfig batch_config =
      TBatchConfigBuilder().BuildFromConf(conf.GetBatchConf());

  /* Load any compression libraries we need, according to the compression info
     from our config file.  This will throw if a library fails to load.  We
     want to fail early if there is a problem loading a library. */
  CompressionInit(conf.GetCompressionConf());

  /* The TBruceServer constructor will use the random number generator, so
     initialize it now. */
  struct timespec t;
  IfLt0(clock_gettime(CLOCK_MONOTONIC_RAW, &t));
  std::srand(static_cast<unsigned>(t.tv_sec ^ t.tv_nsec));

  return TServerConfig(std::move(cfg), std::move(conf),
                       std::move(batch_config), std::move(protocol),
                       pool_block_size);
}

void TBruceServer::HandleShutdownSignal(int /*signum*/) {
  std::lock_guard<std::mutex> lock(ServerListMutex);

  for (TBruceServer *server: ServerList) {
    assert(server);
    server->RequestShutdown();
  }
}

static inline size_t
ComputeBlockCount(size_t max_buffer_kb, size_t block_size) {
  return std::max<size_t>(1, (1024 * max_buffer_kb) / block_size);
}

TBruceServer::TBruceServer(TServerConfig &&config)
    : SigMask(TSet::Exclude, { SIGINT, SIGTERM }),
      Config(std::move(config.Config)),
      Conf(std::move(config.Conf)),
      KafkaProtocol(std::move(config.KafkaProtocol)),
      PoolBlockSize(config.PoolBlockSize),
      Started(false),
      Pool(PoolBlockSize,
           ComputeBlockCount(Config->MsgBufferMax, PoolBlockSize),
           Capped::TPool::TSync::Mutexed),
      AnomalyTracker(DiscardFileLogger, Config->DiscardReportInterval,
                     Config->DiscardReportBadMsgPrefixSize),
      StatusPort(0),
      DebugSetup(Config->DebugDir.c_str(), Config->MsgDebugTimeLimit,
                 Config->MsgDebugByteLimit),
      InputThreadFatalError(false),
      RouterThreadFatalError(false),
      Dispatcher(*Config, Conf.GetCompressionConf(), *KafkaProtocol,
          MsgStateTracker, AnomalyTracker, config.BatchConfig, DebugSetup),
      RouterThread(*Config, Conf, *KafkaProtocol, AnomalyTracker,
          MsgStateTracker, config.BatchConfig, DebugSetup, Dispatcher),
      InputThread(*Config, Pool, MsgStateTracker, AnomalyTracker,
          RouterThread.GetMsgChannel()),
      MetadataTimestamp(RouterThread.GetMetadataTimestamp()),
      ShutdownRequested(ATOMIC_FLAG_INIT) {
  config.BatchConfig.Clear();
  std::lock_guard<std::mutex> lock(ServerListMutex);
  ServerList.push_front(this);
  MyServerListItem = ServerList.begin();
}

TBruceServer::~TBruceServer() noexcept {
  assert(this);
  std::lock_guard<std::mutex> lock(ServerListMutex);
  assert(*MyServerListItem == this);
  ServerList.erase(MyServerListItem);
}

void TBruceServer::BindStatusSocket(bool bind_ephemeral) {
  assert(this);
  TAddress status_address(TAddress::IPv4Any,
                          bind_ephemeral ? 0 : Config->StatusPort);
  TmpStatusSocket = IfLt0(socket(status_address.GetFamily(), SOCK_STREAM, 0));
  int flag = true;
  IfLt0(setsockopt(TmpStatusSocket, SOL_SOCKET, SO_REUSEADDR, &flag,
                   sizeof(flag)));

  /* This will throw if the server is already running (unless we used an
     ephemeral port, which happens when test code runs us). */
  Bind(TmpStatusSocket, status_address);

  TAddress sock_name = GetSockName(TmpStatusSocket);
  StatusPort = sock_name.GetPort();
  assert(bind_ephemeral || (StatusPort == Config->StatusPort));
}

int TBruceServer::Run() {
  assert(this);

  /* Regardless of what happens, we must notify test code when we have either
     finished initialization or are shutting down (possibly due to a fatal
     exception). */
  class t_init_wait_notifier final {
    NO_COPY_SEMANTICS(t_init_wait_notifier);

    public:
    explicit t_init_wait_notifier(TEventSemaphore &init_wait_sem)
        : Notified(false),
        NotifySem(init_wait_sem) {
    }

    ~t_init_wait_notifier() noexcept {
      Notify();
    }

    void Notify() noexcept {
      if (!Notified) {
        try {
          NotifySem.Push();
          Notified = true;
        } catch (const std::system_error &x) {
          syslog(LOG_ERR, "Error notifying on server init finished: %s",
              x.what());
        }
      }
    }

    private:
    bool Notified;

    TEventSemaphore &NotifySem;
  } init_wait_notifier(InitWaitSem);  // t_init_wait_notifier

  if (Started) {
    throw std::logic_error("Multiple calls to Run() method not supported");
  }

  Started = true;

  /* The main thread handles all signals, and keeps them all blocked except in
     specific places where it is ready to handle them.  This simplifies signal
     handling.  It is important that we have all signals blocked when creating
     threads, since they inherit our signal mask, and we want them to block all
     signals. */
  BlockAllSignals();

  syslog(LOG_NOTICE, "Server started");

  /* This starts the input and router threads but doesn't wait for them to
     finish initialization. */
  StartMsgHandlingThreads();

  /* The destructor shuts down Bruce's web interface if we start it below.  We
     want this to happen _after_ the message handling threads have shut down.
   */
  TWebInterface web_interface(StatusPort, MsgStateTracker, AnomalyTracker,
      MetadataTimestamp, RouterThread.GetMetadataUpdateRequestSem(),
      DebugSetup);

  /* Wait for the input thread to finish initialization, but don't wait for the
     router thread since Kafka problems can delay its initialization
     indefinitely.  Even while the router thread is still starting, the input
     thread can receive datagrams from clients and queue them for routing.  The
     input thread must be fully functional as soon as possible, and always
     remain responsive so clients never block while sending messages.  If Kafka
     problems delay router thread initialization indefinitely, messages will be
     queued until we run out of buffer space and start logging discards. */
  if (MsgHandlingInitWait()) {
    /* Input thread initialization succeeded.  Start the Mongoose HTTP server,
       which provides Bruce's web interface.  It runs in separate threads. */
    web_interface.StartHttpServer();

    /* We can close this now, since Mongoose has the port claimed. */
    TmpStatusSocket.Reset();

    syslog(LOG_NOTICE,
        "Started web interface, waiting for signals and errors");

    init_wait_notifier.Notify();

    /* Wait for signals and fatal errors.  Return when it is time for the
       server to shut down.  On return, if InputThreadFatalError or
       RouterThreadFatalError is true, then a fatal error was detected in a
       message handling thread.  Otherwise, we received a shutdown signal and
       no fatal errors were detected. */
    HandleEvents();
  }

  Shutdown();
  return GotFatalError() ? EXIT_FAILURE : EXIT_SUCCESS;
}

void TBruceServer::RequestShutdown() {
  assert(this);

  if (!ShutdownRequested.test_and_set()) {
    GotShutdownSignal.Increment();
    ShutdownRequestSem.Push();
  }
}

void TBruceServer::BlockAllSignals() {
  assert(this);

  const Signal::TSet block_all(Signal::TSet::Full);
  pthread_sigmask(SIG_SETMASK, block_all.Get(), nullptr);
}

void TBruceServer::StartMsgHandlingThreads() {
  assert(this);

  if (!Config->DiscardLogPath.empty()) {
    /* We must do this before starting the input thread so all discards are
       tracked properly when discard file logging is enabled.  This starts a
       thread when discard file logging is enabled. */
    DiscardFileLogger.Init(Config->DiscardLogPath.c_str(),
        static_cast<uint64_t>(Config->DiscardLogMaxFileSize) * 1024,
        static_cast<uint64_t>(Config->DiscardLogMaxArchiveSize) * 1024,
        Config->DiscardLogBadMsgPrefixSize,
        Config->UseOldOutputFormat);
  }

  /* Start threads without waiting for them to finish initialization. */
  syslog(LOG_NOTICE, "Starting input thread");
  InputThread.Start();
  syslog(LOG_NOTICE, "Starting router thread");
  RouterThread.Start();
}

bool TBruceServer::MsgHandlingInitWait() {
  assert(this);
  std::array<struct pollfd, 4> events;
  struct pollfd &input_thread_init = events[0];
  struct pollfd &input_thread_error = events[1];
  struct pollfd &router_thread_error = events[2];
  struct pollfd &shutdown_request = events[3];
  input_thread_init.fd = InputThread.GetInitWaitFd();
  input_thread_error.fd = InputThread.GetShutdownWaitFd();
  router_thread_error.fd = RouterThread.GetShutdownWaitFd();
  shutdown_request.fd = ShutdownRequestSem.GetFd();

  for (auto &item : events) {
    item.events = POLLIN;
    item.revents = 0;
  }

  int ret = ppoll(&events[0], events.size(), nullptr, SigMask.Get());
  assert(ret);

  if ((ret < 0) && (errno != EINTR)) {
    IfLt0(ret);  // this will throw
  }

  if (input_thread_error.revents) {
    syslog(LOG_ERR, "Main thread detected input thread termination 1 on fatal "
        "error");
    InputThreadFatalError = true;
  }

  if (router_thread_error.revents) {
    syslog(LOG_ERR, "Main thread detected router thread termination 1 on "
        "fatal error");
    RouterThreadFatalError = true;
  }

  if (input_thread_error.revents || router_thread_error.revents) {
    return false;
  }

  if (shutdown_request.revents || (ret < 0)) {
    syslog(LOG_NOTICE, "Got shutdown signal while waiting for input thread "
        "initialization");
    return false;
  }

  syslog(LOG_NOTICE, "Main thread detected successful input thread "
      "initialization");
  assert(input_thread_init.revents);
  return true;
}

void TBruceServer::HandleEvents() {
  assert(this);

  /* This is for periodically verifying that we are getting queried for discard
     info. */
  TTimerFd discard_query_check_timer(
      1000 * (1 + Config->DiscardReportInterval));

  std::array<struct pollfd, 4> events;
  struct pollfd &discard_query_check = events[0];
  struct pollfd &input_thread_error = events[1];
  struct pollfd &router_thread_error = events[2];
  struct pollfd &shutdown_request = events[3];
  discard_query_check.fd = discard_query_check_timer.GetFd();
  discard_query_check.events = POLLIN;
  input_thread_error.fd = InputThread.GetShutdownWaitFd();
  input_thread_error.events = POLLIN;
  router_thread_error.fd = RouterThread.GetShutdownWaitFd();
  router_thread_error.events = POLLIN;
  shutdown_request.fd = ShutdownRequestSem.GetFd();
  shutdown_request.events = POLLIN;

  for (; ; ) {
    for (auto &item : events) {
      item.revents = 0;
    }

    int ret = ppoll(&events[0], events.size(), nullptr, SigMask.Get());
    assert(ret);

    if ((ret < 0) && (errno != EINTR)) {
      IfLt0(ret);  // this will throw
    }

    if (input_thread_error.revents) {
      syslog(LOG_ERR, "Main thread detected input thread termination 2 on "
          "fatal error");
      InputThreadFatalError = true;
    }

    if (router_thread_error.revents) {
      syslog(LOG_ERR, "Main thread detected router thread termination 2 on "
          "fatal error");
      RouterThreadFatalError = true;
    }

    if (input_thread_error.revents || router_thread_error.revents) {
      break;
    }

    if (discard_query_check_timer.GetFd().IsReadable()) {
      discard_query_check_timer.Pop();
      AnomalyTracker.CheckGetInfoRate();
    }

    if (shutdown_request.revents || (ret < 0)) {
      syslog(LOG_NOTICE, "Got shutdown signal while server running");
      break;
    }

    assert(discard_query_check.revents);
  }
}

void TBruceServer::Shutdown() {
  assert(this);

  if (InputThreadFatalError) {
    InputThread.Join();
    assert(!InputThread.ShutdownWasOk());
  } else {
    syslog(LOG_NOTICE, "Shutting down input thread");
    InputThread.RequestShutdown();
    InputThread.Join();
    bool input_thread_ok = InputThread.ShutdownWasOk();
    syslog(LOG_NOTICE, "Input thread terminated %s",
              input_thread_ok ? "normally" : "on error");

    if (!input_thread_ok) {
      InputThreadFatalError = true;
    }
  }

  if (RouterThreadFatalError) {
    RouterThread.Join();
    assert(!RouterThread.ShutdownWasOk());
  } else {
    syslog(LOG_NOTICE, "Shutting down router thread");
    RouterThread.RequestShutdown();
    RouterThread.Join();
    bool router_thread_ok = RouterThread.ShutdownWasOk();
    syslog(LOG_NOTICE, "Router thread terminated %s",
              router_thread_ok ? "normally" : "on error");

    if (!router_thread_ok) {
      RouterThreadFatalError = true;
    }
  }

  /* Let the DiscardFileLogger destructor disable discard file logging.  Then
     we know it gets disabled only after everything that may generate discards
     has been destroyed. */
}

std::mutex TBruceServer::ServerListMutex;

std::list<TBruceServer *> TBruceServer::ServerList;
