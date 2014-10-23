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
#include <signal/masker.h>
#include <signal/set.h>
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
    : Config(std::move(config.Config)),
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
      Dispatcher(*Config, Conf.GetCompressionConf(), *KafkaProtocol,
          MsgStateTracker, AnomalyTracker, config.BatchConfig, DebugSetup),
      RouterThread(*Config, Conf, *KafkaProtocol, AnomalyTracker,
          MsgStateTracker, config.BatchConfig, DebugSetup, Dispatcher),
      InputThread(*Config, Pool, MsgStateTracker, AnomalyTracker,
          RouterThread),
      MetadataTimestamp(RouterThread.GetMetadataTimestamp()),
      ShutdownRequested(ATOMIC_FLAG_INIT) {
  config.BatchConfig.Clear();
  std::lock_guard<std::mutex> lock(ServerListMutex);
  ServerList.push_front(this);
  MyServerListItem = ServerList.begin();
}

TBruceServer::~TBruceServer() noexcept {
  assert(this);

  if (InputThread.IsStarted()) {
    try {
      ShutDownInputThread();
    } catch (const std::exception &x) {
      syslog(LOG_ERR, "Server caught exception while shutting down input "
             "thread on fatal error: %s", x.what());
    } catch (...) {
      syslog(LOG_ERR, "Server caught unknown exception while shutting down "
             "input thread on fatal error");
    }
  }

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
     ephemeral port, which is what happens when test code runs us). */
  Bind(TmpStatusSocket, status_address);

  TAddress sock_name = GetSockName(TmpStatusSocket);
  StatusPort = sock_name.GetPort();
  assert(bind_ephemeral || (StatusPort == Config->StatusPort));
}

int TBruceServer::Run() {
  assert(this);

  if (Started) {
    throw std::logic_error("Multiple calls to Run() method not supported");
  }

  Started = true;
  syslog(LOG_NOTICE, "Server started");
  const Signal::TSet sigset(TSet::Exclude, { SIGINT, SIGTERM });
  Signal::TMasker masker(*sigset);

  if (!Config->DiscardLogPath.empty()) {
    DiscardFileLogger.Init(Config->DiscardLogPath.c_str(),
        static_cast<uint64_t>(Config->DiscardLogMaxFileSize) * 1024,
        static_cast<uint64_t>(Config->DiscardLogMaxArchiveSize) * 1024,
        Config->DiscardLogBadMsgPrefixSize,
        Config->UseOldOutputFormat);
  }

  syslog(LOG_NOTICE, "Starting input thread");

  try {
    if (!StartInputThread()) {
      /* Got shutdown signal during startup.  This is not an error. */
      return EXIT_SUCCESS;
    }
  } catch (const TInputThreadStartFailure &) {
    syslog(LOG_ERR, "Failure during input thread initialization");
    return EXIT_FAILURE;
  } catch (const TInputThreadShutdownFailure &) {
    syslog(LOG_ERR, "Failure during input thread shutdown");
    return EXIT_FAILURE;
  }

  syslog(LOG_NOTICE, "Started input thread, now starting web interface");

  /* Start the Mongoose HTTP server, which is used for status monitoring.  It
     runs in a separate thread. */
  TWebInterface web_interface(StatusPort, MsgStateTracker, AnomalyTracker,
      MetadataTimestamp, InputThread.GetMetadataUpdateRequestSem(),
      DebugSetup);
  web_interface.StartHttpServer();

  /* We can close this now, since Mongoose has the port claimed. */
  TmpStatusSocket.Reset();

  syslog(LOG_NOTICE, "Started web interface, waiting for shutdown signal");

  /* The server is running.  Now wait for a shutdown signal, or for the server
     to shut down on its own due to a fatal error. */
  if (!WaitForShutdownSignal()) {
    syslog(LOG_ERR, "Input thread terminated on fatal error");
    return EXIT_FAILURE;  // fatal server error
  }

  return RespondToShutdownSignal() ? EXIT_SUCCESS : EXIT_FAILURE;
}

void TBruceServer::RequestShutdown() {
  assert(this);

  if (!ShutdownRequested.test_and_set()) {
    GotShutdownSignal.Increment();
    ShutdownRequestSem.Push();
  }
}

bool TBruceServer::StartInputThread() {
  assert(this);

  /* shutdown_wait_event.fd will become readable if the server encounters a
     fatal error on startup. */
  std::array<struct pollfd, 3> events;
  struct pollfd &init_wait_event = events[0];
  struct pollfd &shutdown_wait_event = events[1];
  struct pollfd &shutdown_request_event = events[2];
  init_wait_event.fd = InputThread.GetInitWaitFd();
  init_wait_event.events = POLLIN;
  init_wait_event.revents = 0;
  shutdown_wait_event.fd = InputThread.GetShutdownWaitFd();
  shutdown_wait_event.events = POLLIN;
  shutdown_wait_event.revents = 0;
  shutdown_request_event.fd = ShutdownRequestSem.GetFd();
  shutdown_request_event.events = POLLIN;
  shutdown_request_event.revents = 0;

  {
    /* The input thread will inherit our signal mask.  We want it to block
       everything, so block everything while starting the thread. */
    const Signal::TSet block_all(Signal::TSet::Full);
    Signal::TMasker masker(*block_all);
    InputThread.Start();
  }

  /* We must be responsive to shutdown signals here. */
  int ret = poll(&events[0], events.size(), -1);
  assert(ret);

  if ((ret < 0) && (errno != EINTR)) {
    IfLt0(ret);  // this will throw
  }

  {
    /* We don't want to be interrupted by signals here. */
    const Signal::TSet block_all(Signal::TSet::Full);
    Signal::TMasker masker(*block_all);

    /* No matter what happens, make sure test code doesn't get stuck waiting
       for us. */
    ServerStartedSem.Push();

    if (ret > 0) {
      if (shutdown_wait_event.revents) {
        /* Server encountered fatal error on startup. */
        InputThread.Join();
        assert(InputThread.GetShutdownStatus() ==
               TInputThread::TShutdownStatus::Error);
        throw TInputThreadStartFailure();
      }

      if (shutdown_request_event.revents == 0) {
        assert(init_wait_event.revents);
        return true;  // input thread started
      }
    }

    if (!RespondToShutdownSignal()) {
      throw TInputThreadShutdownFailure();
    }
  }

  return false;  // got shutdown signal
}

bool TBruceServer::ShutDownInputThread() {
  assert(this);
  syslog(LOG_NOTICE, "Shutting down input thread");
  InputThread.RequestShutdown();
  InputThread.Join();
  bool input_thread_ok = (InputThread.GetShutdownStatus() ==
                          TInputThread::TShutdownStatus::Normal);
  syslog(LOG_NOTICE, "Server finished: input thread terminated %s",
            input_thread_ok ? "normally" : "on error");
  return input_thread_ok;
}

bool TBruceServer::RespondToShutdownSignal() {
  assert(this);
  syslog(LOG_NOTICE, "Got shutdown signal");
  bool ret = ShutDownInputThread();
  DiscardFileLogger.Shutdown();
  return ret;
}

void TBruceServer::BlockAllSignals() {
  assert(this);

  const Signal::TSet block_all(Signal::TSet::Full);
  pthread_sigmask(SIG_SETMASK, block_all.Get(), nullptr);
}

bool TBruceServer::WaitForShutdownSignal() {
  assert(this);

  /* This is for periodically verifying that we are getting queried for discard
     info. */
  Base::TTimerFd discard_query_check(
      1000 * (1 + Config->DiscardReportInterval));

  /* Wait for a shutdown signal, or for the server to shut down on its own due
     to a fatal error.  Also, periodically verify that we are getting queried
     for discard info frequently enough.  If not, then the monitoring script
     that is supposed to get discard info from bruce isn't doing its job. */
  Base::TFd shutdown_requested_fd = ShutdownRequestSem.GetFd();
  Base::TFd timer_fd = discard_query_check.GetFd();
  std::array<struct pollfd, 3> events;
  struct pollfd &shutdown_requested_event = events[0];
  struct pollfd &shutdown_wait_event = events[1];
  struct pollfd &discard_query_check_event = events[2];
  shutdown_requested_event.fd = shutdown_requested_fd;
  shutdown_requested_event.events = POLLIN;
  shutdown_wait_event.fd = InputThread.GetShutdownWaitFd();
  shutdown_wait_event.events = POLLIN;
  discard_query_check_event.fd = timer_fd;
  discard_query_check_event.events = POLLIN;

  for (; ; ) {
    for (auto &item : events) {
      item.revents = 0;
    }

    int ret = poll(&events[0], events.size(), -1);

    /* EINTR indicates we got interrupted by the shutdown signal.  Any other
       error is fatal. */
    if ((ret < 0) && (errno != EINTR)) {
      IfLt0(ret);  // this will throw
    }

    if (shutdown_wait_event.revents) {
      /* The server encountered a fatal error.  We no longer need to respond to
         signals, so block all signals until we exit.  Then we don't have to
         worry about getting interrupted. */
      BlockAllSignals();

      InputThread.Join();
      assert(InputThread.GetShutdownStatus() ==
             TInputThread::TShutdownStatus::Error);
      return false;
    }

    if (shutdown_requested_fd.IsReadable()) {
      /* We got a shutdown signal.  We no longer need to respond to signals, so
         block all signals until we exit.  Then we don't have to worry about
         getting interrupted. */
      BlockAllSignals();
      break;
    }

    if (timer_fd.IsReadable()) {
      discard_query_check.Pop();
      AnomalyTracker.CheckGetInfoRate();
    }
  }

  return true;
}

std::list<TBruceServer *> TBruceServer::ServerList;

std::mutex TBruceServer::ServerListMutex;
