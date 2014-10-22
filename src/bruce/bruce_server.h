/* <bruce/bruce_server.h>

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

   Bruce server implementation.
 */

#pragma once

#include <atomic>
#include <cassert>
#include <exception>
#include <list>
#include <mutex>
#include <stdexcept>

#include <netinet/in.h>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/timer_fd.h>
#include <base/thrower.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/batch/global_batch_config.h>
#include <bruce/conf/conf.h>
#include <bruce/config.h>
#include <bruce/debug/debug_setup.h>
#include <bruce/discard_file_logger.h>
#include <bruce/input_thread.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/metadata_timestamp.h>
#include <bruce/msg_dispatch/kafka_dispatcher.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/router_thread.h>
#include <capped/pool.h>

namespace Bruce {

  class TBruceServer final {
    NO_COPY_SEMANTICS(TBruceServer);

    public:
    DEFINE_ERROR(TUnsupportedProtocolVersion, std::runtime_error,
                 "Only protocol version 0 (used by Kafka 0.8) is currently "
                 "supported.  Kafka versions below 0.8 are not supported.");

    DEFINE_ERROR(TBadRequiredAcks, std::runtime_error,
                 "required_acks value must be -1 or > 0");

    DEFINE_ERROR(TBadReplicationTimeout, std::runtime_error,
                 "replication_timeout value out of range");

    DEFINE_ERROR(TBadDiscardReportInterval, std::runtime_error,
                 "discard_report_interval value must be positive");

    DEFINE_ERROR(TMaxInputMsgSizeTooSmall, std::runtime_error,
                 "max_input_msg_size is too small");

    DEFINE_ERROR(TMaxInputMsgSizeTooLarge, std::runtime_error,
                 "max_input_msg_size is too large");

    DEFINE_ERROR(TMustAllowLargeDatagrams, std::runtime_error, "You didn't "
                 "specify allow_large_unix_datagrams, and max_input_msg_size "
                 "is large enough that clients sending large datagrams will "
                 "need to increase SO_SNDBUF above the default value.  Either "
                 "decrease max_input_msg_size or specify "
                 "allow_large_unix_datagrams.");

    DEFINE_ERROR(TBadDebugDir, std::runtime_error,
                 "debug_dir must be an absolute path");

    DEFINE_ERROR(TBadDiscardLogMaxFileSize, std::runtime_error,
                 "discard_log_max_file_size must be at least twice the "
                 "maximum input message size.  To disable discard logfile "
                 "generation, leave discard_log_path unspecified.");

    class TServerConfig final {
      NO_COPY_SEMANTICS(TServerConfig);

      public:
      TServerConfig(TServerConfig &&) = default;

      TServerConfig &operator=(TServerConfig &&) = default;

      const TConfig &GetCmdLineConfig() const {
        assert(this);
        return *Config;
      }

      private:
      TServerConfig(std::unique_ptr<const TConfig> &&config,
          Conf::TConf &&conf, Batch::TGlobalBatchConfig &&batch_config,
          std::unique_ptr<const KafkaProto::TWireProtocol> &&proto,
          size_t pool_block_size)
          : Config(std::move(config)),
            Conf(std::move(conf)),
            BatchConfig(std::move(batch_config)),
            KafkaProtocol(std::move(proto)),
            PoolBlockSize(pool_block_size) {
      }

      std::unique_ptr<const TConfig> Config;

      Conf::TConf Conf;

      Batch::TGlobalBatchConfig BatchConfig;

      std::unique_ptr<const KafkaProto::TWireProtocol> KafkaProtocol;

      size_t PoolBlockSize;

      friend class TBruceServer;
    };  // TServerConfig

    static TServerConfig CreateConfig(int argc, char **argv,
        bool &large_sendbuf_required, size_t pool_block_size = 128);

    static void HandleShutdownSignal(int signum);

    explicit TBruceServer(TServerConfig &&config);

    ~TBruceServer() noexcept;

    /* Must not be called until _after_ InitConfig() has been called. */
    size_t GetPoolBlockSize() const {
      assert(this);
      return PoolBlockSize;
    }

    const TConfig &GetConfig() const {
      assert(this);
      return *Config;
    }

    /* Used for testing. */
    const TAnomalyTracker &GetAnomalyTracker() const {
      assert(this);
      return AnomalyTracker;
    }

    /* Test code passes true for 'bind_ephemeral'. */
    void BindStatusSocket(bool bind_ephemeral = false);

    in_port_t GetStatusPort() const {
      assert(this);
      return StatusPort;
    }

    /* Return a file descriptor that becomes readable when the UNIX domain
       input socket has been created or the server is shutting down.  Test code
       calls this. */
    const Base::TFd &GetInitWaitFd() const {
      assert(this);
      return ServerStartedSem.GetFd();
    }

    /* This is called by test code. */
    size_t GetAckCount() const {
      assert(this);
      return Dispatcher.GetAckCount();
    }

    int Run();

    /* Called by SIGINT/SIGTERM handler.  Also called by test code to shut down
       bruce. */
    void RequestShutdown();

    private:
    /* Thrown by StartInputThread() on input thread initialization error. */
    class TInputThreadStartFailure final : public std::exception {
      public:
      virtual ~TInputThreadStartFailure() noexcept { }

      virtual const char *what() const noexcept {
        return "TInputThreadStartFailure";
      }
    };  // TInputThreadStartFailure

    /* Thrown by StartInputThread() on input thread shutdown error. */
    class TInputThreadShutdownFailure final : public std::exception {
      public:
      virtual ~TInputThreadShutdownFailure() noexcept { }

      virtual const char *what() const noexcept {
        return "TInputThreadShutdownFailure";
      }
    };  // TInputThreadShutdownFailure

    bool StartInputThread();

    bool ShutDownInputThread();

    bool RespondToShutdownSignal();

    /* Block all signals and keep them blocked (no RAII behavior that would
       unblock them when some object gets destroyed). */
    void BlockAllSignals();

    bool WaitForShutdownSignal();

    /* Global list of all TBruceServer objects. */
    static std::list<TBruceServer *> ServerList;

    /* Protects 'ServerList'. */
    static std::mutex ServerListMutex;

    /* Points to item in 'ServerList' for this TBruceServer object. */
    std::list<TBruceServer *>::iterator MyServerListItem;

    /* Configuration obtained from command line arguments. */
    const std::unique_ptr<const TConfig> Config;

    /* Configuration obtained from config file. */
    Conf::TConf Conf;

    const std::unique_ptr<const KafkaProto::TWireProtocol> KafkaProtocol;

    const size_t PoolBlockSize;

    bool Started;

    Capped::TPool Pool;

    TDiscardFileLogger DiscardFileLogger;

    TMsgStateTracker MsgStateTracker;

    /* For tracking discarded messages and possible duplicates. */
    TAnomalyTracker AnomalyTracker;

    /* The only purpose of this is to prevent multiple instances of the server
       from running simultaneously.  In this case, we want to fail as early as
       possible.  Once Mongoose has started, it has the port claimed so we
       close this socket. */
    Base::TFd TmpStatusSocket;

    in_port_t StatusPort;

    Debug::TDebugSetup DebugSetup;

    MsgDispatch::TKafkaDispatcher Dispatcher;

    TRouterThread RouterThread;

    TInputThread InputThread;

    const TMetadataTimestamp &MetadataTimestamp;

    /* Set when we get a shutdown signal or test code calls RequestShutdown().
       Here we use atomic flag because test process might get signal while
       calling RequestShutdown().
     */
    std::atomic_flag ShutdownRequested;

    /* Becomes readable when the UNIX domain input socket has been created.
       Test code monitors this. */
    Base::TEventSemaphore ServerStartedSem;

    /* SIGINT/SIGTERM handler pushes this. */
    Base::TEventSemaphore ShutdownRequestSem;
  };  // TBruceServer

}  // Bruce
