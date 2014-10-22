/* <bruce/mock_kafka_server/server.h>

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

   Mock Kafka server that receives messages from bruce daemon.
 */

#pragma once

#include <cassert>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

#include <netinet/in.h>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/thrower.h>
#include <bruce/mock_kafka_server/client_handler_factory_base.h>
#include <bruce/mock_kafka_server/cmd_handler.h>
#include <bruce/mock_kafka_server/config.h>
#include <bruce/mock_kafka_server/connect_handler.h>
#include <bruce/mock_kafka_server/port_map.h>
#include <bruce/mock_kafka_server/received_request_tracker.h>
#include <bruce/mock_kafka_server/setup.h>
#include <bruce/mock_kafka_server/single_client_handler_base.h>
#include <fiber/dispatcher.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TServer final {
      NO_COPY_SEMANTICS(TServer);

      public:
      DEFINE_ERROR(TUnsupportedProtocolVersion, std::runtime_error,
          "Only protocol version 0 (used by Kafka 0.8) is currently "
          "supported.");

      const TConfig &GetConfig() const {
        assert(this);
        return Ss.Config;
      }

      /* Parameter 'config' provides server configuration.  A true value for
         parameter 'use_ephemeral_ports' tells the server to bind() to
         ephemeral ports to avoid failures due to a port already being in use.
         When running in standalone executable mode, false is passed for
         'use_ephemeral_ports'.  When running as part of a unit test, true is
         passed for 'use_ephemeral_ports'. */
      TServer(const TConfig &config, bool use_ephemeral_ports,
          bool track_received_requests)
          : UseEphemeralPorts(use_ephemeral_ports),
            InitSucceeded(false),
            Ss(config, track_received_requests),
            CmdPort(0),
            PortMap(new TPortMap) {
      }

      int Init();

      int Run();

      in_port_t GetCmdPort() const {
        assert(this);
        return CmdPort;
      }

      in_port_t VirtualPortToPhys(in_port_t v_port) const {
        assert(this);
        return PortMap->VirtualPortToPhys(v_port);
      }

      in_port_t PhysicalPortToVirt(in_port_t p_port) const {
        assert(this);
        return PortMap->PhysicalPortToVirt(p_port);
      }

      /* Unit tests code calls this to get info on requests processed by the
         server. */
      void GetHandledRequests(
          std::list<TReceivedRequestTracker::TRequestInfo> &result) {
        assert(this);
        Ss.ReceivedRequests.GetRequestInfo(result);
      }

      /* Unit tests code calls this to get info on requests processed by the
         server.  Nonblocking version of GetHandledRequests(). */
      void NonblockingGetHandledRequests(
          std::list<TReceivedRequestTracker::TRequestInfo> &result) {
        assert(this);
        Ss.ReceivedRequests.NonblockingGetRequestInfo(result);
      }

      private:
      bool InitOutputDir();

      void ShutDownWorkers();

      bool InitCmdPort();

      void InitKafkaPorts();

      /* See big comment in <bruce/mock_kafka_server/port_map.h> for an
         explanation of this. */
      const bool UseEphemeralPorts;

      bool InitSucceeded;

      TSharedState Ss;

      std::unique_ptr<TClientHandlerFactoryBase> ClientHandlerFactory;

      /* Listening socket FD for receiving commands (used for error injection,
         etc.). */
      Base::TFd CmdListenFd;

      /* For Kafka producer clients. */
      std::vector<Base::TFd> ListenFdVec;

      std::unique_ptr<TCmdHandler> CmdHandler;

      /* There is 1 of these per listening FD. */
      std::vector<std::shared_ptr<TConnectHandler>> ConnectHandlers;

      /* This is the port the server listens on for error injection commands.
       */
      in_port_t CmdPort;

      /* Mappings between virtual and physical ports.  See big comment in
         <bruce/mock_kafka_server/port_map.h> for an explanation of what is
         going on here. */
      std::shared_ptr<TPortMap> PortMap;
    };  // TServer

  }  // MockKafkaServer

}  // Bruce
