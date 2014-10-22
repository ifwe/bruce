/* <bruce/mock_kafka_server/server.cc>

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

   Implements <bruce/mock_kafka_server/server.h>.
 */

#include <bruce/mock_kafka_server/server.h>

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

#include <syslog.h>

#include <base/debug_log.h>
#include <bruce/util/exceptions.h>
#include <bruce/util/worker_thread.h>
#include <socket/address.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MockKafkaServer;
using namespace Bruce::Util;
using namespace Fiber;
using namespace Socket;

int TServer::Init() {
  assert(this);

  if (InitSucceeded) {
    throw std::logic_error("Init() method already called");
  }

  if (!InitOutputDir()) {
    return EXIT_FAILURE;
  }

  try {
    TSetup().Get(Ss.Config.SetupFile, Ss.Setup);
  } catch (const TFileOpenError &x) {
    std::cerr << x.what() << std::endl;
    return EXIT_FAILURE;
  } catch (const TFileReadError &x) {
    std::cerr << x.what() << std::endl;
    return EXIT_FAILURE;
  }

  Ss.Dispatcher.reset(new TDispatcher);

  if (!InitCmdPort()) {
    return EXIT_FAILURE;
  }

  InitKafkaPorts();
  InitSucceeded = true;
  return EXIT_SUCCESS;
}

int TServer::Run() {
  assert(this);

  class t_shutdown final {
    NO_COPY_SEMANTICS(t_shutdown);

    public:
    explicit t_shutdown(TServer &server)
        : Server(server) {
    }

    ~t_shutdown() noexcept {
      Server.ShutDownWorkers();
    }

    private:
    TServer &Server;
  };  // t_shutdown

  if (!InitSucceeded && (Init() != EXIT_SUCCESS)) {
    return EXIT_FAILURE;
  }

  t_shutdown shutdown(*this);  // destructor calls ShutDownWorkers()
  Ss.Dispatcher->Run(std::chrono::milliseconds(1), { });  // shutdown on SIGINT
  ConnectHandlers.clear();
  ListenFdVec.clear();
  return EXIT_SUCCESS;
}

bool TServer::InitOutputDir() {
  if (Ss.Config.OutputDir.empty() || Ss.Config.OutputDir[0] != '/') {
    std::cerr << "Output directory must be an absolute pathname" << std::endl;
    return false;
  }

  std::string cmd("/bin/mkdir -p ");
  cmd += Ss.Config.OutputDir;
  int ret = std::system(cmd.c_str());

  if (ret) {
    std::cerr << "Failed to create output directory " << Ss.Config.OutputDir
        << std::endl;
    return false;
  }

  cmd = "/bin/rm -fr ";
  cmd += Ss.Config.OutputDir;
  cmd += "/server.out.*";
  ret = std::system(cmd.c_str());

  if (ret) {
    std::cerr << "Failed to remove old files from output directory "
        << Ss.Config.OutputDir << std::endl;
    return false;
  }

  return true;
}

void TServer::ShutDownWorkers() {
  assert(this);
  std::unordered_map<int, TSharedState::TPerConnectionState> &state_map =
      Ss.PerConnectionMap;

  for (auto item : state_map) {
    item.second.Worker->RequestShutdown();
  }

  for (auto item : state_map) {
    try {
      item.second.Worker->Join();
    } catch (const TWorkerThread::TThreadThrewStdException &x) {
      syslog(LOG_ERR, "%s", x.what());
    } catch (const TWorkerThread::TThreadThrewUnknownException &x) {
      syslog(LOG_ERR, "%s", x.what());
    }
  }

  state_map.clear();
}

bool TServer::InitCmdPort() {
  assert(this);
  in_port_t kafka_port_begin = Ss.Setup.BasePort;
  in_port_t kafka_port_end = kafka_port_begin + Ss.Setup.Ports.size();

  if ((Ss.Config.CmdPort >= kafka_port_begin) &&
      (Ss.Config.CmdPort < kafka_port_end)) {
    std::cerr << "Command port is in Kafka port range" << std::endl;
    return false;
  }

  CmdHandler.reset(new TCmdHandler(Ss));
  TAddress server_address(TAddress::IPv4Any,
                          UseEphemeralPorts ? 0 : Ss.Config.CmdPort);
  CmdListenFd = IfLt0(socket(server_address.GetFamily(), SOCK_STREAM, 0));
  int flag = true;
  IfLt0(setsockopt(CmdListenFd, SOL_SOCKET, SO_REUSEADDR, &flag,
                   sizeof(flag)));
  Bind(CmdListenFd, server_address);
  TAddress sock_name = GetSockName(CmdListenFd);
  CmdPort = sock_name.GetPort();
  assert(UseEphemeralPorts || (CmdPort == Ss.Config.CmdPort));
  CmdHandler->RegisterWithDispatcher(*Ss.Dispatcher, CmdListenFd,
                                     POLLIN | POLLERR);
  IfLt0(listen(CmdListenFd, 1024));
  return true;
}

void TServer::InitKafkaPorts() {
  assert(this);

  if (!ClientHandlerFactory) {
    ClientHandlerFactory.reset(
        TClientHandlerFactoryBase::CreateFactory(Ss.Config, Ss.Setup));

    if (!ClientHandlerFactory) {
      THROW_ERROR(TUnsupportedProtocolVersion);
    }
  }

  ListenFdVec.clear();
  ListenFdVec.resize(Ss.Setup.Ports.size());
  ConnectHandlers.clear();
  ConnectHandlers.resize(ListenFdVec.size());

  for (size_t port_offset = 0;
       port_offset < ConnectHandlers.size();
       ++port_offset) {
    ConnectHandlers[port_offset].reset(
        new TConnectHandler(Ss, *ClientHandlerFactory, port_offset, PortMap));
  }

  assert(ListenFdVec.size() == Ss.Setup.Ports.size());
  assert(ConnectHandlers.size() == Ss.Setup.Ports.size());

  for (size_t i = 0; i < Ss.Setup.Ports.size(); ++i) {
    /* See big comment in <bruce/mock_kafka_server/port_map.h> for an
       explanation of what is going on here. */
    in_port_t virtual_port = Ss.Setup.BasePort + i;
    TAddress server_address(TAddress::IPv4Any,
        UseEphemeralPorts ? static_cast<in_port_t>(0) : virtual_port);

    TFd &fd = ListenFdVec[i];
    fd = IfLt0(socket(server_address.GetFamily(), SOCK_STREAM, 0));
    int flag = true;
    IfLt0(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag)));
    Bind(fd, server_address);
    TAddress sock_name = GetSockName(fd);

    /* Get physical port and store mapping between virtual and physical ports.
     */
    in_port_t physical_port = sock_name.GetPort();
    assert(UseEphemeralPorts || (physical_port == virtual_port));
    PortMap->AddMapping(virtual_port, physical_port);

    ConnectHandlers[i]->RegisterWithDispatcher(*Ss.Dispatcher, fd,
        POLLIN | POLLERR);
  }

  for (TFd &fd : ListenFdVec) {
    IfLt0(listen(fd, 1024));
  }
}
