/* <bruce/mock_kafka_server/config.h>

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

   Configuration options for mock Kafka server.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include <netinet/in.h>

namespace Bruce {

  namespace MockKafkaServer {

    struct TConfig {
      /* Throws TArgParseError on error parsing args. */
      TConfig(int argc, char *argv[]);

      bool LogEcho;

      size_t ProtocolVersion;

      size_t QuietLevel;

      std::string SetupFile;

      std::string OutputDir;

      in_port_t CmdPort;

      bool SingleOutputFile;
    };  // TConfig

  }  // MockKafkaServer

}  // Bruce
