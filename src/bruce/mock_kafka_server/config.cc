/* <bruce/mock_kafka_server/config.cc>

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

   Implements <bruce/mock_kafka_server/config.h>.
 */

#include <bruce/mock_kafka_server/config.h>

#include <base/basename.h>
#include <bruce/build_id.h>
#include <bruce/util/arg_parse_error.h>
#include <tclap/CmdLine.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MockKafkaServer;
using namespace Bruce::Util;

static void ParseArgs(int argc, char *argv[], TConfig &config) {
  using namespace TCLAP;
  const std::string prog_name = Basename(argv[0]);
  std::vector<const char *> arg_vec(&argv[0], &argv[0] + argc);
  arg_vec[0] = prog_name.c_str();

  try {
    CmdLine cmd("Mock Kafka server for testing Bruce.", ' ', bruce_build_id);
    SwitchArg arg_log_echo("", "log_echo", "Echo syslog messages to standard "
        "error.", cmd, config.LogEcho);
    ValueArg<decltype(config.ProtocolVersion)> arg_protocol_version("",
        "protocol_version", "Version of Kafka protocol to use (currently only "
        "0 is supported).", false, config.ProtocolVersion, "VERSION");
    cmd.add(arg_protocol_version);
    ValueArg<decltype(config.QuietLevel)> arg_quiet_level("", "quiet_level",
        "Limit output verbosity.", false, config.QuietLevel, "LEVEL");
    cmd.add(arg_quiet_level);
    ValueArg<decltype(config.SetupFile)> arg_setup_file("", "setup_file",
        "Setup file.", true, config.SetupFile, "FILE");
    cmd.add(arg_setup_file);
    ValueArg<decltype(config.OutputDir)> arg_output_dir("", "output_dir",
        "Directory where server writes its output files.", true,
        config.OutputDir, "DIR");
    cmd.add(arg_output_dir);
    ValueArg<decltype(config.CmdPort)> arg_cmd_port("", "cmd_port", "Command "
        "port (for error injection, etc.).", false, config.CmdPort, "PORT");
    cmd.add(arg_cmd_port);
    SwitchArg arg_single_output_file("", "single_output_file", "Use single "
        "output file for all clients", cmd, config.SingleOutputFile);
    cmd.parse(argc, &arg_vec[0]);
    config.LogEcho = arg_log_echo.getValue();
    config.ProtocolVersion = arg_protocol_version.getValue();
    config.QuietLevel = arg_quiet_level.getValue();
    config.SetupFile = arg_setup_file.getValue();
    config.OutputDir = arg_output_dir.getValue();
    config.CmdPort = arg_cmd_port.getValue();
    config.SingleOutputFile = arg_single_output_file.getValue();
  } catch (const ArgException &x) {
    throw TArgParseError(x.error(), x.argId());
  }
}

TConfig::TConfig(int argc, char *argv[])
    : LogEcho(false),
      ProtocolVersion(0),
      QuietLevel(0),
      CmdPort(9080),
      SingleOutputFile(false) {
  ParseArgs(argc, argv, *this);
}
