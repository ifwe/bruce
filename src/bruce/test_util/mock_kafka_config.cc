/* <bruce/test_util/mock_kafka_config.cc>

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

   Implements <bruce/test_util/mock_kafka_config.h>.
 */

#include <bruce/test_util/mock_kafka_config.h>

#include <unistd.h>

#include <base/error_utils.h>

#include <gtest/gtest.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::TestUtil;

TMockKafkaConfig::TMockKafkaConfig(const std::vector<std::string> &config_file)
    : KafkaStarted(false),
      SetupFile("/tmp/bruce_tmp.XXXXXX", true),
      OutputDir("/tmp/bruce_tmp.XXXXXX", true) {
  for (const std::string &line : config_file) {
    IfLt0(write(SetupFile.GetFd(), line.data(), line.size()));
    IfLt0(write(SetupFile.GetFd(), "\n", 1));
  }

  Args.push_back("mock_kafka_server");
  Args.push_back("--log_echo");
  Args.push_back("--output_dir");
  Args.push_back(OutputDir.GetName());
  Args.push_back("--setup_file");
  Args.push_back(SetupFile.GetName());
  Args.push_back(nullptr);
  Cfg.reset(new Bruce::MockKafkaServer::TConfig(Args.size() - 1,
                                      const_cast<char **>(&Args[0])));
  MainThread.reset(new Bruce::MockKafkaServer::TMainThread(*Cfg));
}

void TMockKafkaConfig::StartKafka() {
  if (!KafkaStarted) {
    MainThread->Start();
    MainThread->GetInitWaitFd().IsReadable(-1);
    bool success =Inj.Connect("localhost", MainThread->GetCmdPort());
    ASSERT_TRUE(success);
    KafkaStarted = true;
  }
}

void TMockKafkaConfig::StopKafka() {
  if (KafkaStarted) {
    MainThread->RequestShutdown();
    MainThread->Join();
    KafkaStarted = false;
  }
}
