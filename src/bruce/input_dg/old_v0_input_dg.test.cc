/* <bruce/input_dg/old_v0_input_dg.test.cc>

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

   Unit test for <bruce/input_dg/old_v0_input_dg_writer.h>,
   <bruce/input_dg/old_v0_input_dg_reader.h>, and
   <bruce/input_dg/input_dg_util.h>.
 */

#include <bruce/input_dg/input_dg_util.h>
#include <bruce/input_dg/old_v0_input_dg_writer.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <bruce/anomaly_tracker.h>
#include <bruce/config.h>
#include <bruce/msg.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/test_util/misc_util.h>
#include <capped/pool.h>
#include <capped/reader.h>

#include <gtest/gtest.h>

using namespace Bruce;
using namespace Bruce::InputDg;
using namespace Bruce::TestUtil;
using namespace Capped;

namespace {

  struct TTestConfig {
    std::vector<const char *> Args;

    std::unique_ptr<Bruce::TConfig> Cfg;

    std::unique_ptr<TPool> Pool;

    TDiscardFileLogger DiscardFileLogger;

    TAnomalyTracker AnomalyTracker;

    TMsgStateTracker MsgStateTracker;

    TTestConfig();
  };  // TTestConfig

  TTestConfig::TTestConfig()
      : Pool(new TPool(128, 16384, TPool::TSync::Mutexed)),
        AnomalyTracker(DiscardFileLogger, 0,
                       std::numeric_limits<size_t>::max()) {
    Args.push_back("bruce");
    Args.push_back("--config_path");
    Args.push_back("/nonexistent/path");
    Args.push_back("--msg_buffer_max");
    Args.push_back("1");  // dummy value
    Args.push_back("--receive_socket_name");
    Args.push_back("dummy_value");
    Args.push_back(nullptr);
    Cfg.reset(new Bruce::TConfig(Args.size() - 1,
                                 const_cast<char **>(&Args[0])));
  }

  /* The fixture for testing reading/writing of old format input datagrams. */
  class TOldV0InputDgTest : public ::testing::Test {
    protected:
    TOldV0InputDgTest() {
    }

    virtual ~TOldV0InputDgTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TOldV0InputDgTest

  TEST_F(TOldV0InputDgTest, Test1) {
    TTestConfig cfg;
    int64_t timestamp = 8675309;
    std::string topic("dumb jokes");
    std::string body("Why did the chicken cross the road?  Because he got "
                     "bored writing unit tests.");
    std::vector<uint8_t> buf;
    size_t expected_dg_size = TOldV0InputDgWriter::ComputeDgSize(topic.size(),
        body.size());
    size_t dg_size = TOldV0InputDgWriter().WriteDg(buf, timestamp,
        topic.data(), topic.data() + topic.size(), body.data(),
        body.data() + body.size());
    ASSERT_EQ(dg_size, expected_dg_size);
    ASSERT_EQ(buf.size(), dg_size);

    TMsg::TPtr msg = BuildMsgFromDg(&buf[0], dg_size, *cfg.Cfg, *cfg.Pool,
        cfg.AnomalyTracker, cfg.MsgStateTracker);
    ASSERT_TRUE(!!msg);
    SetProcessed(msg);
    ASSERT_EQ(msg->GetTimestamp(), timestamp);
    ASSERT_EQ(msg->GetTopic(), topic);
    ASSERT_TRUE(ValueEquals(msg, body));
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
