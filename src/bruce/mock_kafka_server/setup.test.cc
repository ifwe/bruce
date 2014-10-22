/* <bruce/mock_kafka_server/setup.test.cc>

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

   Unit tests for <bruce/mock_kafka_server/setup.h>.
 */

#include <bruce/mock_kafka_server/setup.h>

#include <fstream>
#include <iostream>

#include <base/ofdstream.h>
#include <base/tmp_file.h>

#include <gtest/gtest.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MockKafkaServer;

namespace {

  // The fixture for testing class TSetup.
  class TSetupTest : public ::testing::Test {
    protected:
    TSetupTest() {
    }

    virtual ~TSetupTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TSetupTest

  TEST_F(TSetupTest, Test1) {
    TTmpFile tmp_file;
    tmp_file.SetDeleteOnDestroy(true);
    std::ofstream ofs(tmp_file.GetName());
    ofs << "   # this is a comment" << std::endl
        << std::endl
        << "# another comment" << std::endl
        << "ports 10000 3" << std::endl
        << "  # I like comments" << std::endl
        << "        " << std::endl
        << "topic foo 4 1" << std::endl
        << "#blah blah" << std::endl
        << "     " << std::endl;
    ofs.close();
    TSetup::TInfo info;
    std::string filename(tmp_file.GetName());
    bool threw = false;

    try {
      TSetup().Get(filename, info);
    } catch (const std::exception &x) {
      threw = true;
    }

    ASSERT_FALSE(threw);
    ASSERT_EQ(info.BasePort, 10000);
    ASSERT_EQ(info.Ports.size(), 3U);

    for (const TSetup::TPort &port : info.Ports) {
      ASSERT_EQ(port.ReadDelay, 0U);
      ASSERT_EQ(port.ReadDelayInterval, 1U);
      ASSERT_EQ(port.AckDelay, 0U);
      ASSERT_EQ(port.AckDelayInterval, 1U);
    }

    ASSERT_EQ(info.Topics.size(), 1U);
    auto iter = info.Topics.find("foo");
    ASSERT_TRUE(iter != info.Topics.end());
    const TSetup::TTopic &topic = iter->second;
    ASSERT_EQ(topic.Partitions.size(), 4U);
    ASSERT_EQ(topic.FirstPortOffset, 1U);

    for (const TSetup::TPartition &partition : topic.Partitions) {
      ASSERT_EQ(partition.AckError, 0);
      ASSERT_EQ(partition.AckErrorInterval, 1U);
    }
  }

  TEST_F(TSetupTest, Test2) {
    TTmpFile tmp_file;
    tmp_file.SetDeleteOnDestroy(true);
    std::ofstream ofs(tmp_file.GetName());
    ofs << "   # this is a comment" << std::endl
        << std::endl
        << "# another comment" << std::endl
        << "ports 10000 3" << std::endl
        << "port 10000 read_delay 5000:100 ack_delay 6000:50" << std::endl
        << "port 10002 read_delay 5001:101 ack_delay 6001:51" << std::endl
        << "  # I like comments" << std::endl
        << "        " << std::endl
        << "topic foo 4 1" << std::endl
        << "partition_error foo 1 5 100" << std::endl
        << "partition_error foo 3 6 101" << std::endl
        << "#blah blah" << std::endl
        << "     " << std::endl;
    ofs.close();
    TSetup::TInfo info;
    std::string filename(tmp_file.GetName());
    bool threw = false;

    try {
      TSetup().Get(filename, info);
    } catch (const std::exception &x) {
      threw = true;
    }

    ASSERT_FALSE(threw);
    ASSERT_EQ(info.BasePort, 10000);
    ASSERT_EQ(info.Ports.size(), 3U);

    ASSERT_EQ(info.Ports[0].ReadDelay, 5000U);
    ASSERT_EQ(info.Ports[0].ReadDelayInterval, 100U);
    ASSERT_EQ(info.Ports[0].AckDelay, 6000U);
    ASSERT_EQ(info.Ports[0].AckDelayInterval, 50U);
    ASSERT_EQ(info.Ports[1].ReadDelay, 0U);
    ASSERT_EQ(info.Ports[1].ReadDelayInterval, 1U);
    ASSERT_EQ(info.Ports[1].AckDelay, 0U);
    ASSERT_EQ(info.Ports[1].AckDelayInterval, 1U);
    ASSERT_EQ(info.Ports[2].ReadDelay, 5001U);
    ASSERT_EQ(info.Ports[2].ReadDelayInterval, 101U);
    ASSERT_EQ(info.Ports[2].AckDelay, 6001U);
    ASSERT_EQ(info.Ports[2].AckDelayInterval, 51U);

    ASSERT_EQ(info.Topics.size(), 1U);
    auto iter = info.Topics.find("foo");
    ASSERT_TRUE(iter != info.Topics.end());
    const TSetup::TTopic &topic = iter->second;
    ASSERT_EQ(topic.Partitions.size(), 4U);
    ASSERT_EQ(topic.FirstPortOffset, 1U);

    ASSERT_EQ(topic.Partitions[0].AckError, 0);
    ASSERT_EQ(topic.Partitions[0].AckErrorInterval, 1U);
    ASSERT_EQ(topic.Partitions[1].AckError, 5);
    ASSERT_EQ(topic.Partitions[1].AckErrorInterval, 100U);
    ASSERT_EQ(topic.Partitions[2].AckError, 0);
    ASSERT_EQ(topic.Partitions[2].AckErrorInterval, 1U);
    ASSERT_EQ(topic.Partitions[3].AckError, 6);
    ASSERT_EQ(topic.Partitions[3].AckErrorInterval, 101U);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
