/* <bruce/input_thread.test.cc>

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

   Unit test for <bruce/input_thread.h>
 */

#include <bruce/input_thread.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <base/field_access.h>
#include <base/tmp_file_name.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/client/bruce_client.h>
#include <bruce/client/bruce_client_socket.h>
#include <bruce/client/status_codes.h>
#include <bruce/config.h>
#include <bruce/debug/debug_setup.h>
#include <bruce/discard_file_logger.h>
#include <bruce/input_thread.h>
#include <bruce/kafka_proto/choose_proto.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/metadata_timestamp.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/test_util/misc_util.h>
#include <bruce/test_util/mock_router_thread.h>
#include <bruce/util/time_util.h>
#include <capped/blob.h>
#include <capped/pool.h>
#include <capped/reader.h>

#include <gtest/gtest.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Client;
using namespace Bruce::Debug;
using namespace Bruce::KafkaProto;
using namespace Bruce::TestUtil;
using namespace Bruce::Util;
using namespace Capped;

namespace {

  struct TBruceConfig {
    private:
    bool BruceStarted;

    public:
    TTmpFileName UnixSocketName;

    std::vector<const char *> Args;

    std::unique_ptr<TConfig> Cfg;

    std::unique_ptr<TWireProtocol> Protocol;

    TPool Pool;

    TDiscardFileLogger DiscardFileLogger;

    TAnomalyTracker AnomalyTracker;

    TMsgStateTracker MsgStateTracker;

    TMetadataTimestamp MetadataTimestamp;

    TDebugSetup DebugSetup;

    std::unique_ptr<TMockRouterThread> MockRouterThread;

    std::unique_ptr<TInputThread> InputThread;

    explicit TBruceConfig(size_t pool_block_size);

    ~TBruceConfig() noexcept {
      StopBruce();
    }

    void StartBruce() {
      if (!BruceStarted) {
        TInputThread &input_thread = *InputThread;
        input_thread.Start();
        input_thread.GetInitWaitFd().IsReadable(-1);
        BruceStarted = true;
      }
    }

    void StopBruce() {
      if (BruceStarted) {
        TInputThread &input_thread = *InputThread;
        input_thread.RequestShutdown();
        input_thread.Join();
        BruceStarted = false;
      }
    }
  };

  static inline size_t
  ComputeBlockCount(size_t max_buffer_kb, size_t block_size) {
    return std::max<size_t>(1, (1024 * max_buffer_kb) / block_size);
  }

  TBruceConfig::TBruceConfig(size_t pool_block_size)
      : BruceStarted(false),
        Pool(pool_block_size, ComputeBlockCount(1, pool_block_size),
             TPool::TSync::Mutexed),
        AnomalyTracker(DiscardFileLogger, 0,
                       std::numeric_limits<size_t>::max()),
        DebugSetup("/unused/path", TDebugSetup::MAX_LIMIT,
                   TDebugSetup::MAX_LIMIT) {
    Args.push_back("bruce");
    Args.push_back("--config_path");
    Args.push_back("/nonexistent/path");
    Args.push_back("--msg_buffer_max");
    Args.push_back("1");  /* this is 1 * 1024 bytes, not 1 byte */
    Args.push_back("--receive_socket_name");
    Args.push_back(UnixSocketName);
    Args.push_back(nullptr);
    Cfg.reset(new TConfig(Args.size() - 1, const_cast<char **>(&Args[0])));
    Protocol.reset(ChooseProto(Cfg->ProtocolVersion, Cfg->RequiredAcks,
                   static_cast<int32_t>(Cfg->ReplicationTimeout),
                   Cfg->RetryOnUnknownPartition));
    MockRouterThread.reset(new TMockRouterThread(*Cfg, *Protocol,
                           AnomalyTracker, MetadataTimestamp));
    InputThread.reset(new TInputThread(*Cfg, Pool, MsgStateTracker,
        AnomalyTracker, *MockRouterThread));
  }

  static void MakeDg(std::vector<uint8_t> &dg, const std::string &topic,
      const std::string &body) {
    size_t dg_size = 0;
    int ret = bruce_find_any_partition_msg_size(topic.size(), 0,
            body.size(), &dg_size);
    ASSERT_EQ(ret, BRUCE_OK);
    dg.resize(dg_size);
    ret = bruce_write_any_partition_msg(&dg[0], dg.size(), topic.c_str(),
            GetEpochMilliseconds(), nullptr, 0, body.data(), body.size());
    ASSERT_EQ(ret, BRUCE_OK);
  }

  /* The fixture for testing class TInputThread. */
  class TInputThreadTest : public ::testing::Test {
    protected:
    TInputThreadTest() {
    }

    virtual ~TInputThreadTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TInputThreadTest

  TEST_F(TInputThreadTest, SuccessfulForwarding) {
    /* If this value is set too large, message(s) will be discarded and the
       test will fail. */
    const size_t pool_block_size = 256;

    TBruceConfig conf(pool_block_size);
    TMockRouterThread &mock_router_thread = *conf.MockRouterThread;
    conf.StartBruce();
    TBruceClientSocket sock;
    int ret = sock.Bind(conf.UnixSocketName);
    ASSERT_EQ(ret, BRUCE_OK);
    std::vector<std::string> topics;
    std::vector<std::string> bodies;
    topics.push_back("topic1");
    bodies.push_back("Scooby");
    topics.push_back("topic2");
    bodies.push_back("Shaggy");
    topics.push_back("topic3");
    bodies.push_back("Velma");
    topics.push_back("topic4");
    bodies.push_back("Daphne");
    std::vector<uint8_t> dg_buf;

    for (size_t i = 0; i < topics.size(); ++i) {
      MakeDg(dg_buf, topics[i], bodies[i]);
      ret = sock.Send(&dg_buf[0], dg_buf.size());
      ASSERT_EQ(ret, BRUCE_OK);
    }

    TGate<TMsg::TPtr> &msg_channel = mock_router_thread.MsgChannel;
    std::list<TMsg::TPtr> msg_list;
    const Base::TFd &msg_available_fd = msg_channel.GetMsgAvailableFd();

    while (msg_list.size() < 4) {
      if (!msg_available_fd.IsReadable(30000)) {
        ASSERT_TRUE(false);
        break;
      }

      msg_list.splice(msg_list.end(), msg_channel.Get());
    }

    ASSERT_EQ(msg_list.size(), 4U);
    size_t i = 0;

    for (std::list<TMsg::TPtr>::iterator iter = msg_list.begin();
         iter != msg_list.end();
         ++i, ++iter) {
      TMsg::TPtr &msg_ptr = *iter;

      /* Prevent spurious assertion failure in msg dtor. */
      SetProcessed(msg_ptr);

      ASSERT_EQ(msg_ptr->GetTopic(), topics[i]);
      ASSERT_TRUE(ValueEquals(msg_ptr, bodies[i]));
    }

    TAnomalyTracker::TInfo bad_stuff;
    conf.AnomalyTracker.GetInfo(bad_stuff);
    ASSERT_EQ(bad_stuff.DiscardTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.DuplicateTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
    ASSERT_EQ(bad_stuff.MalformedMsgCount, 0U);
    ASSERT_EQ(bad_stuff.UnsupportedVersionMsgCount, 0U);

    msg_list.clear();
  }

  TEST_F(TInputThreadTest, NoBufferSpaceDiscard) {
    /* This setting must be chosen properly, since it determines how many
       messages will be discarded. */
    const size_t pool_block_size = 256;

    TBruceConfig conf(pool_block_size);
    TInputThread &input_thread = *conf.InputThread;
    TMockRouterThread &mock_router_thread = *conf.MockRouterThread;
    conf.StartBruce();
    TBruceClientSocket sock;
    int ret = sock.Bind(conf.UnixSocketName);
    ASSERT_EQ(ret, BRUCE_OK);
    std::vector<std::string> topics;
    std::vector<std::string> bodies;
    topics.push_back("topic1");
    bodies.push_back("Scooby");
    topics.push_back("topic2");
    bodies.push_back("Shaggy");
    topics.push_back("topic3");
    bodies.push_back("Velma");
    topics.push_back("topic4");
    bodies.push_back("Daphne");

    /* Fred gets discarded due to the buffer space cap. */
    topics.push_back("topic5");
    bodies.push_back("Fred");

    std::vector<uint8_t> dg_buf;

    for (size_t i = 0; i < topics.size(); ++i) {
      MakeDg(dg_buf, topics[i], bodies[i]);
      ret = sock.Send(&dg_buf[0], dg_buf.size());
      ASSERT_EQ(ret, BRUCE_OK);
    }

    TGate<TMsg::TPtr> &msg_channel = mock_router_thread.MsgChannel;
    std::list<TMsg::TPtr> msg_list;
    const Base::TFd &msg_available_fd = msg_channel.GetMsgAvailableFd();

    while (msg_list.size() < 4) {
      if (!msg_available_fd.IsReadable(30000)) {
        ASSERT_TRUE(false);
        break;
      }

      msg_list.splice(msg_list.end(), msg_channel.Get());
    }

    for (size_t i = 0;
         (input_thread.GetMsgReceivedCount() < 5) && (i < 3000);
         ++i) {
      SleepMilliseconds(10);
    }

    ASSERT_EQ(input_thread.GetMsgReceivedCount(), 5U);
    ASSERT_EQ(msg_list.size(), 4U);
    size_t i = 0;

    for (std::list<TMsg::TPtr>::iterator iter = msg_list.begin();
         iter != msg_list.end();
         ++i, ++iter) {
      TMsg::TPtr &msg_ptr = *iter;

      /* Prevent spurious assertion failure in msg dtor. */
      SetProcessed(msg_ptr);

      ASSERT_EQ(msg_ptr->GetTopic(), topics[i]);
      ASSERT_TRUE(ValueEquals(msg_ptr, bodies[i]));
    }

    TAnomalyTracker::TInfo bad_stuff;
    conf.AnomalyTracker.GetInfo(bad_stuff);
    ASSERT_EQ(bad_stuff.DiscardTopicMap.size(), 1U);
    ASSERT_EQ(bad_stuff.DuplicateTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
    ASSERT_EQ(bad_stuff.MalformedMsgCount, 0U);
    ASSERT_EQ(bad_stuff.UnsupportedVersionMsgCount, 0U);
    ASSERT_EQ(bad_stuff.DiscardTopicMap.begin()->first, topics[4]);
    const TAnomalyTracker::TTopicInfo &discard_info =
        bad_stuff.DiscardTopicMap.begin()->second;
    ASSERT_EQ(discard_info.Count, 1U);
    msg_list.clear();
  }

  TEST_F(TInputThreadTest, MalformedMessageDiscards) {
    /* If this value is set too large, message(s) will be discarded and the
       test will fail. */
    const size_t pool_block_size = 256;

    TBruceConfig conf(pool_block_size);
    TInputThread &input_thread = *conf.InputThread;
    TMockRouterThread &mock_router_thread = *conf.MockRouterThread;
    conf.StartBruce();

    /* This message will get discarded because it's malformed. */
    std::string topic("scooby_doo");
    std::string msg_body("I like scooby snacks");
    TBruceClientSocket sock;
    int ret = sock.Bind(conf.UnixSocketName);
    ASSERT_EQ(ret, BRUCE_OK);
    std::vector<uint8_t> dg_buf;
    MakeDg(dg_buf, topic, msg_body);

    /* Overwrite the size field with an incorrect value. */
    ASSERT_GE(dg_buf.size(), sizeof(int32_t));
    WriteInt32ToHeader(&dg_buf[0], dg_buf.size() - 1);

    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);
    TGate<TMsg::TPtr> &msg_channel = mock_router_thread.MsgChannel;

    for (size_t i = 0;
         (input_thread.GetMsgReceivedCount() < 1) && (i < 3000);
         ++i) {
      SleepMilliseconds(10);
    }

    ASSERT_EQ(input_thread.GetMsgReceivedCount(), 1U);
    std::list<TMsg::TPtr> msg_list(msg_channel.NonblockingGet());
    ASSERT_TRUE(msg_list.empty());
    TAnomalyTracker::TInfo bad_stuff;
    conf.AnomalyTracker.GetInfo(bad_stuff);
    ASSERT_EQ(bad_stuff.DiscardTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.DuplicateTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
    ASSERT_EQ(bad_stuff.MalformedMsgCount, 1U);
    ASSERT_TRUE(bad_stuff.BadTopics.empty());
    msg_list.clear();
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
