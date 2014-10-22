/* <bruce/batch/per_topic_batcher.test.cc>

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

   Unit test for <bruce/batch/per_topic_batcher.h>
 */

#include <bruce/batch/per_topic_batcher.h>

#include <algorithm>
#include <memory>
#include <string>

#include <bruce/batch/batch_config.h>
#include <bruce/batch/batch_config_builder.h>
#include <bruce/msg.h>
#include <bruce/msg_creator.h>
#include <bruce/test_util/misc_util.h>
#include <capped/blob.h>
#include <capped/pool.h>
#include <capped/reader.h>

#include <gtest/gtest.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;
using namespace Bruce::TestUtil;
using namespace Capped;

namespace {

  std::shared_ptr<TPerTopicBatcher::TConfig> MakeDisabledTopicBatchConfig() {
    TBatchConfigBuilder builder;
    TBatchConfig config;
    builder.SetDefaultTopic(&config);
    return std::move(builder.Build().GetPerTopicConfig());
  }

  std::shared_ptr<TPerTopicBatcher::TConfig> MakeTopicBatchConfig() {
    TBatchConfigBuilder builder;
    TBatchConfig config;
    config.TimeLimit = 10;
    config.MsgCount = 3;
    config.ByteCount = 0;
    builder.AddTopic("t1", &config);
    config.TimeLimit = 20;
    config.MsgCount = 4;
    config.ByteCount = 0;
    builder.AddTopic("t2", &config);
    config.TimeLimit = 30;
    config.MsgCount = 5;
    config.ByteCount = 0;
    builder.AddTopic("t3", &config);
    config.TimeLimit = 40;
    config.MsgCount = 3;
    config.ByteCount = 0;
    builder.SetDefaultTopic(&config);
    return std::move(builder.Build().GetPerTopicConfig());
  }

  /* The fixture for testing class TPerTopicBatcher. */
  class TPerTopicBatcherTest : public ::testing::Test {
    protected:
    TPerTopicBatcherTest() {
    }

    virtual ~TPerTopicBatcherTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TPerTopicBatcherTest

  TEST_F(TPerTopicBatcherTest, Test1) {
    TTestMsgCreator mc;  // create this first since it contains buffer pool
    TPerTopicBatcher batcher(MakeDisabledTopicBatchConfig());
    TMsg::TPtr msg = mc.NewMsg("topic", "message body", 5);
    std::list<std::list<TMsg::TPtr>> complete_batches =
        batcher.AddMsg(std::move(msg), 5);
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_TRUE(!!msg);
    SetProcessed(msg);
    ASSERT_TRUE(complete_batches.empty());
  }

  TEST_F(TPerTopicBatcherTest, Test2) {
    TTestMsgCreator mc;  // create this first since it contains buffer pool
    TPerTopicBatcher batcher(MakeTopicBatchConfig());
    TOpt<TMsg::TTimestamp> opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    TMsg::TPtr msg = mc.NewMsg("t1", "t1 msg 1", 5);
    std::list<std::list<TMsg::TPtr>> complete_batches =
        SetProcessed(batcher.AddMsg(std::move(msg), 5));
    ASSERT_TRUE(batcher.SanityCheck());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 15);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    msg = mc.NewMsg("t1", "t1 msg 2", 5);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 6));
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 15);
    msg = mc.NewMsg("t2", "t2 msg 1", 7);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 14));
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 15);
    msg = mc.NewMsg("t3", "t3 msg 1", 10);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 34));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_EQ(complete_batches.size(), 2);
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 40);
    bool got_t1 = false;
    bool got_t2 = false;

    for (std::list<TMsg::TPtr> &msg_list : complete_batches) {
      ASSERT_FALSE(msg_list.empty());
      std::string topic = msg_list.front()->GetTopic();

      if (topic == "t1") {
        ASSERT_EQ(msg_list.size(), 2);
        ASSERT_TRUE(ValueEquals(msg_list.front(), "t1 msg 1"));
        msg_list.pop_front();
        ASSERT_TRUE(ValueEquals(msg_list.front(), "t1 msg 2"));
        got_t1 = true;
      } else if (topic == "t2") {
        ASSERT_EQ(msg_list.size(), 1);
        ASSERT_TRUE(ValueEquals(msg_list.front(), "t2 msg 1"));
        got_t2 = true;
      } else {
        ASSERT_TRUE(false);
      }
    }

    ASSERT_TRUE(got_t1);
    ASSERT_TRUE(got_t2);
    complete_batches = SetProcessed(batcher.GetCompleteBatches(39));
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_TRUE(complete_batches.empty());
    msg = mc.NewMsg("t4", "t4 msg 1", 40);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 39));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_TRUE(complete_batches.empty());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    msg = mc.NewMsg("t4", "t4 msg 2", 45);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 39));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_TRUE(complete_batches.empty());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 40);
    complete_batches = SetProcessed(batcher.GetCompleteBatches(40));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_EQ(complete_batches.size(), 1U);
    ASSERT_EQ(complete_batches.front().size(), 1U);
    ASSERT_EQ(complete_batches.front().front()->GetTopic(), "t3");
    ASSERT_TRUE(ValueEquals(complete_batches.front().front(), "t3 msg 1"));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 80);
    msg = mc.NewMsg("t1", "t1 msg 3", 45);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 50));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_TRUE(complete_batches.empty());
    msg = mc.NewMsg("t1", "t1 msg 4", 54);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 54));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_TRUE(complete_batches.empty());
    std::list<TMsg::TPtr> batch = SetProcessed(batcher.DeleteTopic("t1"));
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_EQ(batch.size(), 2U);
    ASSERT_TRUE(ValueEquals(batch.front(), "t1 msg 3"));
    batch.pop_front();
    ASSERT_TRUE(ValueEquals(batch.front(), "t1 msg 4"));
    msg = mc.NewMsg("t5", "t5 msg 1", 54);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 54));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_TRUE(complete_batches.empty());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 80);
    complete_batches = SetProcessed(batcher.GetCompleteBatches(79));
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_TRUE(complete_batches.empty());
    complete_batches = SetProcessed(batcher.GetCompleteBatches(94));
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_EQ(complete_batches.size(), 2U);
    bool got_t4 = false;
    bool got_t5 = false;

    for (std::list<TMsg::TPtr> &msg_list : complete_batches) {
      ASSERT_FALSE(msg_list.empty());
      std::string topic = msg_list.front()->GetTopic();

      if (topic == "t4") {
        ASSERT_EQ(msg_list.size(), 2);
        ASSERT_TRUE(ValueEquals(msg_list.front(), "t4 msg 1"));
        msg_list.pop_front();
        ASSERT_TRUE(ValueEquals(msg_list.front(), "t4 msg 2"));
        got_t4 = true;
      } else if (topic == "t5") {
        ASSERT_EQ(msg_list.size(), 1);
        ASSERT_TRUE(ValueEquals(msg_list.front(), "t5 msg 1"));
        got_t5 = true;
      } else {
        ASSERT_TRUE(false);
      }
    }

    ASSERT_TRUE(got_t4);
    ASSERT_TRUE(got_t5);
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    msg = mc.NewMsg("t1", "t1 msg 5", 100);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 110));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_EQ(complete_batches.size(), 1U);
    ASSERT_EQ(complete_batches.front().size(), 1U);
    ASSERT_EQ(complete_batches.front().front()->GetTopic(), "t1");
    ASSERT_TRUE(ValueEquals(complete_batches.front().front(), "t1 msg 5"));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
  }

  TEST_F(TPerTopicBatcherTest, Test3) {
    TTestMsgCreator mc;  // create this first since it contains buffer pool
    TPerTopicBatcher batcher(MakeTopicBatchConfig());
    TOpt<TMsg::TTimestamp> opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    TMsg::TPtr msg = mc.NewMsg("t1", "t1 msg 1", 5);
    std::list<std::list<TMsg::TPtr>> complete_batches =
        SetProcessed(batcher.AddMsg(std::move(msg), 5));
    ASSERT_TRUE(batcher.SanityCheck());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 15);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    msg = mc.NewMsg("t1", "t1 msg 2", 5);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 6));
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 15);
    msg = mc.NewMsg("t2", "t2 msg 1", 7);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 14));
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 15);
    std::list<std::list<TMsg::TPtr>> all_batches =
        SetProcessed(batcher.GetAllBatches());
    ASSERT_TRUE(batcher.SanityCheck());
    ASSERT_EQ(all_batches.size(), 2);
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    bool got_t1 = false;
    bool got_t2 = false;

    for (std::list<TMsg::TPtr> &msg_list : all_batches) {
      ASSERT_FALSE(msg_list.empty());
      std::string topic = msg_list.front()->GetTopic();

      if (topic == "t1") {
        ASSERT_EQ(msg_list.size(), 2);
        ASSERT_TRUE(ValueEquals(msg_list.front(), "t1 msg 1"));
        msg_list.pop_front();
        ASSERT_TRUE(ValueEquals(msg_list.front(), "t1 msg 2"));
        got_t1 = true;
      } else if (topic == "t2") {
        ASSERT_EQ(msg_list.size(), 1);
        ASSERT_TRUE(ValueEquals(msg_list.front(), "t2 msg 1"));
        got_t2 = true;
      } else {
        ASSERT_TRUE(false);
      }
    }

    ASSERT_TRUE(got_t1);
    ASSERT_TRUE(got_t2);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
