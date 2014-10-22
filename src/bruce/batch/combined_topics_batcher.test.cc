/* <bruce/batch/combined_topics_batcher.test.cc>

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

   Unit test for <bruce/batch/combined_topics_batcher.h>
 */

#include <bruce/batch/combined_topics_batcher.h>

#include <algorithm>
#include <memory>
#include <string>

#include <bruce/batch/batch_config.h>
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

  /* The fixture for testing class TCombinedTopicsBatcher. */
  class TCombinedTopicsBatcherTest : public ::testing::Test {
    protected:
    TCombinedTopicsBatcherTest() {
    }

    virtual ~TCombinedTopicsBatcherTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TCombinedTopicsBatcherTest

  TEST_F(TCombinedTopicsBatcherTest, Test1) {
    TTestMsgCreator mc;  // create this first since it contains buffer pool
    TCombinedTopicsBatcher::TConfig config;
    TCombinedTopicsBatcher batcher(config);
    ASSERT_FALSE(batcher.BatchingIsEnabled());
    ASSERT_TRUE(batcher.IsEmpty());
    TMsg::TPtr msg = mc.NewMsg("topic", "message body", 5);
    std::list<std::list<TMsg::TPtr>> complete_batches =
        batcher.AddMsg(std::move(msg), 5);
    ASSERT_TRUE(!!msg);
    SetProcessed(msg);
    ASSERT_TRUE(complete_batches.empty());
    ASSERT_TRUE(batcher.IsEmpty());
  }

  TEST_F(TCombinedTopicsBatcherTest, Test2) {
    TTestMsgCreator mc;  // create this first since it contains buffer pool
    std::shared_ptr<std::unordered_set<std::string>>
        filter(new std::unordered_set<std::string>);
    TCombinedTopicsBatcher::TConfig
        config(TBatchConfig(20, 3, 25), filter, true);
    TCombinedTopicsBatcher batcher(config);
    ASSERT_TRUE(batcher.BatchingIsEnabled());
    ASSERT_TRUE(batcher.IsEmpty());
    TOpt<TMsg::TTimestamp> opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    TMsg::TPtr msg = mc.NewMsg("t1", "t1 msg 1", 5);
    std::list<std::list<TMsg::TPtr>> complete_batches =
        SetProcessed(batcher.AddMsg(std::move(msg), 5));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 25);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("t1", "t1 msg 2", 6);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 7));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 25);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("t2", "t2 msg 1", 8);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 8));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    ASSERT_FALSE(!!msg);
    ASSERT_EQ(complete_batches.size(), 2U);
    ASSERT_TRUE(batcher.IsEmpty());
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
    msg = mc.NewMsg("t1", "123456789012345678901234", 10);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 29));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 30);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("t1", "x", 29);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 29));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(batcher.IsEmpty());
    ASSERT_EQ(complete_batches.size(), 1U);
    ASSERT_EQ(complete_batches.front().size(), 2U);
    ASSERT_EQ(complete_batches.front().front()->GetTopic(), "t1");
    ASSERT_TRUE(ValueEquals(complete_batches.front().front(),
                           "123456789012345678901234"));
    complete_batches.front().pop_front();
    ASSERT_EQ(complete_batches.front().front()->GetTopic(), "t1");
    ASSERT_TRUE(ValueEquals(complete_batches.front().front(), "x"));
    msg = mc.NewMsg("t1", "t1 msg 3", 40);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 45));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 60);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("t1", "t1 msg 4", 50);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 60));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    ASSERT_FALSE(!!msg);
    ASSERT_EQ(complete_batches.size(), 1U);
    ASSERT_TRUE(batcher.IsEmpty());
    ASSERT_EQ(complete_batches.front().size(), 2U);
    ASSERT_EQ(complete_batches.front().front()->GetTopic(), "t1");
    ASSERT_TRUE(ValueEquals(complete_batches.front().front(), "t1 msg 3"));
    complete_batches.front().pop_front();
    ASSERT_EQ(complete_batches.front().front()->GetTopic(), "t1");
    ASSERT_TRUE(ValueEquals(complete_batches.front().front(), "t1 msg 4"));
    msg = mc.NewMsg("t1", "t1 msg 5", 70);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 70));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 90);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("t2", "t2 msg 2", 75);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 75));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 90);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    std::list<std::list<TMsg::TPtr>> batch_list =
        SetProcessed(batcher.TakeBatch());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    ASSERT_EQ(batch_list.size(), 2U);
    ASSERT_TRUE(batcher.IsEmpty());
    ASSERT_TRUE(batcher.BatchingIsEnabled());

    std::list<TMsg::TPtr> batch_1 = std::move(batch_list.front());
    batch_list.pop_front();
    std::list<TMsg::TPtr> batch_2 = std::move(batch_list.front());
    ASSERT_EQ(batch_1.size(), 1U);
    ASSERT_EQ(batch_2.size(), 1U);

    if (batch_1.front()->GetTopic() == "t2") {
      batch_1.swap(batch_2);
    }

    ASSERT_EQ(batch_1.front()->GetTopic(), "t1");
    ASSERT_TRUE(ValueEquals(batch_1.front(), "t1 msg 5"));
    ASSERT_EQ(batch_2.front()->GetTopic(), "t2");
    ASSERT_TRUE(ValueEquals(batch_2.front(), "t2 msg 2"));

    msg = mc.NewMsg("t1", "t1 msg 6", 70);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 70));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 90);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("t2", "t2 msg 3", 75);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 75));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_TRUE(opt_nct.IsKnown());
    ASSERT_EQ(*opt_nct, 90);
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(complete_batches.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    batch_list = SetProcessed(batcher.TakeBatch());
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    ASSERT_EQ(batch_list.size(), 2U);
    ASSERT_TRUE(batcher.IsEmpty());
    ASSERT_TRUE(batcher.BatchingIsEnabled());

    batch_1 = std::move(batch_list.front());
    batch_list.pop_front();
    batch_2 = std::move(batch_list.front());
    ASSERT_EQ(batch_1.size(), 1U);
    ASSERT_EQ(batch_2.size(), 1U);

    if (batch_1.front()->GetTopic() == "t2") {
      batch_1.swap(batch_2);
    }

    ASSERT_EQ(batch_1.front()->GetTopic(), "t1");
    ASSERT_TRUE(ValueEquals(batch_1.front(), "t1 msg 6"));
    ASSERT_EQ(batch_2.front()->GetTopic(), "t2");
    ASSERT_TRUE(ValueEquals(batch_2.front(), "t2 msg 3"));

    msg = mc.NewMsg("t2", "t2 msg 4", 75);
    complete_batches = SetProcessed(batcher.AddMsg(std::move(msg), 95));
    opt_nct = batcher.GetNextCompleteTime();
    ASSERT_FALSE(opt_nct.IsKnown());
    ASSERT_FALSE(!!msg);
    ASSERT_EQ(complete_batches.size(), 1U);
    ASSERT_TRUE(batcher.IsEmpty());
    ASSERT_EQ(complete_batches.front().size(), 1U);
    ASSERT_EQ(complete_batches.front().front()->GetTopic(), "t2");
    ASSERT_TRUE(ValueEquals(complete_batches.front().front(), "t2 msg 4"));
  }

  TEST_F(TCombinedTopicsBatcherTest, Test3) {
    TTestMsgCreator mc;  // create this first since it contains buffer pool
    std::shared_ptr<std::unordered_set<std::string>>
        filter(new std::unordered_set<std::string>);
    TCombinedTopicsBatcher::TConfig
        config(TBatchConfig(10, 3, 8), filter, true);
    TCombinedTopicsBatcher batcher(config);
    ASSERT_TRUE(batcher.BatchingIsEnabled());
    ASSERT_TRUE(batcher.IsEmpty());
    TMsg::TPtr msg = mc.NewMsg("Bugs Bunny", "wabbits", 0);
    std::list<std::list<TMsg::TPtr>> msg_list =
        SetProcessed(batcher.AddMsg(std::move(msg), 0));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(msg_list.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("Bugs Bunny", "x", 0);
    msg_list = SetProcessed(batcher.AddMsg(std::move(msg), 0));
    ASSERT_FALSE(!!msg);
    ASSERT_EQ(msg_list.size(), 1U);
    ASSERT_EQ(msg_list.front().size(), 2U);
    ASSERT_TRUE(ValueEquals(msg_list.front().front(), "wabbits"));
    msg_list.front().pop_front();
    ASSERT_TRUE(ValueEquals(msg_list.front().front(), "x"));
    ASSERT_TRUE(batcher.IsEmpty());
    msg = mc.NewMsg("Bugs Bunny", "wabbits", 0);
    msg_list = SetProcessed(batcher.AddMsg(std::move(msg), 0));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(msg_list.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("Bugs Bunny", "xx", 0);
    msg_list = SetProcessed(batcher.AddMsg(std::move(msg), 0));
    ASSERT_FALSE(!!msg);
    ASSERT_EQ(msg_list.size(), 1U);
    ASSERT_EQ(msg_list.front().size(), 1U);
    ASSERT_TRUE(ValueEquals(msg_list.front().front(), "wabbits"));
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("Bugs Bunny", "y", 0);
    msg_list = SetProcessed(batcher.AddMsg(std::move(msg), 0));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(msg_list.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("Bugs Bunny", "wabbits", 0);
    msg_list = SetProcessed(batcher.AddMsg(std::move(msg), 10));
    ASSERT_TRUE(!!msg);
    SetProcessed(msg);
    ASSERT_EQ(msg_list.size(), 1U);
    ASSERT_EQ(msg_list.front().size(), 2U);
    ASSERT_TRUE(ValueEquals(msg_list.front().front(), "xx"));
    msg_list.front().pop_front();
    ASSERT_TRUE(ValueEquals(msg_list.front().front(), "y"));
    ASSERT_TRUE(batcher.IsEmpty());
    msg = mc.NewMsg("Bugs Bunny", "wabbits", 0);
    msg_list = SetProcessed(batcher.AddMsg(std::move(msg), 0));
    ASSERT_FALSE(!!msg);
    ASSERT_TRUE(msg_list.empty());
    ASSERT_FALSE(batcher.IsEmpty());
    msg = mc.NewMsg("Bugs Bunny", "12345678", 0);
    msg_list = SetProcessed(batcher.AddMsg(std::move(msg), 0));
    ASSERT_TRUE(!!msg);
    SetProcessed(msg);
    ASSERT_TRUE(batcher.IsEmpty());
    ASSERT_EQ(msg_list.size(), 1U);
    ASSERT_EQ(msg_list.front().size(), 1U);
    ASSERT_TRUE(ValueEquals(msg_list.front().front(), "wabbits"));
    msg_list = SetProcessed(batcher.AddMsg(std::move(msg), 0));
    ASSERT_TRUE(!!msg);
    SetProcessed(msg);
    ASSERT_TRUE(batcher.IsEmpty());
    ASSERT_TRUE(msg_list.empty());
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
