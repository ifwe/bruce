/* <bruce/anomaly_tracker.test.cc>

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

   Unit test for <bruce/anomaly_tracker.h>
 */

#include <bruce/anomaly_tracker.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

#include <bruce/discard_file_logger.h>
#include <bruce/msg.h>
#include <bruce/msg_creator.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/test_util/misc_util.h>
#include <capped/blob.h>
#include <capped/pool.h>
#include <capped/reader.h>

#include <gtest/gtest.h>

using namespace Bruce;
using namespace Bruce::TestUtil;
using namespace Capped;

namespace {

  /* A fake clock class for testing with an annoying name that rhymes.  Pretty
     exciting, huh? */
  class TMockClock final {
    public:
    explicit TMockClock(uint64_t *value_store)
        : ValueStore(value_store) {
      *ValueStore = 0;
    }

    TMockClock(uint64_t *value_store, uint64_t timestamp)
        : ValueStore(value_store) {
      *ValueStore = timestamp;
    }

    uint64_t operator()() const {
      assert(this);
      return *ValueStore;
    }

    TMockClock &operator=(const TMockClock &that) {
      assert(this);

      if (this != &that) {
        *ValueStore = *that.ValueStore;
      }

      return *this;
    }

    TMockClock &operator=(uint64_t timestamp) {
      assert(this);
      *ValueStore = timestamp;
      return *this;
    }

    private:
    /* Pointer to storage for clock value. */
    uint64_t *ValueStore;
  };  // TMockClock

  struct TAnomalyTrackerConfig {
    std::unique_ptr<TPool> Pool;

    uint64_t ClockValue;

    TMockClock Clock;  // hickory dickory dock

    TDiscardFileLogger DiscardFileLogger;

    TAnomalyTracker AnomalyTracker;

    TMsgStateTracker MsgStateTracker;

    explicit TAnomalyTrackerConfig(size_t report_interval);

    TMsg::TPtr NewMsg(const std::string &topic, const std::string &value) {
      TMsg::TPtr msg = TMsgCreator::CreateAnyPartitionMsg(Clock(), topic.data(),
          topic.data() + topic.size(), nullptr, 0, value.data(), value.size(),
          false, *Pool, MsgStateTracker);

      /* Avoid assertion failure in TMsg destructor. */
      SetProcessed(msg);

      return std::move(msg);
    }
  };  // TAnomalyTrackerConfig

  TAnomalyTrackerConfig::TAnomalyTrackerConfig(size_t report_interval)
      : Pool(new TPool(64, 1024 * 1024, TPool::TSync::Mutexed)),
        ClockValue(0),
        Clock(&ClockValue),
        AnomalyTracker(DiscardFileLogger, report_interval,
                       std::numeric_limits<size_t>::max(), Clock) {
  }

  /* The fixture for testing class TAnomalyTracker. */
  class TAnomalyTrackerTest : public ::testing::Test {
    protected:
    TAnomalyTrackerTest() {
    }

    virtual ~TAnomalyTrackerTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TAnomalyTrackerTest

  TEST_F(TAnomalyTrackerTest, BasicTest) {
    TAnomalyTrackerConfig cfg(100);
    ASSERT_EQ(cfg.AnomalyTracker.GetReportInterval(), 100U);
    const std::string topic_1("topic_1"), topic_2("topic_2"),
        bad_topic_1("bad"), bad_topic_2("badder"), bad_topic_3("baddest"),
        bad_msg_1("bad msg 1"), bad_msg_2("bad msg 2"), bad_msg_3("bad msg 3");
    const std::string body("hubba bubba");
    const char *body_begin = body.data();
    const char *body_end = body_begin + body.size();
    cfg.Clock = 5;
    TMsg::TPtr msg = cfg.NewMsg(topic_1, body);
    ASSERT_TRUE(msg->GetTimestamp() == 5);
    cfg.AnomalyTracker.TrackDiscard(msg,
        TAnomalyTracker::TDiscardReason::KafkaErrorAck);
    msg = cfg.NewMsg(topic_2, body);
    ASSERT_TRUE(msg->GetTimestamp() == 5);
    cfg.AnomalyTracker.TrackDiscard(msg,
        TAnomalyTracker::TDiscardReason::KafkaErrorAck);
    cfg.AnomalyTracker.TrackDuplicate(msg);
    cfg.Clock = 10;
    msg = cfg.NewMsg(topic_2, body);
    ASSERT_TRUE(msg->GetTimestamp() == 10);
    cfg.AnomalyTracker.TrackDiscard(msg,
        TAnomalyTracker::TDiscardReason::KafkaErrorAck);
    cfg.AnomalyTracker.TrackDiscard(msg,
        TAnomalyTracker::TDiscardReason::KafkaErrorAck);
    cfg.AnomalyTracker.TrackDuplicate(msg);
    cfg.Clock = 20;
    msg = cfg.NewMsg(topic_2, body);
    ASSERT_TRUE(msg->GetTimestamp() == 20);
    cfg.AnomalyTracker.TrackDiscard(msg,
        TAnomalyTracker::TDiscardReason::KafkaErrorAck);
    cfg.Clock = 25;
    cfg.AnomalyTracker.TrackNoMemDiscard(cfg.Clock(), topic_2.data(),
        topic_2.data() + topic_2.size(), nullptr, nullptr, body_begin,
        body_end);
    msg = cfg.NewMsg(bad_topic_1, body);
    cfg.AnomalyTracker.TrackBadTopicDiscard(msg);
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_topic_2.data(),
        bad_topic_2.data() + bad_topic_2.size(), nullptr, nullptr, body_begin,
        body_end);
    cfg.AnomalyTracker.TrackBadTopicDiscard(msg);
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_topic_3.data(),
        bad_topic_3.data() + bad_topic_3.size(), nullptr, nullptr, body_begin,
        body_end);

    cfg.AnomalyTracker.TrackMalformedMsgDiscard(bad_msg_1.data(),
        bad_msg_1.data() + bad_msg_1.size());
    cfg.AnomalyTracker.TrackMalformedMsgDiscard(bad_msg_2.data(),
        bad_msg_2.data() + bad_msg_2.size());
    cfg.AnomalyTracker.TrackMalformedMsgDiscard(bad_msg_3.data(),
        bad_msg_3.data() + bad_msg_3.size());
    cfg.AnomalyTracker.TrackMalformedMsgDiscard(bad_msg_2.data(),
        bad_msg_2.data() + bad_msg_2.size());

    TAnomalyTracker::TInfo filling_report;
    std::shared_ptr<const TAnomalyTracker::TInfo> last_full_report =
        cfg.AnomalyTracker.GetInfo(filling_report);

    ASSERT_FALSE(!!last_full_report);

    ASSERT_EQ(filling_report.GetReportId(), 0U);
    ASSERT_EQ(filling_report.GetStartTime(), 0U);
    ASSERT_EQ(filling_report.DiscardTopicMap.size(), 2U);

    auto topic_1_iter = filling_report.DiscardTopicMap.find(topic_1);
    ASSERT_TRUE(topic_1_iter != filling_report.DiscardTopicMap.end());
    const TAnomalyTracker::TTopicInfo &topic_1_info = topic_1_iter->second;
    ASSERT_TRUE(topic_1_info.Interval.First == 5);
    ASSERT_TRUE(topic_1_info.Interval.Last == 5);
    ASSERT_EQ(topic_1_info.Count, 1U);

    auto topic_2_iter = filling_report.DiscardTopicMap.find(topic_2);
    ASSERT_TRUE(topic_2_iter != filling_report.DiscardTopicMap.end());
    const TAnomalyTracker::TTopicInfo &topic_2_info = topic_2_iter->second;
    ASSERT_TRUE(topic_2_info.Interval.First == 5);
    ASSERT_TRUE(topic_2_info.Interval.Last == 25);
    ASSERT_EQ(topic_2_info.Count, 5U);

    auto duplicate_topic_iter = filling_report.DuplicateTopicMap.find(topic_2);
    ASSERT_TRUE(duplicate_topic_iter !=
                filling_report.DuplicateTopicMap.end());
    const TAnomalyTracker::TTopicInfo &duplicate_topic_info =
        duplicate_topic_iter->second;
    ASSERT_TRUE(duplicate_topic_info.Interval.First == 5);
    ASSERT_TRUE(duplicate_topic_info.Interval.Last == 10);
    ASSERT_EQ(duplicate_topic_info.Count, 2U);

    const std::list<std::string> &malformed = filling_report.MalformedMsgs;
    ASSERT_EQ(malformed.size(), 3U);
    auto malformed_iter = malformed.begin();
    ASSERT_EQ(*malformed_iter, bad_msg_2);
    ASSERT_EQ(*++malformed_iter, bad_msg_3);
    ASSERT_EQ(*++malformed_iter, bad_msg_1);
    ASSERT_EQ(filling_report.MalformedMsgCount, 4U);

    const std::list<std::string> &bad_topic = filling_report.BadTopics;
    ASSERT_EQ(bad_topic.size(), 3U);
    auto bad_topic_iter = bad_topic.begin();
    ASSERT_EQ(*bad_topic_iter, bad_topic_3);
    ASSERT_EQ(*++bad_topic_iter, bad_topic_1);
    ASSERT_EQ(*++bad_topic_iter, bad_topic_2);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 4U);

    ASSERT_TRUE(filling_report.LongMsgs.empty());

    cfg.Clock = 100;
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);

    if (last_full_report) {
      ASSERT_EQ(last_full_report->GetReportId(), 0U);
      ASSERT_EQ(last_full_report->DiscardTopicMap.size(), 2U);
      ASSERT_EQ(last_full_report->DuplicateTopicMap.size(), 1U);
      ASSERT_EQ(last_full_report->MalformedMsgs.size(), 3U);
      ASSERT_EQ(last_full_report->BadTopics.size(), 3U);
      ASSERT_EQ(filling_report.GetReportId(), 1U);
      ASSERT_EQ(filling_report.DiscardTopicMap.size(), 0U);
      ASSERT_EQ(filling_report.DuplicateTopicMap.size(), 0U);
      ASSERT_EQ(filling_report.MalformedMsgs.size(), 0U);
      ASSERT_EQ(filling_report.BadTopics.size(), 0U);
    }
  }

  TEST_F(TAnomalyTrackerTest, LongMsgDiscardTest) {
    TAnomalyTrackerConfig cfg(100);
    ASSERT_EQ(cfg.AnomalyTracker.GetReportInterval(), 100U);
    const std::string topic_1("topic_1"), topic_2("topic_2"),
        body_1("long msg 1"), body_2("long msg 2"), body_3("long msg 3");
    cfg.Clock = 0;
    TMsg::TPtr msg = cfg.NewMsg(topic_1, body_1);
    ASSERT_TRUE(msg->GetTimestamp() == 0);
    cfg.AnomalyTracker.TrackLongMsgDiscard(msg);
    cfg.Clock = 20;
    msg = cfg.NewMsg(topic_1, body_2);
    ASSERT_TRUE(msg->GetTimestamp() == 20);
    cfg.AnomalyTracker.TrackLongMsgDiscard(msg);
    cfg.Clock = 30;
    msg = cfg.NewMsg(topic_1, body_3);
    ASSERT_TRUE(msg->GetTimestamp() == 30);
    cfg.AnomalyTracker.TrackLongMsgDiscard(msg);
    cfg.Clock = 40;
    msg = cfg.NewMsg(topic_1, body_2);
    ASSERT_TRUE(msg->GetTimestamp() == 40);
    cfg.AnomalyTracker.TrackLongMsgDiscard(msg);
    msg = cfg.NewMsg(topic_2, body_1);
    ASSERT_TRUE(msg->GetTimestamp() == 40);
    cfg.AnomalyTracker.TrackLongMsgDiscard(msg);

    TAnomalyTracker::TInfo filling_report;
    std::shared_ptr<const TAnomalyTracker::TInfo> last_full_report =
        cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_FALSE(!!last_full_report);
    ASSERT_EQ(filling_report.LongMsgs.size(), 4U);
    std::string long_msg(topic_2 + " " + body_1);
    auto list_iter = filling_report.LongMsgs.begin();
    ASSERT_EQ(*list_iter, long_msg);
    long_msg = topic_1 + " " + body_2;
    ASSERT_EQ(*++list_iter, long_msg);
    long_msg = topic_1 + " " + body_3;
    ASSERT_EQ(*++list_iter, long_msg);
    long_msg = topic_1 + " " + body_1;
    ASSERT_EQ(*++list_iter, long_msg);

    auto map_iter = filling_report.DiscardTopicMap.find(topic_1);
    ASSERT_TRUE(map_iter != filling_report.DiscardTopicMap.end());
    ASSERT_TRUE(map_iter->second.Interval.First == 0);
    ASSERT_TRUE(map_iter->second.Interval.Last == 40);
    ASSERT_EQ(map_iter->second.Count, 4U);
    map_iter = filling_report.DiscardTopicMap.find(topic_2);
    ASSERT_TRUE(map_iter != filling_report.DiscardTopicMap.end());
    ASSERT_TRUE(map_iter->second.Interval.First == 40);
    ASSERT_TRUE(map_iter->second.Interval.Last == 40);
    ASSERT_EQ(map_iter->second.Count, 1U);
  }

  TEST_F(TAnomalyTrackerTest, GetInfoTest) {
    TAnomalyTrackerConfig cfg(100);
    ASSERT_EQ(cfg.AnomalyTracker.GetReportInterval(), 100U);
    const std::string bad_topic("bad");
    cfg.Clock = 0;
    const char *bad_begin = bad_topic.data();
    const char *bad_end = bad_begin + bad_topic.size();
    const char *no_body = "";
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    TAnomalyTracker::TInfo filling_report;
    std::shared_ptr<const TAnomalyTracker::TInfo> last_full_report =
        cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_FALSE(!!last_full_report);
    ASSERT_EQ(filling_report.GetReportId(), 0U);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 1U);
    cfg.Clock = 99;
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_FALSE(!!last_full_report);
    ASSERT_EQ(filling_report.GetReportId(), 0U);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 1U);
    cfg.Clock = 100;
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 0U);
    ASSERT_EQ(last_full_report->BadTopicMsgCount, 1U);
    ASSERT_EQ(filling_report.GetReportId(), 1U);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 0U);
    cfg.Clock = 150;
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 0U);
    ASSERT_EQ(last_full_report->BadTopicMsgCount, 1U);
    ASSERT_EQ(filling_report.GetReportId(), 1U);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 2U);
    cfg.Clock = 199;
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 0U);
    ASSERT_EQ(last_full_report->BadTopicMsgCount, 1U);
    ASSERT_EQ(filling_report.GetReportId(), 1U);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 2U);
    cfg.Clock = 200;
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 1U);
    ASSERT_EQ(last_full_report->BadTopicMsgCount, 2U);
    ASSERT_EQ(filling_report.GetReportId(), 2U);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 0U);
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 1U);
    ASSERT_EQ(last_full_report->BadTopicMsgCount, 2U);
    ASSERT_EQ(filling_report.GetReportId(), 2U);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 3U);
    cfg.Clock = 399;
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    cfg.AnomalyTracker.TrackBadTopicDiscard(cfg.Clock(), bad_begin, bad_end,
        nullptr, nullptr, no_body, no_body);
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 2U);
    ASSERT_EQ(last_full_report->BadTopicMsgCount, 3U);
    ASSERT_EQ(filling_report.GetReportId(), 3U);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 4U);
    cfg.Clock = 500;
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 4U);
    ASSERT_EQ(last_full_report->BadTopicMsgCount, 0U);
    ASSERT_EQ(filling_report.GetReportId(), 5U);
    ASSERT_EQ(filling_report.BadTopicMsgCount, 0U);
    cfg.Clock = 1000;
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 9U);
    ASSERT_EQ(filling_report.GetReportId(), 10U);
    cfg.Clock = 1599;
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 14U);
    ASSERT_EQ(filling_report.GetReportId(), 15U);
    cfg.Clock = 1600;
    last_full_report = cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 15U);
    ASSERT_EQ(filling_report.GetReportId(), 16U);
  }

  TEST_F(TAnomalyTrackerTest, GetInfoTest2) {
    TAnomalyTrackerConfig cfg(100);
    ASSERT_EQ(cfg.AnomalyTracker.GetReportInterval(), 100U);
    cfg.Clock = 100;
    TAnomalyTracker::TInfo filling_report;
    std::shared_ptr<const TAnomalyTracker::TInfo> last_full_report =
        cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 0U);
    ASSERT_EQ(filling_report.GetReportId(), 1U);
  }

  TEST_F(TAnomalyTrackerTest, GetInfoTest3) {
    TAnomalyTrackerConfig cfg(100);
    ASSERT_EQ(cfg.AnomalyTracker.GetReportInterval(), 100U);
    cfg.Clock = 200;
    TAnomalyTracker::TInfo filling_report;
    std::shared_ptr<const TAnomalyTracker::TInfo> last_full_report =
        cfg.AnomalyTracker.GetInfo(filling_report);
    ASSERT_TRUE(!!last_full_report);
    ASSERT_EQ(last_full_report->GetReportId(), 1U);
    ASSERT_EQ(filling_report.GetReportId(), 2U);
  }

  TEST_F(TAnomalyTrackerTest, CheckGetInfoRateTest) {
    TAnomalyTrackerConfig cfg(100);
    ASSERT_EQ(cfg.AnomalyTracker.GetReportInterval(), 100U);
    ASSERT_EQ(TAnomalyTracker::GetNoDiscardQueryCount(), 0U);
    cfg.Clock = 100;
    cfg.AnomalyTracker.CheckGetInfoRate();
    ASSERT_EQ(TAnomalyTracker::GetNoDiscardQueryCount(), 0U);
    TAnomalyTracker::TInfo filling_report;
    cfg.AnomalyTracker.GetInfo(filling_report);
    cfg.Clock = 200;
    cfg.AnomalyTracker.CheckGetInfoRate();
    ASSERT_EQ(TAnomalyTracker::GetNoDiscardQueryCount(), 0U);
    cfg.Clock = 201;
    cfg.AnomalyTracker.CheckGetInfoRate();
    ASSERT_EQ(TAnomalyTracker::GetNoDiscardQueryCount(), 1U);
    cfg.Clock = 250;
    cfg.AnomalyTracker.CheckGetInfoRate();
    ASSERT_EQ(TAnomalyTracker::GetNoDiscardQueryCount(), 2U);
    cfg.AnomalyTracker.GetInfo(filling_report);
    cfg.AnomalyTracker.CheckGetInfoRate();
    ASSERT_EQ(TAnomalyTracker::GetNoDiscardQueryCount(), 2U);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
