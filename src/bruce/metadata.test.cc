/* <bruce/metadata.test.cc>

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

   Unit test for <bruce/metadata.h>.
 */

#include <memory>
#include <unordered_set>

#include <bruce/metadata.h>

#include <gtest/gtest.h>

using namespace Bruce;

namespace {

  int FindBrokerIndex(const std::vector<TMetadata::TBroker> &brokers,
      int32_t broker_id) {
    int i = 0;

    for (; static_cast<size_t>(i) < brokers.size(); ++i) {
      if (brokers[i].GetId() == broker_id) {
        return i;
      }
    }

    return -1;
  }

  /* The fixture for testing class TMetadata. */
  class TMetadataTest : public ::testing::Test {
    protected:
    TMetadataTest() {
    }

    virtual ~TMetadataTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TMetadataTest

  TEST_F(TMetadataTest, Test1) {
    TMetadata::TBuilder builder;
    std::unique_ptr<TMetadata> md(builder.Build());
    ASSERT_TRUE(!!md);
    ASSERT_TRUE(md->GetBrokers().empty());
    ASSERT_TRUE(md->GetTopics().empty());
    ASSERT_EQ(md->FindTopicIndex("blah"), -1);
    ASSERT_TRUE(md->SanityCheck());
    ASSERT_EQ(md->NumInServiceBrokers(), 0U);
    std::unique_ptr<TMetadata> md2(builder.Build());
    ASSERT_TRUE(*md2 == *md);
  }

  TEST_F(TMetadataTest, Test2) {
    TMetadata::TBuilder builder;
    builder.OpenBrokerList();
    builder.AddBroker(5, "host1", 101);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(6, 5, 9);
    builder.AddPartitionToTopic(3, 2, 0);
    builder.AddPartitionToTopic(7, 2, 5);  // out of service partition
    builder.AddPartitionToTopic(4, 5, 0);
    builder.AddPartitionToTopic(1, 7, 6);  // out of service partition
    builder.CloseTopic();
    builder.OpenTopic("topic2");
    builder.CloseTopic();
    builder.OpenTopic("topic3");
    builder.AddPartitionToTopic(8, 3, 0);
    builder.AddPartitionToTopic(6, 5, 9);
    builder.AddPartitionToTopic(3, 3, 0);
    builder.CloseTopic();
    std::unique_ptr<TMetadata> md(builder.Build());
    ASSERT_TRUE(!!md);
    ASSERT_TRUE(md->SanityCheck());
    ASSERT_EQ(md->NumInServiceBrokers(), 3U);
    const std::vector<TMetadata::TTopic> &topics = md->GetTopics();
    const std::vector<TMetadata::TBroker> &brokers = md->GetBrokers();
    ASSERT_EQ(md->GetBrokers().size(), 4U);
    ASSERT_EQ(topics.size(), 3U);

    for (const TMetadata::TBroker &b : brokers) {
      ASSERT_EQ((b.GetId() != 7), b.IsInService());
    }

    ASSERT_EQ(md->FindTopicIndex("blah"), -1);
    int topic1_index = md->FindTopicIndex("topic1");
    ASSERT_GE(topic1_index, 0);
    const TMetadata::TTopic &topic1 = topics[topic1_index];
    const std::vector<TMetadata::TPartition> &topic1_ok_partitions =
        topic1.GetOkPartitions();
    ASSERT_EQ(topic1_ok_partitions.size(), 3U);
    size_t i = 0;

    for (i = 0; i < topic1_ok_partitions.size(); ++i) {
      if (topic1_ok_partitions[i].GetId() == 6) {
        break;
      }
    }

    ASSERT_LT(i, topic1_ok_partitions.size());
    ASSERT_EQ(brokers[topic1_ok_partitions[i].GetBrokerIndex()].GetId(), 5U);
    ASSERT_EQ(topic1_ok_partitions[i].GetErrorCode(), 9);

    for (i = 0; i < topic1_ok_partitions.size(); ++i) {
      if (topic1_ok_partitions[i].GetId() == 3) {
        break;
      }
    }

    ASSERT_LT(i, topic1_ok_partitions.size());
    ASSERT_EQ(brokers[topic1_ok_partitions[i].GetBrokerIndex()].GetId(), 2U);
    ASSERT_EQ(topic1_ok_partitions[i].GetErrorCode(), 0);

    for (i = 0; i < topic1_ok_partitions.size(); ++i) {
      if (topic1_ok_partitions[i].GetId() == 4) {
        break;
      }
    }

    ASSERT_LT(i, topic1_ok_partitions.size());
    ASSERT_EQ(brokers[topic1_ok_partitions[i].GetBrokerIndex()].GetId(), 5U);
    ASSERT_EQ(topic1_ok_partitions[i].GetErrorCode(), 0);

    const std::vector<TMetadata::TPartition> &topic1_bad_partitions =
        topic1.GetOutOfServicePartitions();
    ASSERT_EQ(topic1_bad_partitions.size(), 2U);

    for (i = 0; i < topic1_bad_partitions.size(); ++i) {
      if (topic1_bad_partitions[i].GetId() == 7) {
        break;
      }
    }

    ASSERT_LT(i, topic1_bad_partitions.size());
    ASSERT_EQ(brokers[topic1_bad_partitions[i].GetBrokerIndex()].GetId(), 2U);
    ASSERT_EQ(topic1_bad_partitions[i].GetErrorCode(), 5);

    for (i = 0; i < topic1_bad_partitions.size(); ++i) {
      if (topic1_bad_partitions[i].GetId() == 1) {
        break;
      }
    }

    ASSERT_LT(i, topic1_bad_partitions.size());
    ASSERT_EQ(brokers[topic1_bad_partitions[i].GetBrokerIndex()].GetId(), 7U);
    ASSERT_EQ(topic1_bad_partitions[i].GetErrorCode(), 6);

    const std::vector<TMetadata::TPartition> &topic1_all_partitions =
        topic1.GetAllPartitions();
    ASSERT_EQ(topic1_all_partitions.size(), 5U);
    ASSERT_EQ(topic1_all_partitions[0].GetId(), 1U);
    ASSERT_EQ(topic1_all_partitions[1].GetId(), 3U);
    ASSERT_EQ(topic1_all_partitions[2].GetId(), 4U);
    ASSERT_EQ(topic1_all_partitions[3].GetId(), 6U);
    ASSERT_EQ(topic1_all_partitions[4].GetId(), 7U);

    int topic2_index = md->FindTopicIndex("topic2");
    ASSERT_GE(topic2_index, 0);
    const TMetadata::TTopic &topic2 = topics[topic2_index];
    ASSERT_TRUE(topic2.GetOkPartitions().empty());
    ASSERT_TRUE(topic2.GetOutOfServicePartitions().empty());
    ASSERT_TRUE(topic2.GetAllPartitions().empty());
    int topic3_index = md->FindTopicIndex("topic3");
    ASSERT_GE(topic3_index, 0);
    const TMetadata::TTopic &topic3 = topics[topic3_index];
    const std::vector<TMetadata::TPartition> &topic3_ok_partitions =
        topic3.GetOkPartitions();
    ASSERT_EQ(topic3_ok_partitions.size(), 3U);
    ASSERT_TRUE(topic3.GetOutOfServicePartitions().empty());

    for (i = 0; i < topic3_ok_partitions.size(); ++i) {
      if (topic3_ok_partitions[i].GetId() == 8) {
        break;
      }
    }

    ASSERT_LT(i, topic3_ok_partitions.size());
    ASSERT_EQ(brokers[topic3_ok_partitions[i].GetBrokerIndex()].GetId(), 3U);
    ASSERT_EQ(topic3_ok_partitions[i].GetErrorCode(), 0);

    for (i = 0; i < topic3_ok_partitions.size(); ++i) {
      if (topic3_ok_partitions[i].GetId() == 6) {
        break;
      }
    }

    ASSERT_LT(i, topic3_ok_partitions.size());
    ASSERT_EQ(brokers[topic3_ok_partitions[i].GetBrokerIndex()].GetId(), 5U);
    ASSERT_EQ(topic3_ok_partitions[i].GetErrorCode(), 9);

    for (i = 0; i < topic3_ok_partitions.size(); ++i) {
      if (topic3_ok_partitions[i].GetId() == 3) {
        break;
      }
    }

    ASSERT_LT(i, topic3_ok_partitions.size());
    ASSERT_EQ(brokers[topic3_ok_partitions[i].GetBrokerIndex()].GetId(), 3U);
    ASSERT_EQ(topic3_ok_partitions[i].GetErrorCode(), 0);

    const std::vector<TMetadata::TPartition> &topic3_all_partitions =
        topic3.GetAllPartitions();
    ASSERT_EQ(topic3_all_partitions.size(), 3U);
    ASSERT_EQ(topic3_all_partitions[0].GetId(), 3U);
    ASSERT_EQ(topic3_all_partitions[1].GetId(), 6U);
    ASSERT_EQ(topic3_all_partitions[2].GetId(), 8U);

    int index = FindBrokerIndex(brokers, 3);
    ASSERT_GE(index, 0);
    size_t num_choices = 100;
    const int32_t *choices = md->FindPartitionChoices("topic1", index,
        num_choices);
    ASSERT_TRUE(choices == nullptr);
    ASSERT_EQ(num_choices, 0U);
    index = FindBrokerIndex(brokers, 5);
    ASSERT_GE(index, 0);
    num_choices = 100;
    choices = md->FindPartitionChoices("topic1", index, num_choices);
    ASSERT_TRUE(choices != nullptr);
    ASSERT_EQ(num_choices, 2U);
    std::unordered_set<int32_t> choice_set;

    for (i = 0; i < num_choices; ++i) {
      choice_set.insert(choices[i]);
    }

    std::unordered_set<int32_t> expected_choice_set({ 6, 4 });
    ASSERT_TRUE(choice_set == expected_choice_set);
    index = FindBrokerIndex(brokers, 2);
    ASSERT_GE(index, 0);
    num_choices = 100;
    choices = md->FindPartitionChoices("topic1", index, num_choices);
    ASSERT_TRUE(choices != nullptr);
    ASSERT_EQ(num_choices, 1U);
    choice_set.clear();

    for (i = 0; i < num_choices; ++i) {
      choice_set.insert(choices[i]);
    }

    expected_choice_set.clear();
    expected_choice_set.insert(3);
    ASSERT_TRUE(choice_set == expected_choice_set);
    index = FindBrokerIndex(brokers, 3);
    ASSERT_GE(index, 0);
    num_choices = 100;
    choices = md->FindPartitionChoices("topic2", index, num_choices);
    ASSERT_TRUE(choices == nullptr);
    ASSERT_EQ(num_choices, 0U);
    index = FindBrokerIndex(brokers, 3);
    ASSERT_GE(index, 0);
    num_choices = 100;
    choices = md->FindPartitionChoices("topic3", index, num_choices);
    ASSERT_TRUE(choices != nullptr);
    ASSERT_EQ(num_choices, 2U);
    choice_set.clear();

    for (i = 0; i < num_choices; ++i) {
      choice_set.insert(choices[i]);
    }

    expected_choice_set.clear();
    expected_choice_set.insert(8);
    expected_choice_set.insert(3);
    ASSERT_TRUE(choice_set == expected_choice_set);
    index = FindBrokerIndex(brokers, 5);
    ASSERT_GE(index, 0);
    num_choices = 100;
    choices = md->FindPartitionChoices("topic3", index, num_choices);
    ASSERT_TRUE(choices != nullptr);
    ASSERT_EQ(num_choices, 1U);
    choice_set.clear();

    for (i = 0; i < num_choices; ++i) {
      choice_set.insert(choices[i]);
    }

    expected_choice_set.clear();
    expected_choice_set.insert(6);
    ASSERT_TRUE(choice_set == expected_choice_set);
  }

  TEST_F(TMetadataTest, Test3) {
    TMetadata::TBuilder builder;
    builder.OpenBrokerList();
    builder.AddBroker(5, "host1", 101);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(6, 5, 4);
    builder.AddPartitionToTopic(3, 2, 8);
    builder.AddPartitionToTopic(4, 5, 7);
    builder.CloseTopic();
    builder.OpenTopic("topic2");
    builder.CloseTopic();
    builder.OpenTopic("topic3");
    builder.AddPartitionToTopic(8, 3, 9);
    builder.AddPartitionToTopic(6, 5, 0);
    builder.AddPartitionToTopic(3, 3, 6);
    builder.CloseTopic();
    std::unique_ptr<TMetadata> md1(builder.Build());
    ASSERT_TRUE(!!md1);
    ASSERT_TRUE(md1->SanityCheck());

    builder.OpenBrokerList();
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(5, "host1", 101);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(4, 5, 7);
    builder.AddPartitionToTopic(6, 5, 4);
    builder.AddPartitionToTopic(3, 2, 8);
    builder.CloseTopic();
    builder.OpenTopic("topic2");
    builder.CloseTopic();
    builder.OpenTopic("topic3");
    builder.AddPartitionToTopic(3, 3, 6);
    builder.AddPartitionToTopic(8, 3, 9);
    builder.AddPartitionToTopic(6, 5, 0);
    builder.CloseTopic();
    std::unique_ptr<TMetadata> md2(builder.Build());
    ASSERT_TRUE(!!md2);
    ASSERT_TRUE(md2->SanityCheck());

    ASSERT_TRUE(*md1 == *md2);
    ASSERT_TRUE(*md2 == *md1);
    ASSERT_FALSE(*md1 != *md2);
    ASSERT_FALSE(*md2 != *md1);

    builder.OpenBrokerList();
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(1, "host1", 101);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(4, 1, 7);
    builder.AddPartitionToTopic(6, 1, 4);
    builder.AddPartitionToTopic(3, 2, 8);
    builder.CloseTopic();
    builder.OpenTopic("topic2");
    builder.CloseTopic();
    builder.OpenTopic("topic3");
    builder.AddPartitionToTopic(3, 3, 6);
    builder.AddPartitionToTopic(8, 3, 9);
    builder.AddPartitionToTopic(6, 1, 0);
    builder.CloseTopic();
    std::unique_ptr<TMetadata> md3(builder.Build());
    ASSERT_TRUE(!!md3);
    ASSERT_TRUE(md3->SanityCheck());

    ASSERT_FALSE(*md3 == *md2);

    builder.OpenBrokerList();
    builder.AddBroker(7, "blah", 103);
    builder.AddBroker(3, "host4", 104);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(5, "host1", 101);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(4, 5, 7);
    builder.AddPartitionToTopic(6, 5, 4);
    builder.AddPartitionToTopic(3, 2, 8);
    builder.CloseTopic();
    builder.OpenTopic("topic2");
    builder.CloseTopic();
    builder.OpenTopic("topic3");
    builder.AddPartitionToTopic(3, 3, 6);
    builder.AddPartitionToTopic(8, 3, 9);
    builder.AddPartitionToTopic(6, 5, 0);
    builder.CloseTopic();
    std::unique_ptr<TMetadata> md4(builder.Build());
    ASSERT_TRUE(!!md4);
    ASSERT_TRUE(md4->SanityCheck());

    ASSERT_FALSE(*md4 == *md2);

    builder.OpenBrokerList();
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(5, "host1", 105);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(4, 5, 7);
    builder.AddPartitionToTopic(6, 5, 4);
    builder.AddPartitionToTopic(3, 2, 8);
    builder.CloseTopic();
    builder.OpenTopic("topic2");
    builder.CloseTopic();
    builder.OpenTopic("topic3");
    builder.AddPartitionToTopic(3, 3, 6);
    builder.AddPartitionToTopic(8, 3, 9);
    builder.AddPartitionToTopic(6, 5, 0);
    builder.CloseTopic();
    std::unique_ptr<TMetadata> md5(builder.Build());
    ASSERT_TRUE(!!md5);
    ASSERT_TRUE(md5->SanityCheck());

    ASSERT_FALSE(*md5 == *md2);

    builder.OpenBrokerList();
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(5, "host1", 101);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(1, 5, 7);
    builder.AddPartitionToTopic(6, 5, 4);
    builder.AddPartitionToTopic(3, 2, 8);
    builder.CloseTopic();
    builder.OpenTopic("topic2");
    builder.CloseTopic();
    builder.OpenTopic("topic3");
    builder.AddPartitionToTopic(3, 3, 6);
    builder.AddPartitionToTopic(8, 3, 9);
    builder.AddPartitionToTopic(6, 5, 0);
    builder.CloseTopic();
    std::unique_ptr<TMetadata> md6(builder.Build());
    ASSERT_TRUE(!!md6);
    ASSERT_TRUE(md6->SanityCheck());

    ASSERT_FALSE(*md6 == *md2);

    builder.OpenBrokerList();
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(5, "host1", 101);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(4, 5, 7);
    builder.AddPartitionToTopic(6, 2, 4);
    builder.AddPartitionToTopic(3, 2, 8);
    builder.CloseTopic();
    builder.OpenTopic("topic2");
    builder.CloseTopic();
    builder.OpenTopic("topic3");
    builder.AddPartitionToTopic(3, 3, 6);
    builder.AddPartitionToTopic(8, 3, 9);
    builder.AddPartitionToTopic(6, 5, 0);
    builder.CloseTopic();
    std::unique_ptr<TMetadata> md7(builder.Build());
    ASSERT_TRUE(!!md7);
    ASSERT_TRUE(md7->SanityCheck());

    ASSERT_FALSE(*md7 == *md2);

    builder.OpenBrokerList();
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(5, "host1", 101);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(4, 5, 7);
    builder.AddPartitionToTopic(6, 5, 4);
    builder.AddPartitionToTopic(3, 2, 8);
    builder.CloseTopic();
    builder.OpenTopic("topic2");
    builder.CloseTopic();
    builder.OpenTopic("topic3");
    builder.AddPartitionToTopic(3, 3, 6);
    builder.AddPartitionToTopic(8, 3, 9);
    builder.AddPartitionToTopic(6, 5, 5);
    builder.CloseTopic();
    std::unique_ptr<TMetadata> md8(builder.Build());
    ASSERT_TRUE(!!md8);
    ASSERT_TRUE(md8->SanityCheck());

    ASSERT_FALSE(*md8 == *md2);
  }

  TEST_F(TMetadataTest, Test4) {
    TMetadata::TBuilder builder;
    builder.OpenBrokerList();
    builder.AddBroker(5, "host1", 101);
    builder.AddBroker(2, "host2", 102);
    bool threw = false;

    try {
      builder.AddBroker(2, "host3", 103);
    } catch (const TMetadata::TDuplicateBroker &) {
      threw = true;
    }

    ASSERT_TRUE(threw);
    builder.Reset();

    builder.OpenBrokerList();
    builder.AddBroker(5, "host1", 101);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(6, 5, 4);
    builder.AddPartitionToTopic(3, 2, 8);
    builder.AddPartitionToTopic(4, 5, 7);
    builder.CloseTopic();
    threw = false;

    try {
      builder.OpenTopic("topic1");
    } catch (const TMetadata::TDuplicateTopic &) {
      threw = true;
    }

    ASSERT_TRUE(threw);
    builder.Reset();

    builder.OpenBrokerList();
    builder.AddBroker(5, "host1", 101);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(6, 5, 4);
    threw = false;

    try {
      builder.AddPartitionToTopic(3, 1, 8);
    } catch (const TMetadata::TPartitionHasUnknownBroker &) {
      threw = true;
    }

    ASSERT_TRUE(threw);
    builder.Reset();

    builder.OpenBrokerList();
    builder.AddBroker(5, "host1", 101);
    builder.AddBroker(2, "host2", 102);
    builder.AddBroker(7, "host3", 103);
    builder.AddBroker(3, "host4", 104);
    builder.CloseBrokerList();
    builder.OpenTopic("topic1");
    builder.AddPartitionToTopic(6, 5, 4);
    threw = false;

    try {
      builder.AddPartitionToTopic(6, 2, 8);
    } catch (const TMetadata::TDuplicatePartition &) {
      threw = true;
    }

    ASSERT_TRUE(threw);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
