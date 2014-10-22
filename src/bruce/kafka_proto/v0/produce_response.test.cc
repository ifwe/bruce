/* <bruce/kafka_proto/v0/produce_response.test.cc>

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

   Unit tests for <bruce/kafka_proto/v0/produce_response_reader.h> and
   <bruce/kafka_proto/v0/produce_response_writer.h>.
 */

#include <bruce/kafka_proto/v0/produce_response_reader.h>
#include <bruce/kafka_proto/v0/produce_response_writer.h>

#include <string>

#include <gtest/gtest.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

namespace {

  /* The fixture for testing classes TProduceResponseReader and
     TProduceResponseWriter. */
  class TProduceResponseTest : public ::testing::Test {
    protected:
    TProduceResponseTest() {
    }

    virtual ~TProduceResponseTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TProduceResponseTest

  TEST_F(TProduceResponseTest, ProduceResponseTest1) {
    std::vector<uint8_t> buf;
    TProduceResponseWriter writer;
    writer.OpenResponse(buf, 1234567);
    writer.CloseResponse();
    ASSERT_EQ(buf.size(), 12U);
    TProduceResponseReader reader;
    reader.SetResponse(&buf[0], buf.size());
    ASSERT_EQ(reader.GetCorrelationId(), 1234567);
    ASSERT_EQ(reader.GetNumTopics(), 0U);
    ASSERT_FALSE(reader.FirstTopic());
  }

  TEST_F(TProduceResponseTest, ProduceResponseTest2) {
    std::vector<uint8_t> buf;
    TProduceResponseWriter writer;
    writer.OpenResponse(buf, 1234567);
    std::string topic("The Jetsons");
    const char *topic_c_str = topic.c_str();
    writer.OpenTopic(topic_c_str, topic_c_str + topic.size());
    writer.CloseTopic();
    writer.CloseResponse();
    TProduceResponseReader reader;
    reader.SetResponse(&buf[0], buf.size());
    ASSERT_EQ(reader.GetCorrelationId(), 1234567);
    ASSERT_EQ(reader.GetNumTopics(), 1U);
    ASSERT_TRUE(reader.FirstTopic());
    std::string topic_copy(reader.GetCurrentTopicNameBegin(),
                           reader.GetCurrentTopicNameEnd());
    ASSERT_EQ(topic, topic_copy);
    ASSERT_EQ(reader.GetNumPartitionsInCurrentTopic(), 0U);
    ASSERT_FALSE(reader.FirstPartitionInTopic());
    ASSERT_FALSE(reader.NextTopic());
  }

  TEST_F(TProduceResponseTest, ProduceResponseTest3) {
    std::vector<uint8_t> buf;
    TProduceResponseWriter writer;
    writer.OpenResponse(buf, 1234567);
    std::string topic1("The Jetsons");
    const char *topic1_c_str = topic1.c_str();
    writer.OpenTopic(topic1_c_str, topic1_c_str + topic1.size());
    writer.CloseTopic();
    std::string topic2("The Flintstones");
    const char *topic2_c_str = topic2.c_str();
    writer.OpenTopic(topic2_c_str, topic2_c_str + topic2.size());
    writer.CloseTopic();
    writer.CloseResponse();
    TProduceResponseReader reader;
    reader.SetResponse(&buf[0], buf.size());
    ASSERT_EQ(reader.GetCorrelationId(), 1234567);
    ASSERT_EQ(reader.GetNumTopics(), 2U);
    ASSERT_TRUE(reader.FirstTopic());
    std::string topic1_copy(reader.GetCurrentTopicNameBegin(),
                            reader.GetCurrentTopicNameEnd());
    ASSERT_EQ(topic1, topic1_copy);
    ASSERT_EQ(reader.GetNumPartitionsInCurrentTopic(), 0U);
    ASSERT_FALSE(reader.FirstPartitionInTopic());
    ASSERT_TRUE(reader.NextTopic());
    std::string topic2_copy(reader.GetCurrentTopicNameBegin(),
                            reader.GetCurrentTopicNameEnd());
    ASSERT_EQ(topic2, topic2_copy);
    ASSERT_EQ(reader.GetNumPartitionsInCurrentTopic(), 0U);
    ASSERT_FALSE(reader.FirstPartitionInTopic());
    ASSERT_FALSE(reader.NextTopic());
  }

  TEST_F(TProduceResponseTest, ProduceResponseTest4) {
    std::vector<uint8_t> buf;
    TProduceResponseWriter writer;
    writer.OpenResponse(buf, 1234567);
    std::string topic1("The Jetsons");
    const char *topic1_c_str = topic1.c_str();
    writer.OpenTopic(topic1_c_str, topic1_c_str + topic1.size());
    writer.CloseTopic();
    std::string topic2("The Flintstones");
    const char *topic2_c_str = topic2.c_str();
    writer.OpenTopic(topic2_c_str, topic2_c_str + topic2.size());
    writer.AddPartition(98765, 432, 12345678901LL);
    writer.AddPartition(87654, 321, 23456789012LL);
    writer.CloseTopic();
    std::string topic3("Scooby Doo");
    const char *topic3_c_str = topic3.c_str();
    writer.OpenTopic(topic3_c_str, topic3_c_str + topic3.size());
    writer.CloseTopic();
    writer.CloseResponse();
    TProduceResponseReader reader;
    reader.SetResponse(&buf[0], buf.size());
    ASSERT_EQ(reader.GetCorrelationId(), 1234567);
    ASSERT_EQ(reader.GetNumTopics(), 3U);
    ASSERT_TRUE(reader.FirstTopic());
    std::string topic1_copy(reader.GetCurrentTopicNameBegin(),
                            reader.GetCurrentTopicNameEnd());
    ASSERT_EQ(topic1, topic1_copy);
    ASSERT_EQ(reader.GetNumPartitionsInCurrentTopic(), 0U);
    ASSERT_FALSE(reader.FirstPartitionInTopic());
    ASSERT_TRUE(reader.NextTopic());
    std::string topic2_copy(reader.GetCurrentTopicNameBegin(),
                            reader.GetCurrentTopicNameEnd());
    ASSERT_EQ(topic2, topic2_copy);
    ASSERT_EQ(reader.GetNumPartitionsInCurrentTopic(), 2U);
    ASSERT_TRUE(reader.FirstPartitionInTopic());

    ASSERT_EQ(reader.GetCurrentPartitionNumber(), 98765);
    ASSERT_EQ(reader.GetCurrentPartitionErrorCode(), 432);
    ASSERT_EQ(reader.GetCurrentPartitionOffset(), 12345678901LL);
    ASSERT_TRUE(reader.NextPartitionInTopic());
    ASSERT_EQ(reader.GetCurrentPartitionNumber(), 87654);
    ASSERT_EQ(reader.GetCurrentPartitionErrorCode(), 321);
    ASSERT_EQ(reader.GetCurrentPartitionOffset(), 23456789012LL);
    ASSERT_FALSE(reader.NextPartitionInTopic());

    ASSERT_TRUE(reader.FirstPartitionInTopic());

    ASSERT_EQ(reader.GetCurrentPartitionNumber(), 98765);
    ASSERT_EQ(reader.GetCurrentPartitionErrorCode(), 432);
    ASSERT_EQ(reader.GetCurrentPartitionOffset(), 12345678901LL);
    ASSERT_TRUE(reader.NextPartitionInTopic());
    ASSERT_EQ(reader.GetCurrentPartitionNumber(), 87654);
    ASSERT_EQ(reader.GetCurrentPartitionErrorCode(), 321);
    ASSERT_EQ(reader.GetCurrentPartitionOffset(), 23456789012LL);
    ASSERT_FALSE(reader.NextPartitionInTopic());

    ASSERT_TRUE(reader.NextTopic());
    std::string topic3_copy(reader.GetCurrentTopicNameBegin(),
                            reader.GetCurrentTopicNameEnd());
    ASSERT_EQ(topic3, topic3_copy);
    ASSERT_EQ(reader.GetNumPartitionsInCurrentTopic(), 0U);
    ASSERT_FALSE(reader.FirstPartitionInTopic());
    ASSERT_FALSE(reader.NextTopic());

    ASSERT_TRUE(reader.FirstTopic());
    topic1_copy.assign(reader.GetCurrentTopicNameBegin(),
                       reader.GetCurrentTopicNameEnd());
    ASSERT_EQ(topic1, topic1_copy);
    ASSERT_EQ(reader.GetNumPartitionsInCurrentTopic(), 0U);
    ASSERT_FALSE(reader.FirstPartitionInTopic());
    ASSERT_TRUE(reader.NextTopic());
    topic2_copy.assign(reader.GetCurrentTopicNameBegin(),
                       reader.GetCurrentTopicNameEnd());
    ASSERT_EQ(topic2, topic2_copy);
    ASSERT_EQ(reader.GetNumPartitionsInCurrentTopic(), 2U);
    ASSERT_TRUE(reader.FirstPartitionInTopic());

    ASSERT_EQ(reader.GetCurrentPartitionNumber(), 98765);
    ASSERT_EQ(reader.GetCurrentPartitionErrorCode(), 432);
    ASSERT_EQ(reader.GetCurrentPartitionOffset(), 12345678901LL);
    ASSERT_TRUE(reader.NextPartitionInTopic());
    ASSERT_EQ(reader.GetCurrentPartitionNumber(), 87654);
    ASSERT_EQ(reader.GetCurrentPartitionErrorCode(), 321);
    ASSERT_EQ(reader.GetCurrentPartitionOffset(), 23456789012LL);
    ASSERT_FALSE(reader.NextPartitionInTopic());

    ASSERT_TRUE(reader.FirstPartitionInTopic());

    ASSERT_EQ(reader.GetCurrentPartitionNumber(), 98765);
    ASSERT_EQ(reader.GetCurrentPartitionErrorCode(), 432);
    ASSERT_EQ(reader.GetCurrentPartitionOffset(), 12345678901LL);
    ASSERT_TRUE(reader.NextPartitionInTopic());
    ASSERT_EQ(reader.GetCurrentPartitionNumber(), 87654);
    ASSERT_EQ(reader.GetCurrentPartitionErrorCode(), 321);
    ASSERT_EQ(reader.GetCurrentPartitionOffset(), 23456789012LL);
    ASSERT_FALSE(reader.NextPartitionInTopic());

    ASSERT_TRUE(reader.NextTopic());
    topic3_copy.assign(reader.GetCurrentTopicNameBegin(),
                       reader.GetCurrentTopicNameEnd());
    ASSERT_EQ(topic3, topic3_copy);
    ASSERT_EQ(reader.GetNumPartitionsInCurrentTopic(), 0U);
    ASSERT_FALSE(reader.FirstPartitionInTopic());
    ASSERT_FALSE(reader.NextTopic());
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
