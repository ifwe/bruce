/* <bruce/kafka_proto/v0/produce_request.test.cc>

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

   Unit tests for <bruce/kafka_proto/v0/produce_request_reader.h> and
   <bruce/kafka_proto/v0/produce_request_writer.h>.
 */

#include <bruce/kafka_proto/v0/produce_request_reader.h>
#include <bruce/kafka_proto/v0/produce_request_writer.h>

#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

namespace {

  /* The fixture for testing classes TProduceRequestReader and
     TProduceRequestWriter. */
  class TProduceRequestTest : public ::testing::Test {
    protected:
    TProduceRequestTest() {
    }

    virtual ~TProduceRequestTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TProduceRequestTest

  TEST_F(TProduceRequestTest, ProduceRequestTest1) {
    std::vector<uint8_t> buf;
    TProduceRequestWriter writer;
    std::string client_id("client id");
    writer.OpenRequest(buf, 1234567, client_id.data(),
        client_id.data() + client_id.size(), 3, 100);
    writer.CloseRequest();
    ASSERT_EQ(buf.size(), 33U);
    TProduceRequestReader reader;
    reader.SetRequest(&buf[0], buf.size());
    ASSERT_EQ(reader.GetCorrelationId(), 1234567);
    std::string client_id_copy(reader.GetClientIdBegin(),
                               reader.GetClientIdEnd());
    ASSERT_EQ(client_id_copy, client_id);
    ASSERT_EQ(reader.GetRequiredAcks(), 3);
    ASSERT_EQ(reader.GetReplicationTimeout(), 100);
    ASSERT_EQ(reader.GetNumTopics(), 0);
    ASSERT_EQ(reader.FirstTopic(), false);
  }

  TEST_F(TProduceRequestTest, ProduceRequestTest2) {
    std::vector<std::string> topics;
    topics.push_back("Scooby Doo");
    topics.push_back("The Flintstones");
    topics.push_back("The Ramones");
    std::vector<int32_t> partitions;
    partitions.push_back(5);
    partitions.push_back(10);
    partitions.push_back(15);
    std::vector<std::string> msgs;
    msgs.push_back("Scooby dooby doo");
    msgs.push_back("Yabba dabba doo");
    msgs.push_back("Gabba gabba hey");
    std::vector<std::string> values(msgs);

    for (size_t i = 0; i < values.size(); ++i) {
      values[i] = std::string("Value: ") + values[i];
    }

    for (size_t i = 1; i <= topics.size(); ++i) {
      {
        std::vector<uint8_t> buf;
        TProduceRequestWriter writer;
        writer.OpenRequest(buf, 1234567, nullptr, nullptr, 3, 100);

        for (size_t ii = 0; ii < i; ++ii) {
          const char *t = topics[ii].data();
          writer.OpenTopic(t, t + topics[ii].size());
          writer.CloseTopic();
        }

        writer.CloseRequest();
        TProduceRequestReader reader;
        reader.SetRequest(&buf[0], buf.size());
        ASSERT_EQ(reader.GetCorrelationId(), 1234567);
        ASSERT_TRUE(reader.GetClientIdEnd() == reader.GetClientIdBegin());
        ASSERT_EQ(reader.GetRequiredAcks(), 3);
        ASSERT_EQ(reader.GetReplicationTimeout(), 100);
        ASSERT_EQ(reader.GetNumTopics(), i);

        for (size_t ii = 0; ii < i; ++ii) {
          ASSERT_TRUE(reader.NextTopic());
          std::string s(reader.GetCurrentTopicNameBegin(),
                        reader.GetCurrentTopicNameEnd());
          ASSERT_EQ(s, topics[ii]);
          ASSERT_EQ(reader.GetNumMsgSetsInCurrentTopic(), 0U);
          ASSERT_FALSE(reader.NextMsgSetInTopic());
        }

        ASSERT_FALSE(reader.NextTopic());
      }

      for (size_t j = 1; j <= partitions.size(); ++j) {
        for (size_t k = 0; k <= msgs.size(); ++k) {
          std::vector<uint8_t> buf;
          TProduceRequestWriter writer;
          writer.OpenRequest(buf, 1234567, nullptr, nullptr, 3, 100);

          for (size_t ii = 0; ii < i; ++ii) {
            const char *t = topics[ii].data();
            writer.OpenTopic(t, t + topics[ii].size());

            for (size_t jj = 0; jj < j; ++jj) {
              writer.OpenMsgSet(partitions[jj]);

              for (size_t kk = 0; kk < k; ++kk) {
                const uint8_t *key_begin =
                    reinterpret_cast<const uint8_t *>(msgs[kk].data());
                const uint8_t *key_end = key_begin + msgs[kk].size();
                const uint8_t *value_begin =
                    reinterpret_cast<const uint8_t *>(values[kk].data());
                const uint8_t *value_end = value_begin + values[kk].size();
                writer.AddMsg(0xab, key_begin, key_end, value_begin,
                              value_end);
              }

              writer.CloseMsgSet();
            }

            writer.CloseTopic();
          }

          writer.CloseRequest();
          TProduceRequestReader reader;
          reader.SetRequest(&buf[0], buf.size());
          ASSERT_EQ(reader.GetCorrelationId(), 1234567);
          ASSERT_TRUE(reader.GetClientIdEnd() == reader.GetClientIdBegin());
          ASSERT_EQ(reader.GetRequiredAcks(), 3);
          ASSERT_EQ(reader.GetReplicationTimeout(), 100);
          ASSERT_EQ(reader.GetNumTopics(), i);

          for (size_t ii = 0; ii < i; ++ii) {
            ASSERT_TRUE(reader.NextTopic());
            std::string t(reader.GetCurrentTopicNameBegin(),
                          reader.GetCurrentTopicNameEnd());
            ASSERT_EQ(t, topics[ii]);
            ASSERT_EQ(reader.GetNumMsgSetsInCurrentTopic(), j);

            for (size_t jj = 0; jj < j; ++jj) {
              ASSERT_TRUE(reader.NextMsgSetInTopic());
              ASSERT_EQ(reader.GetPartitionOfCurrentMsgSet(), partitions[jj]);

              for (size_t kk = 0; kk < k; ++kk) {
                ASSERT_TRUE(reader.NextMsgInMsgSet());
                ASSERT_TRUE(reader.CurrentMsgCrcIsOk());
                ASSERT_EQ(reader.GetCurrentMsgAttributes(), 0xab);
                std::string key(reader.GetCurrentMsgKeyBegin(),
                                reader.GetCurrentMsgKeyEnd());
                std::string value(reader.GetCurrentMsgValueBegin(),
                                  reader.GetCurrentMsgValueEnd());
                ASSERT_EQ(key, msgs[kk]);
                ASSERT_EQ(value, values[kk]);
              }

              ASSERT_FALSE(reader.NextMsgInMsgSet());
            }

            ASSERT_FALSE(reader.NextMsgSetInTopic());
          }

          ASSERT_FALSE(reader.NextTopic());
        }
      }
    }
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
