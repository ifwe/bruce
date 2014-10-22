/* <bruce/kafka_proto/v0/metadata_request.test.cc>

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

   Unit tests for <bruce/kafka_proto/v0/metadata_request_reader.h> and
   <bruce/kafka_proto/v0/metadata_request_writer.h>
 */

#include <bruce/kafka_proto/v0/metadata_request_reader.h>
#include <bruce/kafka_proto/v0/metadata_request_writer.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

#include <base/opt.h>

#include <gtest/gtest.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

namespace {

  /* The fixture for testing classes TMetadataRequestReader and
     TMetadataRequestWriter. */
  class TMetadataRequestTest : public ::testing::Test {
    protected:
    TMetadataRequestTest() {
    }

    virtual ~TMetadataRequestTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TMetadataRequestTest

  void MergeIovecs(const struct iovec &iov1, const struct iovec &iov2,
      std::vector<uint8_t> &result) {
    result.resize(iov1.iov_len + iov2.iov_len);

    if (iov1.iov_len) {
      std::memcpy(&result[0], iov1.iov_base, iov1.iov_len);
    }

    if (iov2.iov_len) {
      std::memcpy(&result[iov1.iov_len], iov2.iov_base, iov2.iov_len);
    }
  }

  TEST_F(TMetadataRequestTest, SingleTopicTest) {
    struct iovec iov[2];
    std::vector<uint8_t> header_buf(
        TMetadataRequestWriter::NumSingleTopicHeaderBytes() + 1);
    std::fill(&header_buf[0], &header_buf[0] + header_buf.size(), 'x');
    const std::string topic("this is a topic");
    TMetadataRequestWriter().WriteSingleTopicRequest(iov[0], iov[1],
        &header_buf[0], topic.data(), topic.data() + topic.size(), 12345);
    ASSERT_EQ(header_buf[header_buf.size() - 1], 'x');

    std::vector<uint8_t> merged_buf;
    MergeIovecs(iov[0], iov[1], merged_buf);

    TOpt<TMetadataRequestReader> reader;
    bool threw = false;

    try {
      reader.MakeKnown(&merged_buf[0], merged_buf.size());
    } catch (const std::runtime_error &) {
      threw = true;
    }

    ASSERT_FALSE(threw);
    ASSERT_EQ(TMetadataRequestReader::RequestSize(&merged_buf[0]),
              merged_buf.size());
    ASSERT_EQ(reader->GetCorrelationId(), 12345);
    ASSERT_FALSE(reader->IsAllTopics());
    const char *topic_begin = reader->GetTopicBegin();
    ASSERT_TRUE(topic_begin != nullptr);
    const char *topic_end = reader->GetTopicEnd();
    ASSERT_TRUE(topic_end != nullptr);
    ASSERT_TRUE(topic_end >= topic_begin);
    std::string topic_copy(topic_begin, topic_end);
    ASSERT_EQ(topic, topic_copy);
  }

  TEST_F(TMetadataRequestTest, AllTopicsTest) {
    struct iovec iov;
    std::vector<uint8_t> header_buf(
        TMetadataRequestWriter::NumAllTopicsHeaderBytes() + 1);
    std::fill(&header_buf[0], &header_buf[0] + header_buf.size(), 'x');
    TMetadataRequestWriter().WriteAllTopicsRequest(iov, &header_buf[0], 12345);
    ASSERT_EQ(header_buf[header_buf.size() - 1], 'x');
    TOpt<TMetadataRequestReader> reader;
    bool threw = false;

    try {
      reader.MakeKnown(iov.iov_base, iov.iov_len);
    } catch (const std::runtime_error &) {
      threw = true;
    }

    ASSERT_FALSE(threw);
    ASSERT_EQ(TMetadataRequestReader::RequestSize(iov.iov_base), iov.iov_len);
    ASSERT_EQ(reader->GetCorrelationId(), 12345);
    ASSERT_TRUE(reader->IsAllTopics());
    ASSERT_TRUE(reader->GetTopicBegin() == nullptr);
    ASSERT_TRUE(reader->GetTopicEnd() == nullptr);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
