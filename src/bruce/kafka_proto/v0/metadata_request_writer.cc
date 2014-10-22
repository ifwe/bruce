/* <bruce/kafka_proto/v0/metadata_request_writer.cc>

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

   Implements <bruce/kafka_proto/v0/metadata_request_writer.h>.
 */

#include <bruce/kafka_proto/v0/metadata_request_writer.h>

#include <cassert>
#include <cstring>
#include <limits>

#include <base/field_access.h>

using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

void TMetadataRequestWriter::WriteSingleTopicRequest(struct iovec &header_iov,
    struct iovec &body_iov, void *header_buf, const char *topic_begin,
    const char *topic_end, int32_t correlation_id) {
  assert(this);
  assert(header_buf);
  assert(topic_begin);
  assert(topic_end > topic_begin);
  header_iov.iov_base = header_buf;
  header_iov.iov_len = NUM_SINGLE_TOPIC_HEADER_BYTES;
  body_iov.iov_base = const_cast<char *>(topic_begin);
  size_t topic_size = topic_end - topic_begin;
  body_iov.iov_len = topic_size;
  WriteHeader(header_buf, topic_size, correlation_id);
}

void TMetadataRequestWriter::WriteSingleTopicRequest(
    std::vector<uint8_t> &result, const char *topic_begin,
    const char *topic_end, int32_t correlation_id) {
  assert(this);
  assert(topic_begin);
  assert(topic_end > topic_begin);
  size_t topic_size = topic_end - topic_begin;
  result.resize(NUM_SINGLE_TOPIC_HEADER_BYTES + topic_size);
  WriteHeader(&result[0], topic_size, correlation_id);
  std::memcpy(&result[NUM_SINGLE_TOPIC_HEADER_BYTES], topic_begin, topic_size);
}

void TMetadataRequestWriter::WriteAllTopicsRequest(struct iovec &iov,
    void *header_buf, int32_t correlation_id) {
  assert(this);
  assert(header_buf);
  iov.iov_base = header_buf;
  iov.iov_len = NUM_ALL_TOPICS_HEADER_BYTES;
  WriteHeader(header_buf, 0, correlation_id);
}

void TMetadataRequestWriter::WriteAllTopicsRequest(
    std::vector<uint8_t> &result, int32_t correlation_id) {
  assert(this);
  result.resize(NUM_ALL_TOPICS_HEADER_BYTES);
  WriteHeader(&result[0], 0, correlation_id);
}

void TMetadataRequestWriter::WriteHeader(void *header_buf, size_t topic_size,
    int32_t correlation_id) {
  assert(this);
  assert(header_buf);
  assert(topic_size <=
         static_cast<size_t>(std::numeric_limits<int16_t>::max()));
  uint8_t *buf = reinterpret_cast<uint8_t *>(header_buf);

  /* A value of 0 for topic_size indicates an all topics request, which has a
     shorter header due to the absence of the topic name length field (since
     there are no topic names). */
  int32_t size_field = topic_size ?
      (static_cast<int32_t>(topic_size) + NUM_SINGLE_TOPIC_HEADER_BYTES -
       THdr::REQUEST_SIZE_SIZE) :
      (NUM_ALL_TOPICS_HEADER_BYTES - THdr::REQUEST_SIZE_SIZE);

  WriteInt32ToHeader(buf, size_field);
  WriteInt16ToHeader(buf + THdr::API_KEY_OFFSET, THdr::API_KEY);
  WriteInt16ToHeader(buf + THdr::API_VERSION_OFFSET, THdr::API_VERSION);
  WriteInt32ToHeader(buf + THdr::CORRELATION_ID_OFFSET, correlation_id);
  WriteInt16ToHeader(buf + THdr::CLIENT_ID_LENGTH_OFFSET,
                     THdr::EMPTY_STRING_LENGTH);
  int32_t topic_count = 0;

  if (topic_size) {
    topic_count = 1;
    WriteInt16ToHeader(buf + THdr::TOPIC_NAME_LENGTH_OFFSET,
                       static_cast<int16_t>(topic_size));
  }

  WriteInt32ToHeader(buf + THdr::TOPIC_COUNT_OFFSET, topic_count);
}
