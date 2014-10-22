/* <bruce/kafka_proto/v0/produce_response_writer.cc>

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

   Implements <bruce/kafka_proto/v0/produce_response_writer.h>.
 */

#include <bruce/kafka_proto/v0/produce_response_writer.h>

#include <cassert>
#include <cstring>
#include <limits>

#include <base/field_access.h>

using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

TProduceResponseWriter::TProduceResponseWriter() {
  Reset();
}

void TProduceResponseWriter::Reset() {
  assert(this);
  OutBuf = nullptr;
  TopicStarted = false;
  CurrentTopicOffset = 0;
  CurrentTopicIndex = 0;
  PartitionCountOffset = 0;
  CurrentPartitionOffset = 0;
  CurrentPartitionIndex = 0;
}

void TProduceResponseWriter::OpenResponse(std::vector<uint8_t> &out,
    int32_t correlation_id) {
  assert(this);

  /* Make sure we start in a sane state. */
  Reset();

  assert(&out);
  assert(OutBuf == nullptr);
  assert(!TopicStarted);
  OutBuf = &out;
  CurrentTopicOffset = REQUEST_OR_RESPONSE_SIZE_SIZE + CORRELATION_ID_SIZE +
                       TOPIC_COUNT_SIZE;
  out.resize(CurrentTopicOffset);
  WriteInt32ToHeader(&out[REQUEST_OR_RESPONSE_SIZE_SIZE], correlation_id);
}

void TProduceResponseWriter::OpenTopic(const char *topic_begin,
    const char *topic_end) {
  assert(this);
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(OutBuf);
  assert(!TopicStarted);
  assert(CurrentTopicOffset >= REQUEST_OR_RESPONSE_SIZE_SIZE +
         CORRELATION_ID_SIZE + TOPIC_COUNT_SIZE);
  assert((topic_end - topic_begin) <=
         static_cast<ptrdiff_t>(std::numeric_limits<int16_t>::max()));
  std::vector<uint8_t> &out = *OutBuf;
  int16_t topic_len = topic_end - topic_begin;
  out.resize(out.size() + TOPIC_NAME_LENGTH_SIZE + topic_len +
             PARTITION_COUNT_SIZE);
  WriteInt16ToHeader(&out[CurrentTopicOffset], topic_len ? topic_len : -1);
  std::memcpy(&out[CurrentTopicOffset + TOPIC_NAME_LENGTH_SIZE], topic_begin,
              topic_len);
  PartitionCountOffset = CurrentTopicOffset + TOPIC_NAME_LENGTH_SIZE +
                         topic_len;
  CurrentPartitionOffset = PartitionCountOffset + PARTITION_COUNT_SIZE;
  CurrentPartitionIndex = 0;
  TopicStarted = true;
}

void TProduceResponseWriter::AddPartition(int32_t partition,
    int16_t error_code, int64_t offset) {
  assert(this);
  assert(partition >= 0);
  assert(offset >= 0);
  assert(OutBuf);
  assert(TopicStarted);
  assert(CurrentTopicOffset >= REQUEST_OR_RESPONSE_SIZE_SIZE +
         CORRELATION_ID_SIZE + TOPIC_COUNT_SIZE);
  assert(CurrentPartitionOffset > CurrentTopicOffset);
  std::vector<uint8_t> &out = *OutBuf;
  out.resize(out.size() + BYTES_PER_PARTITION);
  WriteInt32ToHeader(&out[CurrentPartitionOffset], partition);
  WriteInt16ToHeader(&out[CurrentPartitionOffset + PARTITION_SIZE],
                     error_code);
  WriteInt64ToHeader(&out[CurrentPartitionOffset + PARTITION_SIZE +
                          ERROR_CODE_SIZE], offset);
  CurrentPartitionOffset += BYTES_PER_PARTITION;
  ++CurrentPartitionIndex;
}

void TProduceResponseWriter::CloseTopic() {
  assert(this);
  assert(OutBuf);
  assert(TopicStarted);
  assert(CurrentTopicOffset >= REQUEST_OR_RESPONSE_SIZE_SIZE +
         CORRELATION_ID_SIZE + TOPIC_COUNT_SIZE);
  assert(PartitionCountOffset > CurrentTopicOffset);
  assert(CurrentPartitionOffset >=
         PartitionCountOffset + PARTITION_COUNT_SIZE);
  std::vector<uint8_t> &out = *OutBuf;
  WriteInt32ToHeader(&out[PartitionCountOffset], CurrentPartitionIndex);
  CurrentTopicOffset = CurrentPartitionOffset;
  ++CurrentTopicIndex;
  PartitionCountOffset = 0;
  CurrentPartitionOffset = 0;
  CurrentPartitionIndex = 0;
  TopicStarted = false;
}

void TProduceResponseWriter::CloseResponse() {
  assert(this);
  assert(OutBuf);
  assert(!TopicStarted);
  assert(CurrentTopicOffset >= REQUEST_OR_RESPONSE_SIZE_SIZE +
         CORRELATION_ID_SIZE + TOPIC_COUNT_SIZE);
  std::vector<uint8_t> &out = *OutBuf;
  WriteInt32ToHeader(&out[REQUEST_OR_RESPONSE_SIZE_SIZE + CORRELATION_ID_SIZE],
                     CurrentTopicIndex);
  assert(out.size() > REQUEST_OR_RESPONSE_SIZE_SIZE);
  WriteInt32ToHeader(&out[0], out.size() - REQUEST_OR_RESPONSE_SIZE_SIZE);
  TopicStarted = false;
  CurrentTopicOffset = 0;
  CurrentTopicIndex = 0;
  PartitionCountOffset = 0;
  CurrentPartitionOffset = 0;
  CurrentPartitionIndex = 0;
  OutBuf = nullptr;
}
