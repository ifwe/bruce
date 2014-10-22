/* <bruce/kafka_proto/v0/metadata_request_reader.cc>

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

   Implements <bruce/kafka_proto/v0/metadata_request_reader.h>.
 */

#include <bruce/kafka_proto/v0/metadata_request_reader.h>

using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

static inline const uint8_t *AssignBuf(const void *buf) {
  assert(buf);
  return reinterpret_cast<const uint8_t *>(buf);
}

TMetadataRequestReader::TMetadataRequestReader(const void *request,
    size_t request_size)
    : Begin(AssignBuf(request)),
      End(Begin + RequestSize(Begin)),
      AllTopics(false) {
  assert((Begin + request_size) == End);

  if (request_size < MinSize()) {
    THROW_ERROR(TBadRequestSize);
  }

  /* Check API key. */
  if (ReadInt16FromHeader(Begin + THdr::API_KEY_OFFSET) != THdr::API_KEY) {
    THROW_ERROR(TWrongRequestType);
  }

  /* Check API version. */
  if (ReadInt16FromHeader(Begin + THdr::API_VERSION_OFFSET) !=
      THdr::API_VERSION) {
    THROW_ERROR(TBadApiVersion);
  }

  /* We always send an empty client ID.  A value of -1 indicates an empty
     string. */
  if (ReadInt16FromHeader(Begin + THdr::CLIENT_ID_LENGTH_OFFSET) !=
      THdr::EMPTY_STRING_LENGTH) {
    THROW_ERROR(TBadClientIdLength);
  }

  int32_t topic_count =
      ReadInt32FromHeader(Begin + THdr::TOPIC_COUNT_OFFSET);

  /* We only send metadata requests for a single topic or all topics. */
  if ((topic_count != 0) && (topic_count != 1)) {
    THROW_ERROR(TBadTopicCount);
  }

  AllTopics = (topic_count == 0);

  if (AllTopics) {
    if (request_size != MinSize()) {
      THROW_ERROR(TBadRequestSize);
    }
  } else {
    if (request_size < SingleTopicHeaderSize()) {
      THROW_ERROR(TBadRequestSize);
    }

    int16_t topic_size = ReadInt16FromHeader(
        Begin + THdr::TOPIC_NAME_LENGTH_OFFSET);

    /* We assume that the empty string is not a valid topic name. */
    if (topic_size < 1) {
      THROW_ERROR(TBadTopicSize);
    }

    size_t computed_request_size = topic_size + SingleTopicHeaderSize();

    if (computed_request_size != request_size) {
      THROW_ERROR(TBadRequestSize);
    }
  }
}

const char *TMetadataRequestReader::GetTopicBegin() const {
  assert(this);
  return AllTopics ?
      nullptr :
      reinterpret_cast<const char *>(Begin + SingleTopicHeaderSize());
}

const char *TMetadataRequestReader::GetTopicEnd() const {
  assert(this);
  return AllTopics ?
      nullptr :
      reinterpret_cast<const char *>(Begin + SingleTopicHeaderSize() +
          ReadInt16FromHeader(Begin + THdr::TOPIC_NAME_LENGTH_OFFSET));
}
