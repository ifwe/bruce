/* <bruce/kafka_proto/v0/metadata_request_reader.h>

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

   Helper class for reading a metadata request.  Used for testing.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

#include <base/field_access.h>
#include <base/thrower.h>
#include <bruce/kafka_proto/v0/metadata_request_fields.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      /* Class for reading a metadata request.  To keep things simple, this
         class only supports two kinds of metadata requests:

             1.  A request for a single topic.
             2.  A request for all topics, which is specified by leaving the
                 topic list empty. */
      class TMetadataRequestReader final {
        using THdr = TMetadataRequestFields;

        public:
        DEFINE_ERROR(TBadRequestSize, std::runtime_error,
                     "Invalid Kafka metadata request size");

        DEFINE_ERROR(TBadTopicCount, std::runtime_error,
                     "Invalid topic count in Kafka metadata request");

        DEFINE_ERROR(TWrongRequestType, std::runtime_error,
                     "Expected Kafka metadata request but got some other "
                     "request type");

        DEFINE_ERROR(TBadApiVersion, std::runtime_error,
                     "Unsupported API version in Kafka metadata request");

        DEFINE_ERROR(TBadClientIdLength, std::runtime_error,
                     "Bad client ID length in Kafka metadata request");

        DEFINE_ERROR(TBadTopicSize, std::runtime_error,
                     "Invalid topic size in Kafka metadata request");

        DEFINE_ERROR(TRequestTruncated, std::runtime_error,
                     "Kafka metadata request is truncated");

        /* Return the size of the smallest possible request, which is a request
           for all topics (empty topic list). */
        static size_t MinSize() {
          return THdr::TOPIC_NAME_LENGTH_OFFSET;
        }

        /* Return the header size for a single topic request. */
        static size_t SingleTopicHeaderSize() {
          return THdr::TOPIC_NAME_LENGTH_OFFSET + THdr::TOPIC_NAME_LENGTH_SIZE;
        }

        static size_t BytesNeededToGetRequestSize() {
          /* The first field contains the request size. */
          return THdr::REQUEST_SIZE_SIZE;
        }

        /* 'request_begin' points to the start of a metadata request, which may
           not yet be completely received.  Only the first 4 bytes must be
           present.  Returns the size of the entire request, based on the size
           field at the start of the header. */
        static size_t RequestSize(const void *request_begin) {
          int32_t size_field = ReadInt32FromHeader(request_begin);

          if (size_field < 0) {
            THROW_ERROR(TBadRequestSize);
          }

          /* We add 4 because the value from the size field does not include
             the size of the size field itself. */
          return static_cast<size_t>(size_field + THdr::REQUEST_SIZE_SIZE);
        }

        /* Construct a reader for a metadata request of size 'request_size'
           starting at 'request'.  The exact request size may be obtained via
           static methods BytesNeededToGetRequestSize() and RequestSize()
           above.
         */
        TMetadataRequestReader(const void *request, size_t request_size);

        /* Returns the correlation ID. */
        int32_t GetCorrelationId() const {
          assert(this);
          return ReadInt32FromHeader(Begin + THdr::CORRELATION_ID_OFFSET);
        }

        /* Return true if this is an all topics request.  Else return false. */
        bool IsAllTopics() const {
          assert(this);
          return AllTopics;
        }

        /* Returns a pointer to the first byte of the topic, or null if this is
           an all topics request. */
        const char * GetTopicBegin() const;

        /* Returns a pointer one byte past the last byte of the topic, or null
           if this is an all topics request. */
        const char * GetTopicEnd() const;

        private:
        const uint8_t *Begin;

        const uint8_t *End;

        bool AllTopics;
      };  // TMetadataRequestReader

    }  // V0

  }  // KafkaProto

}  // Bruce
