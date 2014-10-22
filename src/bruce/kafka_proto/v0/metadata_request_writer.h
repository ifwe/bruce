/* <bruce/kafka_proto/v0/metadata_request_writer.h>

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

   Helper class for adding a metadata request to an iovec structure (for gather
   write operations).
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include <sys/uio.h>

#include <base/no_copy_semantics.h>
#include <bruce/kafka_proto/v0/metadata_request_fields.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      /* To keep things simple, this class only supports two kinds of metadata
         requests:

             1.  A request for a single topic.
             2.  A request for all topics, which is specified by leaving the
                 topic list empty.
       */
      class TMetadataRequestWriter final {
        NO_COPY_SEMANTICS(TMetadataRequestWriter);

        using THdr = TMetadataRequestFields;

        public:
        static size_t NumSingleTopicHeaderBytes() {
          return NUM_SINGLE_TOPIC_HEADER_BYTES;
        }

        static size_t NumAllTopicsHeaderBytes() {
          return NUM_ALL_TOPICS_HEADER_BYTES;
        }

        TMetadataRequestWriter() = default;

        /* A single topic request requires 2 iovec structures, given by
           parameters 'header_iov' and 'body_iov'.  'body_iov' must immediately
           follow 'header_iov' in the caller's iovec array.  'header_buf'
           points to a caller-supplied buffer with space for the number of
           bytes returned by static method NumSingleTopicHeaderBytes() above,
           and will be referenced by 'header_iov' on return.  'topic_begin'
           points to the start of a caller-supplied topic, and 'topic_end'
           points one byte past the last topic byte.  'correlation_id' gives
           the Kafka correlation ID to use for the request.

           TODO: Get rid of iovec stuff.
         */
        void WriteSingleTopicRequest(struct iovec &header_iov,
            struct iovec &body_iov, void *header_buf, const char *topic_begin,
            const char *topic_end, int32_t correlation_id);

        void WriteSingleTopicRequest(std::vector<uint8_t> &result,
            const char *topic_begin, const char *topic_end,
            int32_t correlation_id);

        /* An all topics request requires 1 iovec structure, given by parameter
           'iov'.  'header_buf' points to a caller-supplied buffer with space
           for the number of bytes returned by static method
           NumAllTopicsHeaderBytes() above, and will be referenced by 'iov' on
           return.  'correlation_id' gives the Kafka correlation ID to use for
           the request. */
        void WriteAllTopicsRequest(struct iovec &iov, void *header_buf,
            int32_t correlation_id);

        /* Write an all topics request with the given correlation ID to
           'result'.  Resize 'result' to the size of the written request. */
        void WriteAllTopicsRequest(std::vector<uint8_t> &result,
            int32_t correlation_id);

        private:
        static const size_t NUM_SINGLE_TOPIC_HEADER_BYTES =
            THdr::TOPIC_NAME_LENGTH_OFFSET + THdr::TOPIC_NAME_LENGTH_SIZE;

        static const size_t NUM_ALL_TOPICS_HEADER_BYTES =
            THdr::TOPIC_NAME_LENGTH_OFFSET;

        void WriteHeader(void *header_buf, size_t topic_size,
                         int32_t correlation_id);
      };  // TMetadataRequestWriter

    }  // V0

  }  // KafkaProto

}  // Bruce
