/* <bruce/kafka_proto/v0/metadata_request_fields.h>

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

   Constants specifying sizes and offsets of fields in metadata requests.
 */

#pragma once

#include <cstddef>
#include <cstdint>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TMetadataRequestFields final {
        public:
        static const size_t REQUEST_SIZE_SIZE = 4;

        static const size_t API_KEY_SIZE = 2;

        static const size_t API_VERSION_SIZE = 2;

        static const size_t CORRELATION_ID_SIZE = 4;

        static const size_t CLIENT_ID_LENGTH_SIZE = 2;

        static const size_t TOPIC_COUNT_SIZE = 4;

        static const size_t TOPIC_NAME_LENGTH_SIZE = 2;

        static const size_t REQUEST_SIZE_OFFSET = 0;

        static const size_t API_KEY_OFFSET =
            REQUEST_SIZE_OFFSET + REQUEST_SIZE_SIZE;

        static const size_t API_VERSION_OFFSET = API_KEY_OFFSET + API_KEY_SIZE;

        static const size_t CORRELATION_ID_OFFSET =
            API_VERSION_OFFSET + API_VERSION_SIZE;

        static const size_t CLIENT_ID_LENGTH_OFFSET =
            CORRELATION_ID_OFFSET + CORRELATION_ID_SIZE;

        static const size_t TOPIC_COUNT_OFFSET =
            CLIENT_ID_LENGTH_OFFSET + CLIENT_ID_LENGTH_SIZE;

        static const size_t TOPIC_NAME_LENGTH_OFFSET =
            TOPIC_COUNT_OFFSET + TOPIC_COUNT_SIZE;

        static const int16_t API_KEY = 3;

        static const int16_t API_VERSION = 0;

        static const int16_t EMPTY_STRING_LENGTH = -1;
      };  // TMetadataRequestFields

    }  // V0

  }  // KafkaProto

}  // Bruce
