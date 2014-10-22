/* <bruce/kafka_proto/v0/produce_response_constants.h>

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

   Constants related to Kafka protocol version 0 produce responses.
 */

#pragma once

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TProduceResponseConstants {
        public:
        enum { CORRELATION_ID_SIZE = 4 };

        enum { TOPIC_COUNT_SIZE = 4 };

        enum { TOPIC_NAME_LEN_SIZE = 2 };

        enum { PARTITION_COUNT_SIZE = 4 };

        enum { PARTITION_SIZE = 4 };

        enum { ERROR_CODE_SIZE = 2 };

        enum { OFFSET_SIZE = 8 };
      };  // TProduceResponseConstants

    }  // V0

  }  // KafkaProto

}  // Bruce
