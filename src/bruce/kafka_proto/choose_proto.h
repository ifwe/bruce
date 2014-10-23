/* <bruce/kafka_proto/choose_proto.h>

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

   Factory function for choosing Kafka wire protocol.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include <bruce/kafka_proto/wire_protocol.h>

namespace Bruce {

  namespace KafkaProto {

    TWireProtocol *ChooseProto(size_t protocol_version, int16_t required_acks,
        int32_t replication_timeout, bool retry_on_unknown_partition);

  }  // KafkaProto

}  // Bruce
