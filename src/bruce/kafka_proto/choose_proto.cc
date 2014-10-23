/* <bruce/kafka_proto/choose_proto.cc>

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

   Implements <bruce/kafka_proto/choose_proto.h>
 */

#include <bruce/kafka_proto/choose_proto.h>

#include <bruce/kafka_proto/v0/wire_proto.h>

using namespace Bruce;
using namespace Bruce::KafkaProto;

TWireProtocol *Bruce::KafkaProto::ChooseProto(size_t protocol_version,
    int16_t required_acks, int32_t replication_timeout,
    bool retry_on_unknown_partition) {
  if (protocol_version == 0) {
    return new V0::TWireProto(required_acks, replication_timeout,
                              retry_on_unknown_partition);
  }

  return nullptr;  // unsupported protocol version
}
