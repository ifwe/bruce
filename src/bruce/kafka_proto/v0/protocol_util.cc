/* <bruce/kafka_proto/v0/protocol_util.cc>

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

   Implements <bruce/kafka_proto/v0/protocol_util.h>.
 */

#include <bruce/kafka_proto/v0/protocol_util.h>

#include <base/field_access.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <server/counter.h>

using namespace Bruce;
using namespace Bruce::KafkaProto::V0;

SERVER_COUNTER(BadKafkaResponseSize);

size_t Bruce::KafkaProto::V0::GetRequestOrResponseSize(
    const void *response_begin) {
  int32_t size_field = ReadInt32FromHeader(response_begin);

  if (size_field < 0) {
    BadKafkaResponseSize.Increment();
    throw TWireProtocol::TBadResponseSize();
  }

  /* The value stored in the size field does not include the size of the size
     field itself, so we add REQUEST_OR_RESPONSE_SIZE_SIZE bytes for that. */
  return static_cast<size_t>(size_field + REQUEST_OR_RESPONSE_SIZE_SIZE);
}
