/* <bruce/mock_kafka_server/cmd.cc>

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

   Implements <bruce/mock_kafka_server/cmd.h>.
 */

#include <bruce/mock_kafka_server/cmd.h>

using namespace Bruce;
using namespace Bruce::MockKafkaServer;

TCmd::TType TCmd::ToType(uint8_t type) {
  TCmd::TType result = static_cast<TCmd::TType>(type);

  switch (result) {
    case TType::SEND_PRODUCE_RESPONSE_ERROR:
    case TType::DISCONNECT_ON_READ_PRODUCE_REQUEST:
    case TType::DELAY_BEFORE_SEND_PRODUCE_RESPONSE:
    case TType::SEND_METADATA_RESPONSE_ERROR:
    case TType::DISCONNECT_ON_READ_METADATA_REQUEST:
    case TType::DELAY_BEFORE_SEND_METADATA_RESPONSE:
      break;
    default:
      THROW_ERROR(TBadCmdType);
  }

  return result;
}
