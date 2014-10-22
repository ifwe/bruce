/* <bruce/mock_kafka_server/serialize_cmd.cc>

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

   Implements <bruce/mock_kafka_server/serialize_cmd.h>.
 */

#include <bruce/mock_kafka_server/serialize_cmd.h>

#include <cstring>

using namespace Bruce;
using namespace Bruce::MockKafkaServer;

/* The wire format for a command looks like this:

       |A |B|C   |D   |E|F         |G|H         |
       |__|_|____|____|_|__________|_|__________|
        2  1 4    4    1 N          1 N

       A: The length in bytes of the entire command (including this length
          field).  Value is in network byte order.

       B: Command identifier (1 byte): One of the values defined in
          TCmd::TType.

       C: Param1 (4 bytes): A 32 bit unsigned integer parameter in network byte
          order.

       D: Param2 (4 bytes): A 32 bit unsigned integer parameter in network byte
          order.

       E: Name length (1 byte): The length in bytes of the name string that
          immediately follows.  Max value is 255.

       F: Name string (N bytes): For commands related to produce requests, this
          is a message string to match exactly.  For commands related to
          metadata requests, this is a topic string to match exactly.

       G: Client address string length (1 byte): The length in bytes of the
          client address string that immediately follows.  Max value is 255.

       H: Client address string (N bytes): A string representation of a
          client's IP address (for instance, "127.0.0.1").  If nonempty, the
          command is specific to the given client.  If empty, the command is
          for any client.

   The response is a single byte: 0 indicates "command received successfully"
   and 1 indicates "command invalid".
 */

void Bruce::MockKafkaServer::SerializeCmd(
    const TCmd &cmd, std::vector<uint8_t> &buf) {
  size_t size = 13 + cmd.Str.size() + cmd.ClientAddr.size();
  buf.resize(size);
  WriteUint16ToHeader(&buf[0], size);
  buf[2] = static_cast<uint8_t>(cmd.Type);
  WriteInt32ToHeader(&buf[3], cmd.Param1);
  WriteInt32ToHeader(&buf[7], cmd.Param2);
  uint8_t length_byte = cmd.Str.size();
  assert(static_cast<size_t>(length_byte) == cmd.Str.size());
  buf[11] = length_byte;
  std::memcpy(&buf[12], cmd.Str.data(), cmd.Str.size());
  size_t index = 12 + cmd.Str.size();
  length_byte = cmd.ClientAddr.size();
  buf[index] = length_byte;

  if (cmd.ClientAddr.size()) {
    std::memcpy(&buf[index + 1], cmd.ClientAddr.data(), cmd.ClientAddr.size());
  }
}

bool Bruce::MockKafkaServer::DeserializeCmd(const std::vector<uint8_t> &buf,
    TCmd &result) {
  if (buf.size() < BytesNeededToGetCmdSize()) {
    return false;
  }

  size_t cmd_size = GetCmdSize(buf);

  if (buf.size() < cmd_size) {
    return false;
  }

  try {
    result.Type = TCmd::ToType(buf[2]);
  } catch (const TCmd::TBadCmdType &) {
    return false;
  }

  result.Param1 = ReadInt32FromHeader(&buf[3]);
  result.Param2 = ReadInt32FromHeader(&buf[7]);
  size_t len = buf[11];
  size_t offset = 12 + len;
  result.Str.assign(&buf[12], &buf[offset]);
  size_t len2 = buf[offset];
  ++offset;
  result.ClientAddr.clear();
  assert(buf.size() >= offset);
  assert((buf.size() - offset) >= len2);

  if (len2) {
    result.ClientAddr.assign(&buf[offset], &buf[offset] + len2);
  }

  return true;
}
