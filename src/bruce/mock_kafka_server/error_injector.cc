/* <bruce/mock_kafka_server/error_injector.cc>

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

   Implements <bruce/mock_kafka_server/error_injector.h>.
 */

#include <bruce/mock_kafka_server/error_injector.h>

#include <iostream>

#include <base/io_utils.h>
#include <bruce/mock_kafka_server/cmd.h>
#include <bruce/mock_kafka_server/serialize_cmd.h>
#include <bruce/util/connect_to_host.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MockKafkaServer;
using namespace Bruce::Util;

Bruce::MockKafkaServer::TCmd
TErrorInjector::MakeCmdAckError(int16_t error_code,
    const char *msg_body_to_match, const char *client_addr_to_match) {
  std::string msg_body, client_addr;
  int32_t param2 = 1;

  if (msg_body_to_match != nullptr) {
    msg_body = msg_body_to_match;
    param2 = 0;
  }

  if (client_addr_to_match != nullptr) {
    client_addr = client_addr_to_match;
  }

  return TCmd(TCmd::TType::SEND_PRODUCE_RESPONSE_ERROR, error_code, param2,
              std::move(msg_body), std::move(client_addr));
}

Bruce::MockKafkaServer::TCmd
TErrorInjector::MakeCmdDisconnectBeforeAck(const char *msg_body_to_match,
    const char *client_addr_to_match) {
  std::string msg_body, client_addr;
  int32_t param2 = 1;

  if (msg_body_to_match != nullptr) {
    msg_body = msg_body_to_match;
    param2 = 0;
  }

  if (client_addr_to_match != nullptr) {
    client_addr = client_addr_to_match;
  }

  return TCmd(TCmd::TType::DISCONNECT_ON_READ_PRODUCE_REQUEST, 0, param2,
              std::move(msg_body), std::move(client_addr));
}

Bruce::MockKafkaServer::TCmd
TErrorInjector::MakeCmdMetadataResponseError(int16_t error_code,
    const char *topic, const char *client_addr_to_match) {
  assert(topic);
  assert(topic[0] != '\0');
  std::string topic_copy(topic), client_addr;

  if (client_addr_to_match != nullptr) {
    client_addr = client_addr_to_match;
  }

  return TCmd(TCmd::TType::SEND_METADATA_RESPONSE_ERROR, error_code, 0,
              std::move(topic_copy), std::move(client_addr));
}

Bruce::MockKafkaServer::TCmd
TErrorInjector::MakeCmdAllTopicsMetadataResponseError(int16_t error_code,
          const char *topic, const char *client_addr_to_match) {
  std::string topic_copy(topic), client_addr;

  if (client_addr_to_match != nullptr) {
    client_addr = client_addr_to_match;
  }

  return TCmd(TCmd::TType::SEND_METADATA_RESPONSE_ERROR, error_code, 1,
              std::move(topic_copy), std::move(client_addr));
}

Bruce::MockKafkaServer::TCmd
TErrorInjector::MakeCmdDisconnectBeforeMetadataResponse(const char *topic,
    const char *client_addr_to_match) {
  assert(topic);
  std::string topic_copy(topic), client_addr;

  if (client_addr_to_match != nullptr) {
    client_addr = client_addr_to_match;
  }

  return TCmd(TCmd::TType::DISCONNECT_ON_READ_METADATA_REQUEST, 0, 0,
              std::move(topic_copy), std::move(client_addr));
}

Bruce::MockKafkaServer::TCmd
TErrorInjector::MakeCmdDisconnectBeforeAllTopicsMetadataResponse(
    const char *client_addr_to_match) {
  std::string topic, client_addr;

  if (client_addr_to_match != nullptr) {
    client_addr = client_addr_to_match;
  }

  return TCmd(TCmd::TType::DISCONNECT_ON_READ_METADATA_REQUEST, 0, 1,
              std::move(topic), std::move(client_addr));
}

bool TErrorInjector::Connect(const char *host, in_port_t port) {
  assert(this);
  assert(host);
  Disconnect();
  ConnectToHost(host, port, Sock);
  return Sock.IsOpen();
}

bool TErrorInjector::SendCmd(const TCmd &cmd) {
  assert(this);
  assert(Sock.IsOpen());
  std::vector<uint8_t> buf;
  SerializeCmd(cmd, buf);
  bool success = true;

  try {
    success = TryWriteExactly(Sock, &buf[0], buf.size());
  } catch (const TUnexpectedEnd &) {
    success = false;
  }

  if (!success) {
    std::cerr << "Lost connection to server while sending command"
        << std::endl;
    Sock.Reset();
    return false;
  }

  return true;
}

bool TErrorInjector::ReceiveAck() {
  assert(this);
  assert(Sock.IsOpen());
  uint8_t response = 0;
  bool success = true;

  try {
    success = TryReadExactly(Sock, &response, sizeof(response));
  } catch (const TUnexpectedEnd &) {
    success = false;
  }

  if (!success) {
    std::cerr << "Lost connection to server while reading response"
        << std::endl;
    Sock.Reset();
    return false;
  }

  if (response) {
    std::cerr << "Server did not understand command" << std::endl;
    return false;
  }

  return true;
}
