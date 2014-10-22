/* <bruce/mock_kafka_server/v0_client_handler.cc>

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

   Implements <bruce/mock_kafka_server/v0_client_handler.h>.
 */

#include <bruce/mock_kafka_server/v0_client_handler.h>

#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>

#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <base/crc.h>
#include <base/debug_log.h>
#include <base/error_utils.h>
#include <base/field_access.h>
#include <base/io_utils.h>
#include <base/no_default_case.h>
#include <socket/address.h>

using namespace Base;
using namespace Socket;
using namespace Bruce;
using namespace Bruce::KafkaProto;
using namespace Bruce::KafkaProto::V0;
using namespace Bruce::MockKafkaServer;

TV0ClientHandler::~TV0ClientHandler() noexcept {
  /* This will shut down the thread if something unexpected happens. */
  ShutdownOnDestroy();
}

TProduceRequestReaderApi &TV0ClientHandler::GetProduceRequestReader() {
  assert(this);
  return ProduceRequestReader;
}

TMsgSetReaderApi &TV0ClientHandler::GetMsgSetReader() {
  assert(this);
  return MsgSetReader;
}

TProduceResponseWriterApi &TV0ClientHandler::GetProduceResponseWriter() {
  assert(this);
  return ProduceResponseWriter;
}

bool TV0ClientHandler::ValidateMetadataRequestHeader() {
  assert(this);

  try {
    OptMetadataRequestReader.MakeKnown(&InputBuf[0], InputBuf.size());
  } catch (const std::runtime_error &x) {
    Out << x.what() << std::endl;
    return false;
  }

  return true;
}

bool TV0ClientHandler::ValidateMetadataRequest(
          TMetadataRequest &request) {
  assert(OptMetadataRequestReader.IsKnown());
  request.CorrelationId = OptMetadataRequestReader->GetCorrelationId();

  if (OptMetadataRequestReader->IsAllTopics()) {
    request.Topic.clear();
  } else {
    request.Topic.assign(OptMetadataRequestReader->GetTopicBegin(),
                         OptMetadataRequestReader->GetTopicEnd());
  }

  return true;
}

TSingleClientHandlerBase::TSendMetadataResult
TV0ClientHandler::SendMetadataResponse(const TMetadataRequest &request,
    int16_t error, const std::string &error_topic, size_t delay) {
  char host_name[1024];
  IfLt0(gethostname(host_name, sizeof(host_name)));
  size_t host_name_len = std::strlen(host_name);
  const char *host_name_end = &host_name[host_name_len];
  TMetadataResponseWriter writer;
  writer.OpenResponse(MdResponseBuf, request.CorrelationId);
  writer.OpenBrokerList();

  for (size_t node_id = 0; node_id < Setup.Ports.size(); ++node_id) {
    /* Translate port from virtual to physical before writing port to metadata
       response.  See big comment in <bruce/mock_kafka_server/port_map.h> for
       an explanation of what is going on here. */
    in_port_t phys_port = PortMap->VirtualPortToPhys(Setup.BasePort + node_id);
    assert(phys_port);

    writer.AddBroker(node_id, host_name, host_name_end, phys_port);
  }

  writer.CloseBrokerList();
  writer.OpenTopicList();
  std::string topic;
  TAction action = TAction::Respond;
  int16_t code = 0;
  std::string topic_for_code;

  if (request.Topic.empty()) {
    for (auto iter = Setup.Topics.begin();
         iter != Setup.Topics.end();
         ++iter) {
      const std::string &name = iter->first;
      const char *topic_begin = name.data();
      const char *topic_end = topic_begin + name.size();
      topic.assign(topic_begin, topic_end);
      int16_t error_to_inject = 0;

      if (topic == error_topic) {
        error_to_inject = error;
        topic_for_code = error_topic;
        code = error;
      } else {
        error_to_inject = 0;
      }

      WriteSingleTopic(writer, iter->second, topic_begin, topic_end,
                       error_to_inject);
    }
  } else {
    const char *topic_begin = request.Topic.data();
    const char *topic_end = topic_begin + request.Topic.size();
    topic.assign(topic_begin, topic_end);

    int16_t error_to_inject = 0;

    if (topic == error_topic) {
      error_to_inject = error;
      topic_for_code = error_topic;
      code = error;
    } else {
      error_to_inject = 0;
    }

    auto iter = Setup.Topics.find(request.Topic);

    if (iter == Setup.Topics.end()) {
      code = 3;
      topic_for_code = request.Topic;
      action = TAction::RejectBadDest;
      writer.OpenTopic(3, topic_begin, topic_end);
      writer.CloseTopic();
    } else {
      WriteSingleTopic(writer, iter->second, topic_begin, topic_end,
                       error_to_inject);
    }
  }

  writer.CloseTopicList();
  writer.CloseResponse();
  PrintMdReq(GetMetadataRequestCount(), request, action, topic_for_code, code,
             delay);
  OptMetadataRequestReader.Reset();

  switch (TryWriteExactlyOrShutdown(ClientSocket, &MdResponseBuf[0],
                                    MdResponseBuf.size())) {
    case TIoResult::Success: {
      break;
    }
    case TIoResult::Disconnected: {
      Out << "Error: Got disconnected from client while sending metadata "
             "response"
          << std::endl;
      return TSendMetadataResult::ClientDisconnected;
    }
    case TIoResult::UnexpectedEnd:
    case TIoResult::EmptyReadUnexpectedEnd: {
      Out << "Error: Got disconnected unexpectedly from client while sending "
          << "metadata response" << std::endl;
      return TSendMetadataResult::ClientDisconnected;
    }
    case TIoResult::GotShutdownRequest: {
      Out << "Info: Got shutdown request while sending metadata response"
          << std::endl;
      return TSendMetadataResult::GotShutdownRequest;
    }
    NO_DEFAULT_CASE;
  }

  return TSendMetadataResult::SentMetadata;
}

void TV0ClientHandler::WriteSingleTopic(TMetadataResponseWriter &writer,
    const TSetup::TTopic &topic, const char *name_begin,
    const char *name_end, int16_t error) {
  writer.OpenTopic(error, name_begin, name_end);
  writer.OpenPartitionList();
  const std::vector<TSetup::TPartition> &pvec = topic.Partitions;
  size_t node_id = topic.FirstPortOffset;
  size_t node_count = Setup.Ports.size();
  assert(node_id < node_count);

  for (size_t i = 0; i < pvec.size(); ++i) {
    writer.OpenPartition(0, i, node_id);
    writer.OpenReplicaList();
    writer.CloseReplicaList();
    writer.OpenCaughtUpReplicaList();
    writer.CloseCaughtUpReplicaList();
    writer.ClosePartition();
    node_id = (node_id + 1) % node_count;
  }

  writer.ClosePartitionList();
  writer.CloseTopic();
}
