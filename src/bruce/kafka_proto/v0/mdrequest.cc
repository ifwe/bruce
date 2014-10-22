/* <bruce/kafka_proto/v0/mdrequest.cc>

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

   Utility program for sending a metadata request to a Kafka broker and writing
   the response in JSON to standard output.
 */

#include <bruce/kafka_proto/v0/metadata_request_writer.h>
#include <bruce/kafka_proto/v0/metadata_response_reader.h>
#include <bruce/kafka_proto/v0/protocol_util.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <netinet/in.h>
#include <sys/uio.h>

#include <base/basename.h>
#include <base/fd.h>
#include <base/indent.h>
#include <base/io_utils.h>
#include <base/no_copy_semantics.h>
#include <base/thrower.h>
#include <bruce/build_id.h>
#include <bruce/util/arg_parse_error.h>
#include <bruce/util/connect_to_host.h>
#include <rpc/transceiver.h>
#include <tclap/CmdLine.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::KafkaProto::V0;
using namespace Bruce::Util;
using namespace Rpc;

struct TConfig {
  /* Throws TArgParseError on error parsing args. */
  TConfig(int argc, char *argv[]);

  std::string BrokerHost;

  in_port_t BrokerPort;

  std::string Topic;

  size_t RequestCount;
};  // TConfig

static void ParseArgs(int argc, char *argv[], TConfig &config) {
  using namespace TCLAP;
  const std::string prog_name = Basename(argv[0]);
  std::vector<const char *> arg_vec(&argv[0], &argv[0] + argc);
  arg_vec[0] = prog_name.c_str();

  try {
    CmdLine cmd("Utility for sending a metadata request to a Kafka broker and "
        "writing the response to standard output", ' ', bruce_build_id);
    ValueArg<decltype(config.BrokerHost)> arg_broker_host("", "broker_host",
        "Kafka broker to connect to.", true, config.BrokerHost, "HOST");
    cmd.add(arg_broker_host);
    ValueArg<decltype(config.BrokerPort)> arg_broker_port("", "broker_port",
        "Port to connect to.", false, config.BrokerPort, "PORT");
    cmd.add(arg_broker_port);
    ValueArg<decltype(config.Topic)> arg_topic("", "topic", "Topic to request "
        "metadata for.  If omitted, metadata will be requested for all "
        "topics.", false, config.Topic, "TOPIC");
    cmd.add(arg_topic);
    ValueArg<decltype(config.RequestCount)> arg_request_count("",
        "request_count", "Number of requests to send (for testing).", false,
        config.RequestCount, "COUNT");
    cmd.add(arg_request_count);
    cmd.parse(argc, &arg_vec[0]);
    config.BrokerHost = arg_broker_host.getValue();
    config.BrokerPort = arg_broker_port.getValue();
    config.Topic = arg_topic.getValue();
    config.RequestCount = arg_request_count.getValue();
  } catch (const ArgException &x) {
    throw TArgParseError(x.error(), x.argId());
  }
}

TConfig::TConfig(int argc, char *argv[])
    : BrokerPort(9092),
      RequestCount(1) {
  ParseArgs(argc, argv, *this);
}

class TServerClosedConnection final : public std::runtime_error {
  public:
  TServerClosedConnection()
      : std::runtime_error("Server unexpectedly closed connection") {
  }
};  // TServerClosedConnection

class TCorrelationIdMismatch final : public std::runtime_error {
  public:
  TCorrelationIdMismatch()
      : std::runtime_error("Kafka correlation ID mismatch") {
  }
};  // TCorrelationIdMismatch

static const int32_t CORRELATION_ID = 0;

static void SendRequest(int socket_fd, const std::string &topic) {
  TTransceiver xver;
  struct iovec *vecs = nullptr;
  std::vector<uint8_t> header_buf;

  if (topic.empty()) {  // all topics request
    vecs = xver.GetIoVecs(1);
    header_buf.resize(TMetadataRequestWriter::NumAllTopicsHeaderBytes());
    TMetadataRequestWriter().WriteAllTopicsRequest(*vecs, &header_buf[0],
        CORRELATION_ID);
  } else {  // single topic request
    vecs = xver.GetIoVecs(2);
    header_buf.resize(TMetadataRequestWriter::NumSingleTopicHeaderBytes());
    const char *topic_begin = topic.data();
    const char *topic_end = topic_begin + topic.size();
    TMetadataRequestWriter().WriteSingleTopicRequest(vecs[0], vecs[1],
        &header_buf[0], topic_begin, topic_end, CORRELATION_ID);
  }

  assert(vecs);
  assert(!header_buf.empty());

  for (size_t part = 0; xver; xver += part) {
    part = xver.Send(socket_fd);
  }
}

static void ReadResponse(int socket_fd, std::vector<uint8_t> &response_buf) {
  size_t nbytes = BytesNeededToGetRequestOrResponseSize();
  response_buf.resize(nbytes);

  if (!TryReadExactly(socket_fd, &response_buf[0], nbytes)) {
    throw TServerClosedConnection();
  }

  size_t response_size = GetRequestOrResponseSize(&response_buf[0]);
  response_buf.resize(response_size);
  assert(response_size >= nbytes);

  if (!TryReadExactly(socket_fd, &response_buf[nbytes],
                      response_size - nbytes)) {
    throw TServerClosedConnection();
  }

  TMetadataResponseReader reader(&response_buf[0], response_buf.size());

  if (reader.GetCorrelationId() != CORRELATION_ID) {
    throw TCorrelationIdMismatch();
  }
}

class TResponsePrinter final {
  NO_COPY_SEMANTICS(TResponsePrinter);

  public:
  TResponsePrinter(std::ostream &out, const std::vector<uint8_t> &response)
      : Out(out),
        Resp(&response[0], response.size()) {
  }

  void Print();

  private:
  void WriteOneBroker(TIndent &ind0);

  void WriteBrokers(TIndent &ind0);

  void WriteOneReplica(TIndent & ind0);

  void WriteOneCaughtUpReplica(TIndent & ind0);

  void WriteOnePartition(TIndent &ind0);

  void WriteOneTopic(TIndent &ind0);

  void WriteTopics(TIndent &ind0);

  std::ostream &Out;

  TMetadataResponseReader Resp;
};  // TResponsePrinter

void TResponsePrinter::Print() {
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  Out << ind0 << "{" << std::endl;
  WriteBrokers(ind0);
  WriteTopics(ind0);
  Out << ind0 << "}" << std::endl;
}

void TResponsePrinter::WriteOneBroker(TIndent &ind0) {
  TIndent ind1(ind0);
  std::string host(Resp.GetCurrentBrokerHostBegin(),
                   Resp.GetCurrentBrokerHostEnd());
  Out << ind1 << "\"node\": " << Resp.GetCurrentBrokerNodeId() << ","
      << std::endl
      << ind1 << "\"host\": \"" << host << "\"," << std::endl
      << ind1 << "\"port\": " << Resp.GetCurrentBrokerPort() << std::endl;
}

void TResponsePrinter::WriteBrokers(TIndent &ind0) {
  TIndent ind1(ind0);
  Out << ind1 << "\"brokers\": [" << std::endl;

  {
    TIndent ind2(ind1);

    if (Resp.NextBroker()) {
      Out << ind2 << "{" << std::endl;
      WriteOneBroker(ind2);
      Out << ind2 << "}";

      while (Resp.NextBroker()) {
        Out << "," << std::endl << ind2 << "{" << std::endl;
        WriteOneBroker(ind2);
        Out << ind2 << "}";
      }

      Out << std::endl;
    }
  }

  Out << ind1 << "]," << std::endl;
}

void TResponsePrinter::WriteOneReplica(TIndent & ind0) {
  TIndent ind1(ind0);
  Out << ind1 << "\"id\": " << Resp.GetCurrentReplicaNodeId() << std::endl;
}

void TResponsePrinter::WriteOneCaughtUpReplica(TIndent & ind0) {
  TIndent ind1(ind0);
  Out << ind1 << "\"id\": " << Resp.GetCurrentCaughtUpReplicaNodeId()
      << std::endl;
}

void TResponsePrinter::WriteOnePartition(TIndent &ind0) {
  TIndent ind1(ind0);
  Out << ind1 << "\"id\": " << Resp.GetCurrentPartitionId() << "," << std::endl
      << ind1 << "\"leader_id\": " << Resp.GetCurrentPartitionLeaderId() << ","
      << std::endl
      << ind1 << "\"error_code: " << Resp.GetCurrentPartitionErrorCode() << ","
      << std::endl
      << ind1 << "\"replicas\": [" << std::endl;

  {
    TIndent ind2(ind1);

    if (Resp.NextReplicaInPartition()) {
      Out << ind2 << "{" << std::endl;
      WriteOneReplica(ind2);
      Out << ind2 << "}";

      while (Resp.NextReplicaInPartition()) {
        Out << "," << std::endl << ind2 << "{" << std::endl;
        WriteOneReplica(ind2);
        Out << ind2 << "}";
      }

      Out << std::endl;
    }
  }

  Out << ind1 << "]," << std::endl
      << ind1 << "\"caught_up_replicas\": [" << std::endl;

  {
    TIndent ind2(ind1);

    if (Resp.NextCaughtUpReplicaInPartition()) {
      Out << ind2 << "{" << std::endl;
      WriteOneCaughtUpReplica(ind2);
      Out << ind2 << "}";

      while (Resp.NextCaughtUpReplicaInPartition()) {
        Out << "," << std::endl << ind2 << "{" << std::endl;
        WriteOneCaughtUpReplica(ind2);
        Out << ind2 << "}";
      }

      Out << std::endl;
    }
  }

  Out << ind1 << "]" << std::endl;
}

void TResponsePrinter::WriteOneTopic(TIndent &ind0) {
  TIndent ind1(ind0);
  std::string name(Resp.GetCurrentTopicNameBegin(),
                   Resp.GetCurrentTopicNameEnd());
  Out << ind1 << "\"name\": \"" << name << "\"," << std::endl
      << ind1 << "\"error_code: " << Resp.GetCurrentTopicErrorCode() << ","
      << std::endl
      << ind1 << "\"partitions\": [" << std::endl;

  {
    TIndent ind2(ind1);

    if (Resp.NextPartitionInTopic()) {
      Out << ind2 << "{" << std::endl;
      WriteOnePartition(ind2);
      Out << ind2 << "}";

      while (Resp.NextPartitionInTopic()) {
        Out << "," << std::endl << ind2 << "{" << std::endl;
        WriteOnePartition(ind2);
        Out << ind2 << "}";
      }

      Out << std::endl;
    }
  }

  Out << ind1 << "]" << std::endl;
}

void TResponsePrinter::WriteTopics(TIndent &ind0) {
  TIndent ind1(ind0);
  Out << ind1 << "\"topics\": [" << std::endl;

  {
    TIndent ind2(ind1);

    if (Resp.NextTopic()) {
      Out << ind2 << "{" << std::endl;
      WriteOneTopic(ind2);
      Out << ind2 << "}";

      while (Resp.NextTopic()) {
        Out << "," << std::endl << ind2 << "{" << std::endl;
        WriteOneTopic(ind2);
        Out << ind2 << "}";
      }

      Out << std::endl;
    }
  }

  Out << ind1 << "]" << std::endl;
}

static int mdrequest_main(int argc, char **argv) {
  std::unique_ptr<TConfig> cfg;

  try {
    cfg.reset(new TConfig(argc, argv));
  } catch (const TArgParseError &x) {
    /* Error parsing command line arguments. */
    std::cerr << x.what() << std::endl;
    return EXIT_FAILURE;
  }

  TFd broker_socket;
  ConnectToHost(cfg->BrokerHost, cfg->BrokerPort, broker_socket);

  if (!broker_socket.IsOpen()) {
    std::cerr << "Failed to connect to host " << cfg->BrokerHost << " port "
        << cfg->BrokerPort << std::endl;
    return EXIT_FAILURE;
  }

  for (size_t i = 0; i < cfg->RequestCount; ++i) {
    SendRequest(broker_socket, cfg->Topic);
    std::vector<uint8_t> response_buf;
    ReadResponse(broker_socket, response_buf);
    std::ostringstream out;
    TResponsePrinter(out, response_buf).Print();
    std::cout << out.str();
  }

  broker_socket.Reset();
  return EXIT_SUCCESS;
}

int main(int argc, char **argv) {
  int ret = EXIT_SUCCESS;

  try {
    ret = mdrequest_main(argc, argv);
  } catch (const std::exception &ex) {
    std::cerr << "error: " << ex.what() << std::endl;
    ret = EXIT_FAILURE;
  } catch (...) {
    std::cerr << "error: uncaught unknown exception" << std::endl;
    ret = EXIT_FAILURE;
  }

  return ret;
}
