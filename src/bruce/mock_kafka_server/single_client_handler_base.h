/* <bruce/mock_kafka_server/single_client_handler_base.h>

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

   Mock Kafka server base class for handling a single client connection.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/kafka_proto/msg_set_reader_api.h>
#include <bruce/kafka_proto/produce_request_reader_api.h>
#include <bruce/kafka_proto/produce_response_writer_api.h>
#include <bruce/mock_kafka_server/config.h>
#include <bruce/mock_kafka_server/mock_kafka_worker.h>
#include <bruce/mock_kafka_server/port_map.h>
#include <bruce/mock_kafka_server/prod_req/msg_set.h>
#include <bruce/mock_kafka_server/prod_req/prod_req.h>
#include <bruce/mock_kafka_server/prod_req/prod_req_builder.h>
#include <bruce/mock_kafka_server/received_request_tracker.h>
#include <bruce/mock_kafka_server/setup.h>
#include <bruce/mock_kafka_server/shared_state.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TSingleClientHandlerBase : public TMockKafkaWorker {
      NO_COPY_SEMANTICS(TSingleClientHandlerBase);

      public:
      virtual ~TSingleClientHandlerBase() noexcept { }

      protected:
      virtual void Run() override;

      enum class TGetRequestResult {
        GotRequest,
        GotShutdownRequest,
        ClientDisconnected,
        InvalidRequest
      };  // TGetRequestResult

      enum class TSendMetadataResult {
        SentMetadata,
        GotShutdownRequest,
        ClientDisconnected
      };  // TSendMetadataResult

      enum class TRequestType {
        UnknownRequest,
        ProduceRequest,
        MetadataRequest
      };  // TRequestType

      struct TMetadataRequest {
        int32_t CorrelationId;
        std::string Topic;  // empty string means "all topics"

        TMetadataRequest()
            : CorrelationId(0) {
        }
      };  // TMetadataRequest

      TSingleClientHandlerBase(const TConfig &config,
          const TSetup::TInfo &setup,
          const std::shared_ptr<TPortMap> &port_map, size_t port_offset,
          TSharedState &ss, Base::TFd &&client_socket)
          : TMockKafkaWorker(std::move(client_socket)),
            Config(config),
            Setup(setup),
            Ss(ss),
            PortMap(port_map),
            PortOffset(port_offset),
            ProduceRequestCount(0),
            MetadataRequestCount(0),
            MsgSetCount(0),
            MsgCount(0) {
        assert(PortOffset < Setup.Ports.size());
      }

      virtual Bruce::KafkaProto::TProduceRequestReaderApi &
      GetProduceRequestReader() = 0;

      virtual Bruce::KafkaProto::TMsgSetReaderApi &GetMsgSetReader() = 0;

      virtual Bruce::KafkaProto::TProduceResponseWriterApi &
      GetProduceResponseWriter() = 0;

      virtual bool ValidateMetadataRequestHeader() = 0;

      virtual bool ValidateMetadataRequest(TMetadataRequest &request) = 0;

      virtual TSendMetadataResult
      SendMetadataResponse(const TMetadataRequest &request, int16_t error,
          const std::string &error_topic, size_t delay) = 0;

      size_t GetMetadataRequestCount() const {
        assert(this);
        return MetadataRequestCount;
      }

      enum class TAction {
        InjectDisconnect,
        DisconnectOnError,
        RejectBadDest,
        InjectError1,
        InjectError2,
        Respond
      };  // TAction

      void PrintMdReq(size_t req_count, const TMetadataRequest &req,
          TAction action, const std::string &error_topic,
          int16_t topic_error_value, size_t wait);

      const TConfig &Config;

      const TSetup::TInfo &Setup;

      TSharedState &Ss;

      std::shared_ptr<TPortMap> PortMap;

      size_t PortOffset;

      /* output file */
      std::ofstream Out;

      std::vector<uint8_t> InputBuf;

      private:
      static const char *ActionToString(TAction action);

      void GetSocketPeer();

      bool OpenOutputFile();

      TGetRequestResult GetRequest();

      TRequestType GetRequestType();

      bool CheckProduceRequestErrorInjectionCmd(size_t &cmd_seq,
          std::string &msg_body_match, bool &match_any_msg_body,
          int16_t &ack_error, bool &disconnect, size_t &delay);

      bool CheckMetadataRequestErrorInjectionCmd(size_t &cmd_seq,
          bool &all_topics, std::string &topic, int16_t &error,
          bool &disconnect, size_t &delay);

      TAction ChooseMsgSetAction(const ProdReq::TMsgSet &msg_set,
          const std::string &topic, int32_t partition, size_t &delay,
          int16_t &ack_error);

      void PrintCurrentMsgSet(const ProdReq::TMsgSet &msg_set,
          int32_t partition, TAction action, int16_t ack_error);

      bool PrepareProduceResponse(const ProdReq::TProdReq &prod_req,
          std::vector<TReceivedRequestTracker::TRequestInfo> &done_requests);

      bool HandleProduceRequest();

      bool HandleMetadataRequest();

      const TSetup::TPartition *FindPartition(const std::string &topic,
          int32_t partition) const;

      void DoRun();

      std::string PeerAddressString;

      size_t ProduceRequestCount;

      size_t MetadataRequestCount;

      size_t MsgSetCount;

      size_t MsgCount;

      TMetadataRequest MetadataRequest;

      std::vector<uint8_t> OutputBuf;
    };  // TSingleClientHandlerBase

  }  // MockKafkaServer

}  // Bruce
