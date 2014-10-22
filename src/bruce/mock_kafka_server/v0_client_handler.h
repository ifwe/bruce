/* <bruce/mock_kafka_server/v0_client_handler.h>

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

   Kafka protocol version 0 support for mock Kafka server.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/kafka_proto/v0/metadata_request_reader.h>
#include <bruce/kafka_proto/v0/metadata_response_writer.h>
#include <bruce/kafka_proto/v0/msg_set_reader.h>
#include <bruce/kafka_proto/v0/produce_request_reader.h>
#include <bruce/kafka_proto/v0/produce_response_writer.h>
#include <bruce/mock_kafka_server/config.h>
#include <bruce/mock_kafka_server/port_map.h>
#include <bruce/mock_kafka_server/setup.h>
#include <bruce/mock_kafka_server/shared_state.h>
#include <bruce/mock_kafka_server/single_client_handler_base.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TV0ClientHandler final : public TSingleClientHandlerBase {
      NO_COPY_SEMANTICS(TV0ClientHandler);

      public:
      TV0ClientHandler(const TConfig &config, const TSetup::TInfo &setup,
                       const std::shared_ptr<TPortMap> &port_map,
                       size_t port_offset, TSharedState &ss,
                       Base::TFd &&client_socket)
          : TSingleClientHandlerBase(config, setup, port_map, port_offset, ss,
                                     std::move(client_socket)) {
      }

      virtual ~TV0ClientHandler() noexcept;

      protected:
      virtual Bruce::KafkaProto::TProduceRequestReaderApi &
      GetProduceRequestReader() override;

      virtual Bruce::KafkaProto::TMsgSetReaderApi &GetMsgSetReader() override;

      virtual Bruce::KafkaProto::TProduceResponseWriterApi &
      GetProduceResponseWriter() override;

      virtual bool ValidateMetadataRequestHeader() override;

      virtual bool ValidateMetadataRequest(TMetadataRequest &request) override;

      virtual TSendMetadataResult
      SendMetadataResponse(const TMetadataRequest &request, int16_t error,
          const std::string &error_topic, size_t delay) override;

      private:
      void WriteSingleTopic(KafkaProto::V0::TMetadataResponseWriter &writer,
          const TSetup::TTopic &topic, const char *name_begin,
          const char *name_end, int16_t error);

      Bruce::KafkaProto::V0::TProduceRequestReader ProduceRequestReader;

      Bruce::KafkaProto::V0::TMsgSetReader MsgSetReader;

      Bruce::KafkaProto::V0::TProduceResponseWriter ProduceResponseWriter;

      Base::TOpt<KafkaProto::V0::TMetadataRequestReader>
          OptMetadataRequestReader;

      std::vector<uint8_t> MdResponseBuf;
    };  // TV0ClientHandler

  }  // MockKafkaServer

}  // Bruce
