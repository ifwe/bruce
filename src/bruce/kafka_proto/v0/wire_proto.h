/* <bruce/kafka_proto/v0/wire_proto.h>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 Tagged

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

   Kafka wire protocol version 0 implementation class.
 */

#pragma once

#include <bruce/kafka_proto/wire_protocol.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>

#include <base/no_copy_semantics.h>
#include <base/thrower.h>
#include <bruce/conf/compression_type.h>

namespace Bruce {

  namespace KafkaProto {

    namespace V0 {

      class TWireProto final : public TWireProtocol {
        NO_COPY_SEMANTICS(TWireProto);

        public:
        TWireProto(int16_t required_acks, int32_t replication_timeout)
            : TWireProtocol(ComputeConstants()),
              RequiredAcks(required_acks),
              ReplicationTimeout(replication_timeout) {
        }

        virtual ~TWireProto() noexcept { }

        virtual TProduceRequestWriterApi *
        CreateProduceRequestWriter() const override;

        virtual TMsgSetWriterApi *CreateMsgSetWriter() const override;

        virtual TProduceResponseReaderApi *
        CreateProduceResponseReader() const override;

        /* This returns the size of the entire response, including the size
           field. */
        virtual size_t GetResponseSize(
            const void *response_begin) const override;

        virtual TAckResultAction ProcessAck(int16_t ack_value,
            const std::string &topic, int32_t partition) const override;

        virtual void WriteMetadataRequest(std::vector<uint8_t> &result,
            int32_t correlation_id) const override;

        virtual std::unique_ptr<TMetadata>
        BuildMetadataFromResponse(const void *response_buf,
            size_t response_buf_size) const override;

        virtual int16_t GetRequiredAcks() const override;

        virtual int32_t GetReplicationTimeout() const override;

        virtual int8_t GetCompressionAttributes(
            Conf::TCompressionType type) const override;

        private:
        enum class TErrorCode : int16_t {
          Unknown = -1,
          NoError = 0,
          OffsetOutOfRange = 1,
          InvalidMessage = 2,
          UnknownTopicOrPartition = 3,
          InvalidMessageSize = 4,
          LeaderNotAvailable = 5,
          NotLeaderForPartition = 6,
          RequestTimedOut = 7,
          BrokerNotAvailable = 8,
          ReplicaNotAvailable = 9,
          MessageSizeTooLarge = 10,
          StaleControllerEpochCode = 11,
          OffsetMetadataTooLargeCode = 12
        };

        static TConstants ComputeConstants();

        int16_t RequiredAcks;

        int32_t ReplicationTimeout;
      };  // TWireProto

    }  // V0

  }  // KafkaProto

}  // Bruce
