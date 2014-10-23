/* <bruce/kafka_proto/wire_protocol.h>

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

   Base class that provides a uniform API for dealing with different versions
   of the Kafka producer wire format.  Derived classes implement specific
   versions, and the core bruce server code interacts with a base class
   instance to insulate itself from version-specific wire format details.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <base/no_copy_semantics.h>
#include <base/thrower.h>
#include <bruce/conf/compression_type.h>
#include <bruce/kafka_proto/msg_set_writer_api.h>
#include <bruce/kafka_proto/produce_request_writer_api.h>
#include <bruce/kafka_proto/produce_response_reader_api.h>
#include <bruce/metadata.h>

namespace Bruce {

  namespace KafkaProto {

    class TWireProtocol {
      NO_COPY_SEMANTICS(TWireProtocol);

      public:
      DEFINE_ERROR(TPartitionHasUnknownBroker, std::runtime_error,
          "Partition in metadata respose references unknown broker");

      class TBadMetadataResponse : public std::runtime_error {
        protected:
        explicit TBadMetadataResponse(const char *msg)
            : std::runtime_error(msg) {
        }

        public:
        virtual ~TBadMetadataResponse() noexcept { }
      };  // TBadMetadataResponse

      class TBadResponseSize final : public std::runtime_error {
        public:
        TBadResponseSize()
            : std::runtime_error("Invalid Kafka response size") {
        }

        virtual ~TBadResponseSize() noexcept { }
      };  // TBadResponseSize

      enum class TAckResultAction {
        Ok,
        Resend,
        Discard,
        Pause,
        DiscardAndPause
      };

      virtual ~TWireProtocol() noexcept { }

      /* Return the number of bytes of an ack from a Kafka broker that must be
         obtained in order to determine the size of the entire ack. */
      size_t GetBytesNeededToGetResponseSize() const {
        assert(this);
        return Constants.BytesNeededToGetResponseSize;
      }

      /* Return the number of bytes of overhead for a single message in a
         produce request. */
      size_t GetSingleMsgOverhead() const {
        assert(this);
        return Constants.SingleMsgOverhead;
      }

      bool GetRetryOnUnknownPartition() const {
        assert(this);
        return Constants.RetryOnUnknownPartition;
      }

      /* Return a pointer to a newly created produce request writer object.
         Caller assumes responsibility for deleting object. */
      virtual TProduceRequestWriterApi *CreateProduceRequestWriter() const = 0;

      /* Return a pointer to a newly created message set writer object.  Caller
         assumes responsibility for deleting object. */
      virtual TMsgSetWriterApi *CreateMsgSetWriter() const = 0;

      /* Return a pointer to a newly created produce response reader object.
         Caller assumes responsibility for deleting object. */
      virtual TProduceResponseReaderApi *
      CreateProduceResponseReader() const = 0;

      virtual size_t GetResponseSize(const void *response_begin) const = 0;

      virtual TAckResultAction ProcessAck(int16_t ack_value,
          const std::string &topic, int32_t partition) const = 0;

      /* Write an all topics metadata request to 'result'.  Resize 'result' to
         the size of the written request. */
      virtual void WriteMetadataRequest(std::vector<uint8_t> &result,
          int32_t correlation_id) const = 0;

      virtual void WriteSingleTopicMetadataRequest(
          std::vector<uint8_t> &result, const char *topic,
          int32_t correlation_id) const = 0;

      /* Throws a subclass of std::runtime_error on bad metadata response. */
      virtual std::unique_ptr<TMetadata>
      BuildMetadataFromResponse(const void *response_buf,
          size_t response_buf_size) const = 0;

      virtual bool TopicAutocreateWasSuccessful(const char *topic,
          const void *response_buf, size_t response_buf_size) const = 0;

      virtual int16_t GetRequiredAcks() const = 0;

      virtual int32_t GetReplicationTimeout() const = 0;

      virtual int8_t GetCompressionAttributes(
          Conf::TCompressionType type) const = 0;

      protected:
      struct TConstants {
        /* This is the number of bytes of a response from a Kafka broker that
           must be obtained in order to determine the size of the entire
           response. */
        size_t BytesNeededToGetResponseSize;

        /* This is the number of bytes of overhead for a single message in a
           produce request. */
        size_t SingleMsgOverhead;

        /* On receipt of "unknown topic or partition" error ACK, reroute
           message after updating metadata rather than discarding it.  This is
           a workaround for Kafka behavior that occurs when relocating a
           partition to a different broker. */
        bool RetryOnUnknownPartition;
      };

      explicit TWireProtocol(const TConstants &constants)
          : Constants(constants) {
      }

      private:
      const TConstants Constants;
    };  // TWireProtocol

  }  // KafkaProto

}  // Bruce
