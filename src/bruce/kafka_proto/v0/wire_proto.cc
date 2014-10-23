/* <bruce/kafka_proto/v0/wire_proto.cc>

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

   Implements <bruce/kafka_proto/v0/wire_proto.h>.
 */

#include <bruce/kafka_proto/v0/wire_proto.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>

#include <syslog.h>

#include <base/no_default_case.h>
#include <bruce/kafka_proto/v0/metadata_request_writer.h>
#include <bruce/kafka_proto/v0/metadata_response_reader.h>
#include <bruce/kafka_proto/v0/msg_set_writer.h>
#include <bruce/kafka_proto/v0/produce_request_constants.h>
#include <bruce/kafka_proto/v0/produce_request_writer.h>
#include <bruce/kafka_proto/v0/produce_response_reader.h>
#include <bruce/kafka_proto/v0/protocol_util.h>
#include <bruce/metadata.h>
#include <bruce/util/time_util.h>
#include <server/counter.h>

using namespace Bruce;
using namespace Bruce::Conf;
using namespace Bruce::KafkaProto;
using namespace Bruce::KafkaProto::V0;
using namespace Bruce::Util;

SERVER_COUNTER(AckErrorBrokerNotAvailable);
SERVER_COUNTER(AckErrorInvalidMessage);
SERVER_COUNTER(AckErrorInvalidMessageSize);
SERVER_COUNTER(AckErrorLeaderNotAvailable);
SERVER_COUNTER(AckErrorMessageSizeTooLarge);
SERVER_COUNTER(AckErrorNone);
SERVER_COUNTER(AckErrorNotLeaderForPartition);
SERVER_COUNTER(AckErrorOffsetMetadataTooLargeCode);
SERVER_COUNTER(AckErrorOffsetOutOfRange);
SERVER_COUNTER(AckErrorReplicaNotAvailable);
SERVER_COUNTER(AckErrorRequestTimedOut);
SERVER_COUNTER(AckErrorStaleControllerEpochCode);
SERVER_COUNTER(AckErrorUndocumented);
SERVER_COUNTER(AckErrorUnknown);
SERVER_COUNTER(AckErrorUnknownTopicOrPartition);
SERVER_COUNTER(TopicAutocreateGotErrorResponse);
SERVER_COUNTER(TopicAutocreateNoTopicInResponse);
SERVER_COUNTER(TopicAutocreateSuccess);
SERVER_COUNTER(TopicAutocreateUnexpectedTopicInResponse);

TProduceRequestWriterApi *
TWireProto::CreateProduceRequestWriter() const {
  assert(this);
  return new TProduceRequestWriter;
}

TMsgSetWriterApi *
TWireProto::CreateMsgSetWriter() const {
  assert(this);
  return new TMsgSetWriter;
}

TProduceResponseReaderApi *
TWireProto::CreateProduceResponseReader() const {
  assert(this);
  return new TProduceResponseReader;
}

size_t TWireProto::GetResponseSize(const void *response_begin) const {
  assert(this);
  return Bruce::KafkaProto::V0::GetRequestOrResponseSize(response_begin);
}

TWireProtocol::TAckResultAction
TWireProto::ProcessAck(int16_t ack_value, const std::string &topic,
    int32_t partition) const {
  assert(this);

  switch (static_cast<TErrorCode>(ack_value)) {
    case TErrorCode::Unknown: {
      AckErrorUnknown.Increment();
      syslog(LOG_ERR, "Kafka ACK returned unexpected server error");
      return TAckResultAction::Discard;
    }
    case TErrorCode::NoError: {
      AckErrorNone.Increment();
      break;  // successful ACK
    }
    case TErrorCode::OffsetOutOfRange: {
      AckErrorOffsetOutOfRange.Increment();
      syslog(LOG_ERR, "Kafka ACK returned offset out of range error");
      return TAckResultAction::Discard;
    }
    case TErrorCode::InvalidMessage: {
      AckErrorInvalidMessage.Increment();
      syslog(LOG_ERR, "Kafka ACK returned bad CRC error");
      return TAckResultAction::Resend;
    }
    case TErrorCode::UnknownTopicOrPartition: {
      AckErrorUnknownTopicOrPartition.Increment();
      syslog(LOG_ERR, "Kafka ACK returned unknown topic or partition error: "
             "topic [%s] partition %d", topic.c_str(),
             static_cast<int>(partition));

      /* Normally we want to discard the message in addition to hitting the
         pause button to handle the case where the topic no longer exists.  In
         the atypical case where we are doing reconfiguration of the Kafka
         cluster that involves moving partitions from one broker to another,
         we don't want to discard, since the act of moving a partition may
         cause this error code.  This is a somewhat ugly workaround. */
      return GetRetryOnUnknownPartition() ?
          TAckResultAction::Pause : TAckResultAction::DiscardAndPause;
    }
    case TErrorCode::InvalidMessageSize: {
      AckErrorInvalidMessageSize.Increment();
      syslog(LOG_ERR, "Kafka ACK returned negative message size error");
      return TAckResultAction::Discard;
    }
    case TErrorCode::LeaderNotAvailable: {
      AckErrorLeaderNotAvailable.Increment();
      syslog(LOG_ERR, "Kafka ACK returned leader not available error "
                      "(leadership election in progress)");
      return TAckResultAction::Pause;
    }
    case TErrorCode::NotLeaderForPartition: {
      AckErrorNotLeaderForPartition.Increment();
      syslog(LOG_ERR, "Kafka ACK returned not leader for partition error");
      return TAckResultAction::Pause;
    }
    case TErrorCode::RequestTimedOut: {
      AckErrorRequestTimedOut.Increment();
      syslog(LOG_ERR, "Kafka ACK returned request timed out error");
      return TAckResultAction::Pause;
    }
    case TErrorCode::BrokerNotAvailable: {
      AckErrorBrokerNotAvailable.Increment();
      syslog(LOG_ERR, "Kafka ACK returned broker not available error (used "
             "only internally by Kafka)");
      return TAckResultAction::Pause;
    }
    case TErrorCode::ReplicaNotAvailable: {
      AckErrorReplicaNotAvailable.Increment();
      syslog(LOG_ERR, "Kafka ACK returned replica not available error");
      return TAckResultAction::Pause;
    }
    case TErrorCode::MessageSizeTooLarge: {
      AckErrorMessageSizeTooLarge.Increment();
      syslog(LOG_ERR, "Kafka ACK returned message size too large error");
      return TAckResultAction::Discard;
    }
    case TErrorCode::StaleControllerEpochCode: {
      AckErrorStaleControllerEpochCode.Increment();
      syslog(LOG_ERR, "Kafka ACK returned state controller epoch code error");
      return TAckResultAction::Discard;
    }
    case TErrorCode::OffsetMetadataTooLargeCode: {
      AckErrorOffsetMetadataTooLargeCode.Increment();
      syslog(LOG_ERR, "Kafka ACK returned offset metadata too large error");
      return TAckResultAction::Discard;
    }
    default:
      AckErrorUndocumented.Increment();
      syslog(LOG_ERR, "Kafka ACK returned undocumented error code: %d",
             static_cast<int>(ack_value));
      return TAckResultAction::Discard;
  }

  return TAckResultAction::Ok;
}

void TWireProto::WriteMetadataRequest(std::vector<uint8_t> &result,
    int32_t correlation_id) const {
  TMetadataRequestWriter().WriteAllTopicsRequest(result, correlation_id);
}

void TWireProto::WriteSingleTopicMetadataRequest(std::vector<uint8_t> &result,
    const char *topic, int32_t correlation_id) const {
  TMetadataRequestWriter().WriteSingleTopicRequest(result, topic,
      topic + std::strlen(topic), correlation_id);
}

std::unique_ptr<TMetadata>
TWireProto::BuildMetadataFromResponse(const void *response_buf,
    size_t response_buf_size) const {
  TMetadata::TBuilder builder;
  TMetadataResponseReader reader(response_buf, response_buf_size);
  std::string name;
  builder.OpenBrokerList();

  while (reader.NextBroker()) {
    name.assign(reader.GetCurrentBrokerHostBegin(),
                reader.GetCurrentBrokerHostEnd());
    builder.AddBroker(reader.GetCurrentBrokerNodeId(), std::move(name),
                      reader.GetCurrentBrokerPort());
  }

  builder.CloseBrokerList();

  while (reader.NextTopic()) {
    if (reader.GetCurrentTopicErrorCode()) {
      continue;
    }

    name.assign(reader.GetCurrentTopicNameBegin(),
                reader.GetCurrentTopicNameEnd());
    builder.OpenTopic(name);

    while (reader.NextPartitionInTopic()) {
      int16_t error_code = reader.GetCurrentPartitionErrorCode();

      /* Error code 9 means "replica not available".  In this case, it is still
         ok to send messages to the leader. */
      if ((error_code == 0) || (error_code == 9)) {
        builder.AddPartitionToTopic(reader.GetCurrentPartitionId(),
            reader.GetCurrentPartitionLeaderId(), error_code);
      }
    }

    builder.CloseTopic();
  }

  return std::unique_ptr<TMetadata>(builder.Build());
}

bool TWireProto::TopicAutocreateWasSuccessful(const char *topic,
    const void *response_buf, size_t response_buf_size) const {
  assert(this);
  TMetadataResponseReader reader(response_buf, response_buf_size);

  if (!reader.NextTopic()) {
    TopicAutocreateNoTopicInResponse.Increment();
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Autocreate for topic [%s] failed: no topic in metadata "
             "response", topic);
    }

    return false;
  }

  std::string response_topic(reader.GetCurrentTopicNameBegin(),
      reader.GetCurrentTopicNameEnd());

  if (response_topic != topic) {
    TopicAutocreateUnexpectedTopicInResponse.Increment();
    syslog(LOG_ERR, "Autocreate for topic [%s] failed: unexpected topic [%s] "
           "in metadata response", topic, response_topic.c_str());
    return false;
  }

  int16_t error_code = reader.GetCurrentTopicErrorCode();

  /* An error code of 5 means "leader not available", which is what we expect
     to see when the topic was successfully created.  An error code of 0 (no
     error) probably indicates that the topic was already created by some other
     Kafka client (perhaps a Bruce instance running on some other host) since
     we last updated our metadata. */
  if ((error_code != 0) && (error_code != 5)) {
    TopicAutocreateGotErrorResponse.Increment();
    static TLogRateLimiter lim(std::chrono::seconds(30));

    if (lim.Test()) {
      syslog(LOG_ERR, "Autocreate for topic [%s] failed: got error code %d",
             topic, static_cast<int>(error_code));
    }

    return false;
  }

  TopicAutocreateSuccess.Increment();
  return true;
}

int16_t TWireProto::GetRequiredAcks() const {
  assert(this);
  return RequiredAcks;
}

int32_t TWireProto::GetReplicationTimeout() const {
  assert(this);
  return ReplicationTimeout;
}

int8_t TWireProto::GetCompressionAttributes(TCompressionType type) const {
  assert(this);

  switch (type) {
    case TCompressionType::None:
      break;
    case TCompressionType::Snappy:
      return 2;
    NO_DEFAULT_CASE;
  }

  return 0;
}

TWireProtocol::TConstants TWireProto::ComputeConstants(
    bool retry_on_unknown_partition) {
  using PRC = TProduceRequestConstants;
  TConstants constants;
  constants.BytesNeededToGetResponseSize =
      Bruce::KafkaProto::V0::BytesNeededToGetRequestOrResponseSize();
  constants.SingleMsgOverhead = PRC::MSG_OFFSET_SIZE + PRC::MSG_SIZE_SIZE +
      PRC::CRC_SIZE + PRC::MAGIC_BYTE_SIZE + PRC::ATTRIBUTES_SIZE +
      PRC::KEY_LEN_SIZE + PRC::VALUE_LEN_SIZE;
  constants.RetryOnUnknownPartition = retry_on_unknown_partition;
  return constants;
}
