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
SERVER_COUNTER(AckErrorClusterAuthorizationFailedCode);
SERVER_COUNTER(AckErrorGroupAuthorizationFailedCode);
SERVER_COUNTER(AckErrorGroupCoordinatorNotAvailableCode);
SERVER_COUNTER(AckErrorGroupLoadInProgressCode);
SERVER_COUNTER(AckErrorIllegalGenerationCode);
SERVER_COUNTER(AckErrorInconsistentGroupProtocolCode);
SERVER_COUNTER(AckErrorInvalidCommitOffsetSizeCode);
SERVER_COUNTER(AckErrorInvalidGroupIdCode);
SERVER_COUNTER(AckErrorInvalidMessage);
SERVER_COUNTER(AckErrorInvalidMessageSize);
SERVER_COUNTER(AckErrorInvalidRequiredAcksCode);
SERVER_COUNTER(AckErrorInvalidSessionTimeoutCode);
SERVER_COUNTER(AckErrorInvalidTopicCode);
SERVER_COUNTER(AckErrorLeaderNotAvailable);
SERVER_COUNTER(AckErrorMessageSizeTooLarge);
SERVER_COUNTER(AckErrorNotCoordinatorForGroupCode);
SERVER_COUNTER(AckErrorNotEnoughReplicasAfterAppendCode);
SERVER_COUNTER(AckErrorNotEnoughReplicasCode);
SERVER_COUNTER(AckErrorNotLeaderForPartition);
SERVER_COUNTER(AckErrorOffsetMetadataTooLargeCode);
SERVER_COUNTER(AckErrorOffsetOutOfRange);
SERVER_COUNTER(AckErrorRebalanceInProgressCode);
SERVER_COUNTER(AckErrorRecordListTooLargeCode);
SERVER_COUNTER(AckErrorReplicaNotAvailable);
SERVER_COUNTER(AckErrorRequestTimedOut);
SERVER_COUNTER(AckErrorStaleControllerEpochCode);
SERVER_COUNTER(AckErrorTopicAuthorizationFailedCode);
SERVER_COUNTER(AckErrorUndocumented);
SERVER_COUNTER(AckErrorUnknown);
SERVER_COUNTER(AckErrorUnknownMemberIdCode);
SERVER_COUNTER(AckErrorUnknownTopicOrPartition);
SERVER_COUNTER(AckOk);
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

  /* The errors reported below are documented here:

         https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
   */
  switch (static_cast<TErrorCode>(ack_value)) {
    case TErrorCode::Unknown: {
      AckErrorUnknown.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: Unknown (unexpected server error)");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::NoError: {
      AckOk.Increment();
      break;  // successful ACK
    }
    case TErrorCode::OffsetOutOfRange: {
      AckErrorOffsetOutOfRange.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: OffsetOutOfRange");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::InvalidMessage: {
      AckErrorInvalidMessage.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: InvalidMessage (bad CRC)");
      }

      return TAckResultAction::Resend;
    }
    case TErrorCode::UnknownTopicOrPartition: {
      AckErrorUnknownTopicOrPartition.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: UnknownTopicOrPartition "
            "topic [%s] partition %d", topic.c_str(),
            static_cast<int>(partition));
      }

      /* This error may occur in cases where a reconfiguration of the Kafka
         cluster is being performed that involves moving partitions from one
         broker to another.  In this case, we want to reroute rather than
         discard so the messages are redirected to a valid destination broker.
         In the case where the topic no longer exists, the router thread will
         discard the messages during rerouting. */
      return TAckResultAction::Pause;
    }
    case TErrorCode::InvalidMessageSize: {
      AckErrorInvalidMessageSize.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: InvalidMessageSize");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::LeaderNotAvailable: {
      AckErrorLeaderNotAvailable.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: LeaderNotAvailable");
      }

      return TAckResultAction::Pause;
    }
    case TErrorCode::NotLeaderForPartition: {
      AckErrorNotLeaderForPartition.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: NotLeaderForPartition");
      }

      return TAckResultAction::Pause;
    }
    case TErrorCode::RequestTimedOut: {
      AckErrorRequestTimedOut.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: RequestTimedOut");
      }

      return TAckResultAction::Pause;
    }
    case TErrorCode::BrokerNotAvailable: {
      AckErrorBrokerNotAvailable.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: BrokerNotAvailable");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::ReplicaNotAvailable: {
      AckErrorReplicaNotAvailable.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: ReplicaNotAvailable");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::MessageSizeTooLarge: {
      AckErrorMessageSizeTooLarge.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: MessageSizeTooLarge");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::StaleControllerEpochCode: {
      AckErrorStaleControllerEpochCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: StaleControllerEpochCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::OffsetMetadataTooLargeCode: {
      AckErrorOffsetMetadataTooLargeCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: OffsetMetadataTooLargeCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::GroupLoadInProgressCode: {
      AckErrorGroupLoadInProgressCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: GroupLoadInProgressCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::GroupCoordinatorNotAvailableCode: {
      AckErrorGroupCoordinatorNotAvailableCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: GroupCoordinatorNotAvailableCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::NotCoordinatorForGroupCode: {
      AckErrorNotCoordinatorForGroupCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: NotCoordinatorForGroupCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::InvalidTopicCode: {
      AckErrorInvalidTopicCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: InvalidTopicCode topic [%s]",
            topic.c_str());
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::RecordListTooLargeCode: {
      AckErrorRecordListTooLargeCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: RecordListTooLargeCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::NotEnoughReplicasCode: {
      AckErrorNotEnoughReplicasCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: NotEnoughReplicasCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::NotEnoughReplicasAfterAppendCode: {
      AckErrorNotEnoughReplicasAfterAppendCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: NotEnoughReplicasAfterAppendCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::InvalidRequiredAcksCode: {
      AckErrorInvalidRequiredAcksCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: InvalidRequiredAcksCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::IllegalGenerationCode: {
      AckErrorIllegalGenerationCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: IllegalGenerationCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::InconsistentGroupProtocolCode: {
      AckErrorInconsistentGroupProtocolCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: InconsistentGroupProtocolCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::InvalidGroupIdCode: {
      AckErrorInvalidGroupIdCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: InvalidGroupIdCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::UnknownMemberIdCode: {
      AckErrorUnknownMemberIdCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: UnknownMemberIdCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::InvalidSessionTimeoutCode: {
      AckErrorInvalidSessionTimeoutCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: InvalidSessionTimeoutCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::RebalanceInProgressCode: {
      AckErrorRebalanceInProgressCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned error: RebalanceInProgressCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::InvalidCommitOffsetSizeCode: {
      AckErrorInvalidCommitOffsetSizeCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: InvalidCommitOffsetSizeCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::TopicAuthorizationFailedCode: {
      AckErrorTopicAuthorizationFailedCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: TopicAuthorizationFailedCode topic "
            "[%s]", topic.c_str());
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::GroupAuthorizationFailedCode: {
      AckErrorGroupAuthorizationFailedCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: GroupAuthorizationFailedCode");
      }

      return TAckResultAction::Discard;
    }
    case TErrorCode::ClusterAuthorizationFailedCode: {
      AckErrorClusterAuthorizationFailedCode.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR,
            "Kafka ACK returned error: ClusterAuthorizationFailedCode");
      }

      return TAckResultAction::Discard;
    }
    default: {
      AckErrorUndocumented.Increment();
      static TLogRateLimiter lim(std::chrono::seconds(30));

      if (lim.Test()) {
        syslog(LOG_ERR, "Kafka ACK returned undocumented error code: %d",
            static_cast<int>(ack_value));
      }

      return TAckResultAction::Discard;
    }
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

TWireProtocol::TConstants TWireProto::ComputeConstants() {
  using PRC = TProduceRequestConstants;
  TConstants constants;
  constants.BytesNeededToGetResponseSize =
      Bruce::KafkaProto::V0::BytesNeededToGetRequestOrResponseSize();
  constants.SingleMsgOverhead = PRC::MSG_OFFSET_SIZE + PRC::MSG_SIZE_SIZE +
      PRC::CRC_SIZE + PRC::MAGIC_BYTE_SIZE + PRC::ATTRIBUTES_SIZE +
      PRC::KEY_LEN_SIZE + PRC::VALUE_LEN_SIZE;
  return constants;
}
