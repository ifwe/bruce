/* <bruce/metadata_fetcher.cc>

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

   Implements <bruce/metadata_fetcher.h>
 */

#include <bruce/metadata_fetcher.h>

#include <algorithm>
#include <cstddef>
#include <stdexcept>

#include <syslog.h>

#include <base/io_utils.h>
#include <bruce/util/connect_to_host.h>
#include <bruce/util/system_error_codes.h>
#include <server/counter.h>
#include <socket/db/error.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::KafkaProto;
using namespace Bruce::Util;

SERVER_COUNTER(BadMetadataContent);
SERVER_COUNTER(BadMetadataResponse);
SERVER_COUNTER(BadMetadataResponseSize);
SERVER_COUNTER(MetadataHasEmptyBrokerList);
SERVER_COUNTER(MetadataHasEmptyTopicList);
SERVER_COUNTER(MetadataResponseHasExtraJunk);
SERVER_COUNTER(MetadataResponseRead1LostTcpConnection);
SERVER_COUNTER(MetadataResponseRead1Success);
SERVER_COUNTER(MetadataResponseRead1TimedOut);
SERVER_COUNTER(MetadataResponseRead2LostTcpConnection);
SERVER_COUNTER(MetadataResponseRead2TimedOut);
SERVER_COUNTER(MetadataResponseRead2UnexpectedEnd);
SERVER_COUNTER(MetadataResponseReadSuccess);
SERVER_COUNTER(ReadMetadataResponse2Fail);
SERVER_COUNTER(SendMetadataRequestFail);
SERVER_COUNTER(SendMetadataRequestLostTcpConnection);
SERVER_COUNTER(SendMetadataRequestSuccess);
SERVER_COUNTER(SendMetadataRequestTimedOut);
SERVER_COUNTER(SendMetadataRequestUnexpectedEnd);
SERVER_COUNTER(StartSendMetadataRequest);

static std::vector<uint8_t>
CreateMetadataRequest(const TWireProtocol &kafka_protocol) {
  std::vector<uint8_t> result;
  kafka_protocol.WriteMetadataRequest(result, 0);
  return std::move(result);
}

TMetadataFetcher::TMetadataFetcher(const TWireProtocol &kafka_protocol)
    : KafkaProtocol(kafka_protocol),
      MetadataRequest(CreateMetadataRequest(kafka_protocol)) {
}

bool TMetadataFetcher::Connect(const char *host_name, in_port_t port) {
  assert(this);
  Disconnect();

  try {
    ConnectToHost(host_name, port, Sock);
  } catch (const std::system_error &x) {
    syslog(LOG_ERR, "Failed to connect to host %s port %d for metadata: %s",
           host_name, static_cast<int>(port), x.what());
    assert(!Sock.IsOpen());
    return false;
  } catch (const Socket::Db::TError &x) {
    syslog(LOG_ERR, "Failed to connect to host %s port %d for metadata: %s",
           host_name, static_cast<int>(port), x.what());
    assert(!Sock.IsOpen());
    return false;
  }

  return Sock.IsOpen();
}

std::unique_ptr<TMetadata> TMetadataFetcher::Fetch(int timeout_ms) {
  assert(this);

  if (!Sock.IsOpen()) {
    throw std::logic_error("Must connect to host before getting metadata");
  }

  std::unique_ptr<TMetadata> result;

  if (!SendRequest(MetadataRequest, timeout_ms) || !ReadResponse(timeout_ms)) {
    return std::move(result);
  }

  try {
    result = KafkaProtocol.BuildMetadataFromResponse(&ResponseBuf[0],
        ResponseBuf.size());
  } catch (const TWireProtocol::TBadMetadataResponse &x) {
    BadMetadataResponse.Increment();
    syslog(LOG_ERR, "Failed to parse metadata response: %s", x.what());
    return std::move(result);
  } catch (const TMetadata::TBadMetadata &x) {
    BadMetadataContent.Increment();
    syslog(LOG_ERR, "Failed to build metadata structure from response: %s",
           x.what());
    return std::move(result);
  }

  bool bad_metadata = false;

  if (result->GetBrokers().empty()) {
    MetadataHasEmptyBrokerList.Increment();
    bad_metadata = true;
  }

  if (result->GetTopics().empty()) {
    MetadataHasEmptyTopicList.Increment();
    bad_metadata = true;
  }

  if (bad_metadata) {
    syslog(LOG_ERR, "Bad metadata response: broker count %u topic count %u",
           static_cast<unsigned>(result->GetBrokers().size()),
           static_cast<unsigned>(result->GetTopics().size()));
    result.reset();
  }

  return std::move(result);
}

TMetadataFetcher::TTopicAutocreateResult
TMetadataFetcher::TopicAutocreate(const char *topic, int timeout_ms) {
  assert(this);

  if (!Sock.IsOpen()) {
    throw std::logic_error("Must connect to host before getting metadata");
  }

  std::vector<uint8_t> request;
  KafkaProtocol.WriteSingleTopicMetadataRequest(request, topic, 0);

  if (!SendRequest(request, timeout_ms) || !ReadResponse(timeout_ms)) {
    return TTopicAutocreateResult::TryOtherBroker;
  }

  bool success = false;

  try {
    success = KafkaProtocol.TopicAutocreateWasSuccessful(topic,
        &ResponseBuf[0], ResponseBuf.size());
  } catch (const TWireProtocol::TBadMetadataResponse &x) {
    BadMetadataResponse.Increment();
    syslog(LOG_ERR, "Failed to parse metadata response: %s", x.what());
    return TTopicAutocreateResult::TryOtherBroker;
  }

  return success ? TTopicAutocreateResult::Success :
                   TTopicAutocreateResult::Fail;
}

bool TMetadataFetcher::SendRequest(const std::vector<uint8_t> &request,
    int timeout_ms) {
  assert(this);
  StartSendMetadataRequest.Increment();

  try {
    if (!TryWriteExactly(Sock, &request[0], request.size(), timeout_ms)) {
      SendMetadataRequestFail.Increment();
      syslog(LOG_ERR, "Failed to send metadata request");
      return false;
    }
  } catch (const std::system_error &x) {
    if (LostTcpConnection(x)) {
      SendMetadataRequestLostTcpConnection.Increment();
      syslog(LOG_ERR, "Lost TCP connection to broker while trying to send "
             "metadata request: %s", x.what());
      return false;
    }

    if (TimedOut(x)) {
      SendMetadataRequestTimedOut.Increment();
      syslog(LOG_ERR, "Socket timeout while trying to send metadata request");
      return false;
    }

    throw;  // anything else is fatal
  } catch (const TUnexpectedEnd &) {
    SendMetadataRequestUnexpectedEnd.Increment();
    syslog(LOG_ERR, "Lost TCP connection to broker while trying to send "
           "metadata request");
    return false;
  }

  SendMetadataRequestSuccess.Increment();
  return true;
}

bool TMetadataFetcher::ReadResponse(int timeout_ms) {
  assert(this);
  const size_t response_buf_initial_size =
      std::max<size_t>(65536, KafkaProtocol.GetBytesNeededToGetResponseSize());
  ResponseBuf.resize(response_buf_initial_size);
  size_t byte_count = 0;

  try {
    byte_count = ReadAtMost(Sock, &ResponseBuf[0], ResponseBuf.size(),
                            timeout_ms);
  } catch (const std::system_error &x) {
    if (LostTcpConnection(x)) {
      MetadataResponseRead1LostTcpConnection.Increment();
      syslog(LOG_ERR, "Lost TCP connection to broker while trying to read "
             "metadata response: %s", x.what());
      return false;
    }

    if (TimedOut(x)) {
      MetadataResponseRead1TimedOut.Increment();
      syslog(LOG_ERR, "Socket timeout while trying to read metadata response");
      return false;
    }

    throw;  // anything else is fatal
  }

  MetadataResponseRead1Success.Increment();
  size_t response_size = 0;

  try {
    response_size = KafkaProtocol.GetResponseSize(&ResponseBuf[0]);
  } catch (const TWireProtocol::TBadResponseSize &) {
    BadMetadataResponseSize.Increment();
    syslog(LOG_ERR, "Router thread got bad metadata response size");
    return false;
  }

  ResponseBuf.resize(response_size);

  if (ResponseBuf.size() < byte_count) {
    MetadataResponseHasExtraJunk.Increment();
    syslog(LOG_WARNING, "Broker acting strange: metadata response followed by "
           "extra junk");
  } else if (ResponseBuf.size() > byte_count) {
    try {
      if (!TryReadExactly(Sock, &ResponseBuf[byte_count],
                          ResponseBuf.size() - byte_count, timeout_ms)) {
        ReadMetadataResponse2Fail.Increment();
        syslog(LOG_ERR, "Router thread failed to read metadata response");
        return false;
      }
    } catch (const std::system_error &x) {
      if (LostTcpConnection(x)) {
        MetadataResponseRead2LostTcpConnection.Increment();
        syslog(LOG_ERR, "Lost TCP connection to broker while trying to read "
               "metadata response: %s", x.what());
        return false;
      }

      if (TimedOut(x)) {
        MetadataResponseRead2TimedOut.Increment();
        syslog(LOG_ERR, "Socket timeout while trying to read metadata "
               "response");
        return false;
      }

      throw;  // anything else is fatal
    } catch (const TUnexpectedEnd &) {
      MetadataResponseRead2UnexpectedEnd.Increment();
      syslog(LOG_ERR, "Lost TCP connection to broker while trying to read "
             "metadata response");
      return false;
    }
  }

  MetadataResponseReadSuccess.Increment();
  return true;
}
