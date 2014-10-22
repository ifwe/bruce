/* <bruce/mock_kafka_server/single_client_handler_base.cc>

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

   Implements <bruce/mock_kafka_server/single_client_handler_base.h>.
 */

#include <bruce/mock_kafka_server/single_client_handler_base.h>

#include <iostream>
#include <limits>

#include <boost/lexical_cast.hpp>
#include <sys/syscall.h>

#include <base/debug_log.h>
#include <base/field_access.h>
#include <base/gettid.h>
#include <base/no_default_case.h>
#include <base/opt.h>
#include <bruce/kafka_proto/msg_set_reader_api.h>
#include <bruce/mock_kafka_server/cmd.h>
#include <bruce/mock_kafka_server/cmd_bucket.h>
#include <socket/address.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Conf;
using namespace Bruce::KafkaProto;
using namespace Bruce::MockKafkaServer;
using namespace Bruce::MockKafkaServer::ProdReq;

void TSingleClientHandlerBase::Run() {
  assert(this);
  GetSocketPeer();
  DEBUG_LOG("got connection from %s", PeerAddressString.c_str());

  if (!OpenOutputFile()) {
    return;  // failed to open output file
  }

  try {
    DoRun();
  } catch (const std::ofstream::failure &) {
    std::cerr << "Error writing to server output file" << std::endl;
    return;
  }
}

void TSingleClientHandlerBase::PrintMdReq(size_t req_count,
    const TMetadataRequest &req, TAction action,
    const std::string &error_topic, int16_t topic_error_value, size_t wait) {
  assert(this);
  Out << "md_n=" << req_count << " corr=" << req.CorrelationId << " act="
      << ActionToString(action) << " ack=" << topic_error_value << " wait="
      << wait << " inj_err=" << error_topic.size() << "[" << error_topic
      << "] topic=" << req.Topic.size() << "[" << req.Topic << "]"
      << std::endl;
}

const char *TSingleClientHandlerBase::ActionToString(TAction action) {
  const char *ret = "";

  switch (action) {
    case TAction::InjectDisconnect: {
      ret = "dis1";
      break;
    }
    case TAction::DisconnectOnError: {
      ret = "dis2";
      break;
    }
    case TAction::RejectBadDest: {
      ret = "unkn";
      break;
    }
    case TAction::InjectError1: {
      ret = "inj1";
      break;
    }
    case TAction::InjectError2: {
      ret = "inj2";
      break;
    }
    case TAction::Respond: {
      ret = "resp";
      break;
    }
    NO_DEFAULT_CASE;
  }

  return ret;
}

void TSingleClientHandlerBase::GetSocketPeer() {
  assert(this);
  Socket::TAddress address = Socket::GetPeerName(ClientSocket);
  char name_buf[128];
  address.GetName(name_buf, sizeof(name_buf), nullptr, 0, NI_NUMERICHOST);
  PeerAddressString = name_buf;
}

bool TSingleClientHandlerBase::OpenOutputFile() {
  assert(this);
  std::string path(Config.OutputDir);
  std::ios_base::openmode flags = std::ios_base::out;

  if (Config.SingleOutputFile) {
    flags |= std::ios_base::app;
    path += "/server.out";
  } else {
    flags |= std::ios_base::trunc;
    path += "/server.out.";
    size_t port = Setup.BasePort + PortOffset;
    path += boost::lexical_cast<std::string>(port);
    path += ".";
    pid_t tid = Gettid();
    path += boost::lexical_cast<std::string>(tid);
  }

  Out.open(path, flags);

  if (!Out.is_open()) {
    std::cerr << "Error: Failed to open output file " << path << " for writing"
        << std::endl;
    return false;
  }

  Out.exceptions(std::ofstream::badbit);
  return true;
}

TSingleClientHandlerBase::TGetRequestResult
TSingleClientHandlerBase::GetRequest() {
  assert(this);

  /* The first 6 bytes of a request contain the size and API key fields. */
  InputBuf.resize(6);

  switch (TryReadExactlyOrShutdown(ClientSocket, &InputBuf[0],
                                   InputBuf.size())) {
    case TIoResult::Success: {
      break;
    }
    case TIoResult::Disconnected:
    case TIoResult::EmptyReadUnexpectedEnd: {
      if (Config.QuietLevel <= 2) {
        Out << "Info: Client disconnected on request read" << std::endl;
      }

      return TGetRequestResult::ClientDisconnected;
    }
    case TIoResult::UnexpectedEnd: {
      Out << "Error: Client unexpectedly disconnected 1 while reading request"
          << std::endl;
      return TGetRequestResult::ClientDisconnected;
    }
    case TIoResult::GotShutdownRequest: {
      if (Config.QuietLevel <= 2) {
        Out << "Info: Got shutdown request 1 while reading request"
            << std::endl;
      }

      return TGetRequestResult::GotShutdownRequest;
    }
    NO_DEFAULT_CASE;
  }

  int32_t size_field = ReadInt32FromHeader(&InputBuf[0]);

  if ((size_field < 2) ||
      (size_field > (std::numeric_limits<int32_t>::max() - 4))) {
    Out << "Error: Bad request size field" << std::endl;
    return TGetRequestResult::InvalidRequest;
  }

  size_t old_size = InputBuf.size();
  InputBuf.resize(4 + size_field);
  assert(InputBuf.size() >= old_size);

  switch (TryReadExactlyOrShutdown(ClientSocket, &InputBuf[old_size],
                                   InputBuf.size() - old_size)) {
    case TIoResult::Success: {
      break;
    }
    case TIoResult::Disconnected: {
      Out << "Error: Client unexpectedly disconnected 2 while reading request"
          << std::endl;
      return TGetRequestResult::ClientDisconnected;
    }
    case TIoResult::UnexpectedEnd:
    case TIoResult::EmptyReadUnexpectedEnd: {
      Out << "Error: Client unexpectedly disconnected 3 while reading request"
          << std::endl;
      return TGetRequestResult::ClientDisconnected;
    }
    case TIoResult::GotShutdownRequest: {
      Out << "Info: Got shutdown request 1 while reading request"
          << std::endl;
      return TGetRequestResult::GotShutdownRequest;
    }
    NO_DEFAULT_CASE;
  }

  return TGetRequestResult::GotRequest;
}

TSingleClientHandlerBase::TRequestType
TSingleClientHandlerBase::GetRequestType() {
  assert(this);
  assert(InputBuf.size() >= 6);
  int16_t api_key = ReadInt16FromHeader(&InputBuf[4]);

  switch (api_key) {
    case 0: {
      return TRequestType::ProduceRequest;
    }
    case 3: {
      return TRequestType::MetadataRequest;
    }
    default: {
      break;
    }
  }

  Out << "Error: Got unknown request type from client" << std::endl;
  return TRequestType::UnknownRequest;
}

/* Return true if we got a command, or false otherwise. */
bool TSingleClientHandlerBase::CheckProduceRequestErrorInjectionCmd(
    size_t &cmd_seq, std::string &msg_body_match, bool &match_any_msg_body,
    int16_t &ack_error, bool &disconnect, size_t &delay) {
  assert(this);
  cmd_seq = 0;
  msg_body_match.clear();
  match_any_msg_body = false;
  ack_error = 0;
  disconnect = false;
  delay = 0;  // milliseconds
  TCmd cmd;
  bool got_cmd = Ss.CmdBucket.CopyOut(cmd_seq, cmd);

  if (!got_cmd) {
    return false;
  }

  if (!cmd.ClientAddr.empty() && (cmd.ClientAddr != PeerAddressString)) {
    /* Command is directed at a specific client that differs from this one. */
    return false;
  }

  switch (cmd.Type) {
    case TCmd::TType::SEND_PRODUCE_RESPONSE_ERROR: {
      if ((cmd.Param1 < std::numeric_limits<int16_t>::min()) ||
          (cmd.Param1 > std::numeric_limits<int16_t>::max())) {
        Out << "Error: Ack error code to inject is out of range: "
            << cmd.Param1 << std::endl;
        Ss.CmdBucket.Remove(cmd_seq);
        got_cmd = false;
      } else {
        if (cmd.Param2) {
          match_any_msg_body = true;
        } else {
          msg_body_match = cmd.Str;
        }

        ack_error = static_cast<int16_t>(cmd.Param1);
      }

      break;
    }
    case TCmd::TType::DISCONNECT_ON_READ_PRODUCE_REQUEST: {
      if (cmd.Param2) {
        match_any_msg_body = true;
      } else {
        msg_body_match = cmd.Str;
      }

      disconnect = true;
      break;
    }
    case TCmd::TType::DELAY_BEFORE_SEND_PRODUCE_RESPONSE: {
      if (cmd.Param1 < 0) {
        Out << "Error: Cannot inject negative delay before produce response"
            << std::endl;
        Ss.CmdBucket.Remove(cmd_seq);
        got_cmd = false;
      } else {
        if (cmd.Param2) {
          match_any_msg_body = true;
        } else {
          msg_body_match = cmd.Str;
        }

        delay = static_cast<size_t>(cmd.Param1);
      }

      break;
    }
    default: {
      got_cmd = false;
    }
  }

  return got_cmd;
}

/* Return true if we got a command, or false otherwise. */
bool TSingleClientHandlerBase::CheckMetadataRequestErrorInjectionCmd(
    size_t &cmd_seq, bool &all_topics, std::string &topic, int16_t &error,
    bool &disconnect, size_t &delay) {
  assert(this);
  cmd_seq = 0;
  all_topics = false;
  topic.clear();
  error = 0;
  disconnect = false;
  delay = 0;  // milliseconds
  TCmd cmd;
  bool got_cmd = Ss.CmdBucket.CopyOut(cmd_seq, cmd);

  if (!got_cmd) {
    return false;
  }

  switch (cmd.Type) {
    case TCmd::TType::SEND_METADATA_RESPONSE_ERROR: {
      if ((cmd.Param1 < std::numeric_limits<int16_t>::min()) ||
          (cmd.Param1 > std::numeric_limits<int16_t>::max())) {
        Out << "Error: Metadata error code to inject is out of range: "
            << cmd.Param1 << std::endl;
        Ss.CmdBucket.Remove(cmd_seq);
        got_cmd = false;
      } else if (cmd.Param2) {
        all_topics = true;
        topic = cmd.Str;
        error = static_cast<int16_t>(cmd.Param1);
      } else {
        all_topics = false;
        topic = cmd.Str;
        error = static_cast<int16_t>(cmd.Param1);
      }

      break;
    }
    case TCmd::TType::DISCONNECT_ON_READ_METADATA_REQUEST: {
      all_topics = (cmd.Param2 != 0);
      topic = cmd.Str;
      disconnect = true;
      break;
    }
    case TCmd::TType::DELAY_BEFORE_SEND_METADATA_RESPONSE: {
      if (cmd.Param1 < 0) {
        Out << "Error: Cannot inject negative delay before metadata response"
            << std::endl;
        Ss.CmdBucket.Remove(cmd_seq);
        got_cmd = false;
      } else {
        topic = cmd.Str;
        delay = static_cast<size_t>(cmd.Param1);
      }

      break;
    }
    default: {
      got_cmd = false;
    }
  }

  return got_cmd;
}

TSingleClientHandlerBase::TAction
TSingleClientHandlerBase::ChooseMsgSetAction(const TMsgSet &msg_set,
    const std::string &topic, int32_t partition, size_t &delay,
    int16_t &ack_error) {
  assert(this);
  delay = 0;
  ack_error = 0;
  std::string msg_body_match;
  bool match_any_msg_body = false;
  size_t cmd_seq = 0;
  bool disconnect = false;

  if (CheckProduceRequestErrorInjectionCmd(cmd_seq, msg_body_match,
          match_any_msg_body, ack_error, disconnect, delay)) {
    const std::vector<TMsg> &msg_vec = msg_set.GetMsgVec();
    bool taken = false;

    for (const TMsg &msg : msg_vec) {
      if (!msg.GetCrcOk()) {
        Out << "Error: got msg with bad CRC (topic: [" << topic
            << "] partition: " << partition << ")" << std::endl;
        return TAction::DisconnectOnError;
      }

      if ((match_any_msg_body ||
           (msg.GetValue().find(msg_body_match) != std::string::npos)) &&
          (Ss.CmdBucket.Remove(cmd_seq))) {
        taken = true;
        break;
      }
    }

    if (!taken) {
      ack_error = 0;
      disconnect = false;
      delay = 0;
    }
  }

  TAction action = TAction::Respond;
  const TSetup::TPartition *part = FindPartition(topic, partition);

  if (part == nullptr) {
    ack_error = 3;  // unknown topic or partition
    action = TAction::RejectBadDest;
  } else if (disconnect) {
    action = TAction::InjectDisconnect;
  } else {
    if (ack_error) {
      action = TAction::InjectError1;
    } else if (part->AckError &&
               ((MsgSetCount % part->AckErrorInterval) == 0)) {
      ack_error = part->AckError;
      action = TAction::InjectError2;
    }
  }

  return action;
}

void TSingleClientHandlerBase::PrintCurrentMsgSet(const TMsgSet &msg_set,
    int32_t partition, TAction action, int16_t ack_error) {
  assert(this);
  const char *s =
      (msg_set.GetCompressionType() == TCompressionType::None) ? "" : "C ";
  Out << "    " << s << "partition: " << partition << " action: "
      << ActionToString(action) << " ack: " << ack_error << std::endl;
  const std::vector<TMsg> &msg_vec = msg_set.GetMsgVec();
  std::string MsgCountStr;

  for (const TMsg &msg : msg_vec) {
    ++MsgCount;
    const std::string &key = msg.GetKey();
    const std::string &value = msg.GetValue();

    if (key.empty()) {
      Out << "      n: " << MsgCount << " v: [" << value << "]" << std::endl;
    } else {
      MsgCountStr = boost::lexical_cast<std::string>(MsgCount);
      std::string pad(MsgCountStr.size(), ' ');
      Out << "      n: " << MsgCount << " k: [" << key << "]" << std::endl
          << "         " << pad << " v: [" << value << "]" << std::endl;
    }
  }
}

bool TSingleClientHandlerBase::PrepareProduceResponse(const TProdReq &prod_req,
    std::vector<TReceivedRequestTracker::TRequestInfo> &done_requests) {
  assert(this);
  done_requests.clear();
  TProduceResponseWriterApi &writer = GetProduceResponseWriter();
  int32_t corr_id = prod_req.GetCorrelationId();
  writer.OpenResponse(OutputBuf, corr_id);
  std::string topic;
  std::string msg_body_match;
  std::string msg;
  TReceivedRequestTracker::TRequestInfo req_info;
  size_t total_delay = 0;
  const std::vector<TTopicGroup> &topic_group_vec =
      prod_req.GetTopicGroupVec();
  Out << "corr ID: " << corr_id << std::endl;

  for (const TTopicGroup &topic_group : topic_group_vec) {
    topic = topic_group.GetTopic();

    if (topic.empty()) {
      Out << "Error: Produce request contains empty topic" << std::endl;
      return false;
    }

    Out << "  topic: [" << topic << "]" << " clientId: ["
        << prod_req.GetClientId() << "]" << std::endl;
    const char *topic_begin = topic.c_str();
    const char *topic_end = topic_begin + topic.size();
    writer.OpenTopic(topic_begin, topic_end);
    const std::vector<TMsgSet> &msg_set_vec = topic_group.GetMsgSetVec();

    for (const TMsgSet &msg_set : msg_set_vec) {
      ++MsgSetCount;
      int32_t partition = msg_set.GetPartition();
      size_t delay = 0;
      int16_t ack_error = 0;
      TAction action = ChooseMsgSetAction(msg_set, topic, partition, delay,
                                          ack_error);
      PrintCurrentMsgSet(msg_set, partition, action, ack_error);

      if (action == TAction::InjectDisconnect) {
        Out << "Disconnecting due to injected error" << std::endl;
        return false;
      } else if (action == TAction::DisconnectOnError) {
        return false;
      }

      total_delay += delay;
      writer.AddPartition(partition, ack_error, 0);
      req_info.ProduceRequestInfo.MakeKnown();
      TReceivedRequestTracker::TProduceRequestInfo &produce_info =
          *req_info.ProduceRequestInfo;
      produce_info.Topic = topic;
      produce_info.Partition = partition;
      produce_info.CompressionType = msg_set.GetCompressionType();
      produce_info.MsgCount = msg_set.GetMsgVec().size();
      produce_info.ReturnedErrorCode = ack_error;
      const std::vector<TMsg> &msg_vec = msg_set.GetMsgVec();

      if (!msg_vec.empty()) {
        produce_info.FirstMsgKey = msg_vec.front().GetKey();
        produce_info.FirstMsgValue = msg_vec.front().GetValue();
      }

      done_requests.push_back(std::move(req_info));
    }

    writer.CloseTopic();
  }

  writer.CloseResponse();

  if (!total_delay) {
    const TSetup::TPort &port = Setup.Ports[PortOffset];

    if (port.AckDelay &&
        ((ProduceRequestCount % port.AckDelayInterval) == 0)) {
      total_delay = port.AckDelay;
    }
  }

  if (total_delay) {
    Out << "delay " << total_delay << " ms: corr " << corr_id << std::endl;

    if (GetShutdownRequestFd().IsReadable(total_delay)) {
      return false;
    }
  }

  return true;
}

bool TSingleClientHandlerBase::HandleProduceRequest() {
  assert(this);

  struct reader_t final {
    explicit reader_t(TProduceRequestReaderApi &reader) : Reader(reader) { }

    ~reader_t() noexcept { Reader.Clear(); }  // not strictly necessary

    TProduceRequestReaderApi &Reader;
  } r(GetProduceRequestReader());

  std::vector<TReceivedRequestTracker::TRequestInfo> done_requests;
  TOpt<TProdReq> opt_prod_req;

  try {
    opt_prod_req.MakeKnown(
        TProdReqBuilder(r.Reader, GetMsgSetReader())
            .BuildProdReq(&InputBuf[0], InputBuf.size()));

    if (!PrepareProduceResponse(*opt_prod_req, done_requests)) {
      return false;
    }
  } catch (const TProduceRequestReaderApi::TBadProduceRequest &x) {
    Out << "Error: Got bad produce request: " << x.what() << std::endl;;
    return false;
  } catch (const TMsgSetReaderApi::TBadMsgSet &x) {
    Out << "Error: Got bad produce request: " << x.what() << std::endl;;
    return false;
  }

  switch (TryWriteExactlyOrShutdown(ClientSocket, &OutputBuf[0],
                                    OutputBuf.size())) {
    case TIoResult::Success: {
      for (size_t i = 0; i < done_requests.size(); ++i) {
        Ss.ReceivedRequests.PutRequestInfo(std::move(done_requests[i]));
      }

      break;
    }
    case TIoResult::Disconnected: {
      Out << "Error: Got disconnected from client while sending produce "
          << "response" << std::endl;
      return false;
    }
    case TIoResult::UnexpectedEnd:
    case TIoResult::EmptyReadUnexpectedEnd: {
      Out << "Error: Got disconnected unexpectedly from client while sending "
          << "produce response" << std::endl;
      return false;
    }
    case TIoResult::GotShutdownRequest: {
      Out << "Info: Got shutdown request while sending produce response"
          << std::endl;
      return false;
    }
    NO_DEFAULT_CASE;
  }

  return true;
}

bool TSingleClientHandlerBase::HandleMetadataRequest() {
  assert(this);
  size_t cmd_seq = 0;
  bool all_topics = false;
  std::string error_topic;
  std::string topic_match;
  int16_t error = 0;
  bool disconnect = false;
  size_t delay = 0;  // milliseconds
  bool got_cmd = CheckMetadataRequestErrorInjectionCmd(cmd_seq, all_topics,
      error_topic, error, disconnect, delay);

  if (got_cmd && !all_topics) {
    topic_match = error_topic;
  }

  if (!ValidateMetadataRequest(MetadataRequest)) {
    Out << "Error: Server closing connection due to bad metadata request"
        << std::endl;
    return false;
  }

  if (got_cmd &&
      ((!all_topics && (topic_match != MetadataRequest.Topic)) ||
       !Ss.CmdBucket.Remove(cmd_seq))) {
    got_cmd = false;
    error = 0;
    disconnect = false;
    delay = 0;
  }

  if (disconnect) {
    PrintMdReq(MetadataRequestCount, MetadataRequest,
               TAction::InjectDisconnect, error_topic, error, 0);

    if (Ss.TrackReceivedRequests) {
      TReceivedRequestTracker::TRequestInfo info;
      info.MetadataRequestInfo.MakeKnown();
      TReceivedRequestTracker::TMetadataRequestInfo &md_info =
          *info.MetadataRequestInfo;
      md_info.Topic = MetadataRequest.Topic;
      md_info.ReturnedErrorCode = 0;
      Ss.ReceivedRequests.PutRequestInfo(std::move(info));
    }
    return false;
  }

  if (!delay) {
    const TSetup::TPort &port = Setup.Ports[PortOffset];

    if (port.AckDelay &&
        ((MetadataRequestCount % port.AckDelayInterval) == 0)) {
      delay = port.AckDelay;
    }
  }

  if (delay && GetShutdownRequestFd().IsReadable(delay)) {
    Out << "Got shutdown request while handling metadata request" << std::endl;
    return false;
  }

  switch (SendMetadataResponse(MetadataRequest, error, error_topic, delay)) {
    case TSendMetadataResult::SentMetadata: {
      if (Ss.TrackReceivedRequests) {
        TReceivedRequestTracker::TRequestInfo info;
        info.MetadataRequestInfo.MakeKnown();
        TReceivedRequestTracker::TMetadataRequestInfo &md_info =
            *info.MetadataRequestInfo;
        md_info.Topic = MetadataRequest.Topic;
        md_info.ReturnedErrorCode = error;
        Ss.ReceivedRequests.PutRequestInfo(std::move(info));
      }
      break;
    }
    case TSendMetadataResult::GotShutdownRequest:
    case TSendMetadataResult::ClientDisconnected: {
      return false;
    }
    NO_DEFAULT_CASE;
  }

  return true;
}

const TSetup::TPartition *TSingleClientHandlerBase::FindPartition(
    const std::string &topic, int32_t partition) const {
  assert(this);
  auto iter = Setup.Topics.find(topic);

  if (iter == Setup.Topics.end()) {
    return nullptr;
  }

  const TSetup::TTopic &t = iter->second;

  if (static_cast<size_t>(partition) >= t.Partitions.size()) {
    return nullptr;
  }

  size_t topic_port_offset =
      (t.FirstPortOffset + partition) % Setup.Ports.size();

  if (topic_port_offset != PortOffset) {
    return nullptr;  // this partition lives on a different "broker"
  }

  return &t.Partitions[partition];
}

void TSingleClientHandlerBase::DoRun() {
  assert(this);
  const TSetup::TPort &port = Setup.Ports[PortOffset];
  ProduceRequestCount = 0;
  MetadataRequestCount = 0;
  MsgSetCount = 0;
  MsgCount = 0;
  const TFd &shutdown_request_fd = GetShutdownRequestFd();

  while (!shutdown_request_fd.IsReadable()) {
    if (port.ReadDelay &&
        (((ProduceRequestCount + MetadataRequestCount + 1) %
          port.ReadDelayInterval) == 0)) {
      if (Config.QuietLevel <= 1) {
        Out << "Info: Sleeping " << port.ReadDelay
            << " milliseconds before reading next request." << std::endl;
      }

      if (shutdown_request_fd.IsReadable(port.ReadDelay)) {
        break;
      }
    }

    bool done = false;

    switch (GetRequest()) {
      case TGetRequestResult::GotRequest: {
        break;
      }
      case TGetRequestResult::GotShutdownRequest:
      case TGetRequestResult::ClientDisconnected:
      case TGetRequestResult::InvalidRequest: {
        done = true;
        break;
      }
      NO_DEFAULT_CASE;
    }

    if (done) {
      break;
    }

    switch (GetRequestType()) {
      case TRequestType::UnknownRequest: {
        done = true;
        break;
      }
      case TRequestType::ProduceRequest: {
        ++ProduceRequestCount;
        done = !HandleProduceRequest();
        break;
      }
      case TRequestType::MetadataRequest: {
        if (!ValidateMetadataRequestHeader()) {
          Out << "Error: Exiting due to invalid metadata request" << std::endl;
          done = true;
          break;
        }

        ++MetadataRequestCount;
        done = !HandleMetadataRequest();
        break;
      }
      NO_DEFAULT_CASE;
    }

    if (done) {
      break;
    }
  }

  if ((Config.QuietLevel <= 2) && (!Config.SingleOutputFile)) {
    Out << "end: ==============================================" << std::endl
        << std::endl
        << "final counts:" << std::endl
        << "produce requests: " << ProduceRequestCount << std::endl
        << "metadata requests: " << MetadataRequestCount << std::endl
        << "message sets: " << MsgSetCount << std::endl
        << "messages: " << MsgCount << std::endl
        << std::endl;
  }
}
