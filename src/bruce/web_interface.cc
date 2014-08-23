/* <bruce/web_interface.cc>

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

   Implements <bruce/web_interface.h>.
 */

#include <bruce/web_interface.h>

#include <algorithm>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <vector>

#include <sys/types.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include <base/error_utils.h>
#include <base/no_default_case.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/util/time_util.h>
#include <bruce/version.h>
#include <server/counter.h>
#include <server/url_decode.h>
#include <signal/masker.h>
#include <signal/set.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Debug;
using namespace Bruce::Util;
using namespace Server;

SERVER_COUNTER(MongooseEventLog);
SERVER_COUNTER(MongooseGetVersionRequest);
SERVER_COUNTER(MongooseGetCountersRequest);
SERVER_COUNTER(MongooseGetDiscardsRequest);
SERVER_COUNTER(MongooseGetMetadataFetchTimeRequest);
SERVER_COUNTER(MongooseGetMsgStatsRequest);
SERVER_COUNTER(MongooseHttpRequest);
SERVER_COUNTER(MongooseStdException);
SERVER_COUNTER(MongooseUnknownException);
SERVER_COUNTER(MongooseUrlDecodeError);

const char *TWebInterface::ToErrorBlurb(TRequestType request_type) {
  switch (request_type) {
    case TRequestType::UNIMPLEMENTED_REQUEST_METHOD: {
      break;
    }
    case TRequestType::UNKNOWN_GET_REQUEST: {
      return "Unknown GET request";
    }
    case TRequestType::UNKNOWN_POST_REQUEST: {
      return "Unknown POST request";
    }
    case TRequestType::TOP_LEVEL_PAGE: {
      return "Top level page";
    }
    case TRequestType::GET_VERSION: {
      return "Get version";
    }
    case TRequestType::GET_COUNTERS: {
      return "Get counters";
    }
    case TRequestType::GET_DISCARDS: {
      return "Get discards";
    }
    case TRequestType::GET_METADATA_FETCH_TIME: {
      return "Get metadata fetch time";
    }
    case TRequestType::GET_MSG_STATS: {
      return "Get msg stats";
    }
    case TRequestType::MSG_DEBUG_GET_TOPICS: {
      return "Msg debug get topics";
    }
    case TRequestType::MSG_DEBUG_ADD_ALL_TOPICS: {
      return "Msg debug add all topics";
    }
    case TRequestType::MSG_DEBUG_DEL_ALL_TOPICS: {
      return "Msg debug del all topics";
    }
    case TRequestType::MSG_DEBUG_TRUNCATE_FILES: {
      return "Msg debug truncate files";
    }
    case TRequestType::MSG_DEBUG_ADD_TOPIC: {
      return "Msg debug add topic";
    }
    case TRequestType::MSG_DEBUG_DEL_TOPIC: {
      return "Msg debug del topic";
    }
    case TRequestType::METADATA_UPDATE: {
      return "Metadata update";
    }
    NO_DEFAULT_CASE;
  }

  return "Unimplemented request method";
}

void *TWebInterface::OnEvent(mg_event event, mg_connection *conn,
    const mg_request_info *request_info) {
  bool is_handled = false;
  TRequestType request_type = TRequestType::UNIMPLEMENTED_REQUEST_METHOD;
  const char *error_blurb = "";

  try {
    switch (event) {
      case MG_NEW_REQUEST: {
        MongooseHttpRequest.Increment();

        try {
          HandleHttpRequest(conn, request_info, request_type);
        } catch (...) {
          error_blurb = ToErrorBlurb(request_type);
          throw;
        }

        is_handled = true;
        break;
      }
      case MG_EVENT_LOG: {
        error_blurb = "Mongoose event log";
        MongooseEventLog.Increment();
        syslog(LOG_ERR, "Mongoose error: %s %s %s", request_info->log_message,
               request_info->uri, request_info->query_string);
        is_handled = true;
        break;
      }
      default: {
        break;
      }
    }
  } catch (const TUrlDecodeError &x) {
    MongooseUrlDecodeError.Increment();
    mg_printf(conn, "HTTP/1.1 400 BAD REQUEST\r\n"
                    "Content-Type: text/plain\r\n\r\n"
                    "[URL decode error: %s at query string offset %d][%s]",
              x.what(), x.GetOffset(), error_blurb);
    is_handled = true;
  } catch (const std::exception &x) {
    MongooseStdException.Increment();
    mg_printf(conn, "HTTP/1.1 400 BAD REQUEST\r\n"
                    "Content-Type: text/plain\r\n\r\n[%s][std::exception][%s]",
              error_blurb, x.what());
    is_handled = true;
  } catch (...) {
    MongooseUnknownException.Increment();
    mg_printf(conn, "HTTP/1.1 400 BAD REQUEST\r\n"
                    "Content-Type: text/plain\r\n\r\n[%s][unknown exception]",
              error_blurb);
    is_handled = true;
  }

  return is_handled ? const_cast<char *>("") : nullptr;
}

/* Size of string buffer to use for converting time_t (seconds since epoch)
   values to human-readable form.  According to the man page for ctime_r(),
   this must be at least 26 bytes, but we round up to the next power of 2. */
enum { TIME_BUF_SIZE = 32 };

/* Convert 'seconds_since_epoch' to a human-friendly string, placing the result
   in 'time_buf'.  Parameter 'time_buf' should have room for at least 26 bytes,
   according to the man page for ctime_r(). */
static void FillTimeBuf(time_t seconds_since_epoch, char time_buf[]) {
  assert(time_buf);

  if (ctime_r(&seconds_since_epoch, time_buf) == nullptr) {
    time_buf[0] = '\0';
  }

  size_t i = std::strlen(time_buf);

  /* Eliminate trailing newline. */
  if (i) {
    --i;

    if (time_buf[i] == '\n') {
        time_buf[i] = '\0';
    }
  }
}

void TWebInterface::HandleGetServerInfoRequestCompact(std::ostream &os) {
  MongooseGetVersionRequest.Increment();
  uint64_t now = GetEpochSeconds();
  char time_buf[TIME_BUF_SIZE];
  FillTimeBuf(now, time_buf);
  os << "pid: " << getpid() << std::endl
      << "now: " << now << " " << time_buf << std::endl
      << "version: " << GetVersion() << std::endl;
}

void TWebInterface::HandleGetServerInfoRequestJson(std::ostream &os) {
  MongooseGetVersionRequest.Increment();
  uint64_t now = GetEpochSeconds();
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"now\": " << now << "," << std::endl
        << ind1 << "\"version\": \"" << GetVersion() << "\"" << std::endl;
  }

  os << ind0 << "}" << std::endl;
}

void TWebInterface::HandleGetCountersRequestCompact(std::ostream &os) {
  MongooseGetCountersRequest.Increment();
  TCounter::Sample();
  time_t sample_time = TCounter::GetSampleTime();
  time_t reset_time = TCounter::GetResetTime();
  char sample_time_buf[TIME_BUF_SIZE], reset_time_buf[TIME_BUF_SIZE];
  FillTimeBuf(sample_time, sample_time_buf);
  FillTimeBuf(reset_time, reset_time_buf);
  os << "now=" << sample_time << " " << sample_time_buf << std::endl
      << "since=" << reset_time << " " << reset_time_buf << std::endl
      << "pid=" << getpid() << std::endl
      << "version=" << GetVersion() << std::endl
      << std::endl;

  for (const TCounter *counter = TCounter::GetFirstCounter();
       counter != nullptr;
       counter = counter->GetNextCounter()) {
    os << counter->GetCodeLocation() << "." << counter->GetName() << "="
        << counter->GetCount() << std::endl;
  }
}

void TWebInterface::HandleGetCountersRequestJson(std::ostream &os) {
  MongooseGetCountersRequest.Increment();
  TCounter::Sample();
  time_t sample_time = TCounter::GetSampleTime();
  time_t reset_time = TCounter::GetResetTime();
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"now\": " << sample_time << "," << std::endl
        << ind1 << "\"since: \"" << reset_time << "," << std::endl
        << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"version\": \"" << GetVersion() << "\"," << std::endl
        << ind1 << "\"counters\": [" << std::endl;

    {
      TIndent ind2(ind1);

      for (const TCounter *counter = TCounter::GetFirstCounter(),
               *next = nullptr;
           counter != nullptr;
           counter = next) {
        next = counter->GetNextCounter();
        os << ind2 << "{" << std::endl;

        {
          TIndent ind3(ind2);
          os << ind3 << "\"location\": \"" << counter->GetCodeLocation()
              << "\"," << std::endl
              << ind3 << "\"name\": \"" << counter->GetName() << "\","
              << std::endl
              << ind3 << "\"value\": " << counter->GetCount() << std::endl;
        }

        os << ind2 << (next ? "}," : "}") << std::endl;
      }
    }

    os << ind1 << "]" << std::endl;
  }

  os << ind0 << "}" << std::endl;
}

void TWebInterface::WriteDiscardReportCompact(std::ostream &os,
    const TAnomalyTracker::TInfo &info) {
  char time_buf[TIME_BUF_SIZE];
  uint64_t start_time = info.GetStartTime();
  FillTimeBuf(start_time, time_buf);

  os << "    report ID: " << info.GetReportId() << std::endl
      << "    start time: " << start_time << " " << time_buf << std::endl
      << "    malformed msg count: " << info.MalformedMsgCount << std::endl
      << "    unsupported API key msg count: "
      << info.UnsupportedApiKeyMsgCount << std::endl
      << "    unsupported version msg count: "
      << info.UnsupportedVersionMsgCount << std::endl
      << "    bad topic msg count: " << info.BadTopicMsgCount << std::endl
      << std::endl;

  if (UseOldInputFormat) {
    for (const std::string &msg : info.MalformedMsgs) {
      os << "    recent malformed msg: " << msg.size() << "[" << msg << "]"
          << std::endl;
    }
  }

  if (!info.MalformedMsgs.empty()) {
    os << std::endl;
  }

  for (const std::pair<int, size_t> &item : info.UnsupportedVersionMsgs) {
    os << "    unsupported msg version: " << item.first << " count: "
        << item.second << std::endl;
  }

  if (!info.UnsupportedVersionMsgs.empty()) {
    os << std::endl;
  }

  for (const std::string &topic : info.BadTopics) {
    os << "    recent bad topic: " << topic.size() << "[" << topic << "]"
        << std::endl;
  }

  if (!info.BadTopics.empty()) {
    os << std::endl;
  }

  for (const std::string &msg : info.LongMsgs) {
    os << "    recent too long msg: " << msg.size() << "[" << msg << "]"
        << std::endl;
  }

  if (!info.LongMsgs.empty()) {
    os << std::endl;
  }

  for (auto &x : info.DiscardTopicMap) {
    const TAnomalyTracker::TTopicInfo &topic_info = x.second;
    os << "    discard topic: " << x.first.size() << "[" << x.first
        << "] begin [" << topic_info.Interval.First << "] end ["
        << topic_info.Interval.Last << "] count " << topic_info.Count
        << std::endl;
  }

  if (!info.DiscardTopicMap.empty()) {
    os << std::endl;
  }

  for (auto &x : info.DuplicateTopicMap) {
    const TAnomalyTracker::TTopicInfo &topic_info = x.second;
    os << "    possible duplicate topic: " << x.first.size() << "[" << x.first
        << "] begin [" << topic_info.Interval.First << "] end ["
        << topic_info.Interval.Last << "] count " << topic_info.Count
        << std::endl;
  }
}

void TWebInterface::WriteDiscardReportJson(std::ostream &os,
    const TAnomalyTracker::TInfo &info, TIndent &ind0) {
  uint64_t start_time = info.GetStartTime();
  os << ind0 << "\"id\": " << info.GetReportId() << "," << std::endl
      << ind0 << "\"start_time\": " << start_time << "," << std::endl
      << ind0 << "\"malformed_msg_count\": " << info.MalformedMsgCount << ","
      << std::endl
      << ind0 << "\"unsupported_api_key_msg_count\": "
      << info.UnsupportedApiKeyMsgCount << "," << std::endl
      << ind0 << "\"unsupported_version_msg_count\": "
      << info.UnsupportedVersionMsgCount << "," << std::endl
      << ind0 << "\"bad_topic_msg_count\": " << info.BadTopicMsgCount << ","
      << std::endl
      << ind0 << "\"recent_malformed\": [" << std::endl;

  {
    TIndent ind1(ind0);
    bool first_time = true;

    for (const std::string &msg : info.MalformedMsgs) {
      if (!first_time) {
        os << "," << std::endl;
      }

      /* TODO: base64 encode msg */
      os << ind1 << "\"" << msg << "\"";
      first_time = false;
    }

    if (!first_time) {
      os << std::endl;
    }
  }

  os << ind0 << "]," << std::endl
      << ind0 << "\"unsupported_msg_version\": [" << std::endl;

  {
    TIndent ind1(ind0);
    bool first_time = true;

    for (const std::pair<int, size_t> &item : info.UnsupportedVersionMsgs) {
      if (!first_time) {
        os << "," << std::endl;
      }

      os << ind1 << "{" << std::endl;

      {
        TIndent ind2(ind1);
        os << ind2 << "\"version\": " << item.first << "," << std::endl
            << ind2 << "\"count\": " << item.second << std::endl;
      }

      os << ind1 << "}";
      first_time = false;
    }

    if (!first_time) {
      os << std::endl;
    }
  }

  os << ind0 << "]," << std::endl
      << ind0 << "\"recent_bad_topic\": [" << std::endl;

  {
    TIndent ind1(ind0);
    bool first_time = true;

    for (const std::string &topic : info.BadTopics) {
      if (!first_time) {
        os << "," << std::endl;
      }

      os << ind1 << "\"" << topic << "\"";
      first_time = false;
    }

    if (!first_time) {
      os << std::endl;
    }
  }

  os << ind0 << "]," << std::endl
      << ind0 << "\"recent_too_long_msg\": [" << std::endl;

  {
    TIndent ind1(ind0);
    bool first_time = true;

    for (const std::string &msg : info.LongMsgs) {
      if (!first_time) {
        os << "," << std::endl;
      }

      /* TODO: base64 encode msg */
      os << ind1 << "\"" << msg << "\"";
      first_time = false;
    }

    if (!first_time) {
      os << std::endl;
    }
  }

  os << ind0 << "]," << std::endl
      << ind0 << "\"discard_topic\": [" << std::endl;

  {
    TIndent ind1(ind0);
    bool first_time = true;

    for (auto &x : info.DiscardTopicMap) {
      if (!first_time) {
        os << "," << std::endl;
      }

      os << ind1 << "{" << std::endl;

      {
        const TAnomalyTracker::TTopicInfo &topic_info = x.second;
        TIndent ind2(ind1);
        os << ind2 << "\"topic\": \"" << x.first << "\"," << std::endl
            << ind2 << "\"min_timestamp\": " << topic_info.Interval.First
            << "," << std::endl
            << ind2 << "\"max_timestamp\": " << topic_info.Interval.Last << ","
            << std::endl
            << ind2 << "\"count\": " << topic_info.Count << std::endl;
      }

      os << ind1 << "}";
      first_time = false;
    }

    if (!first_time) {
      os << std::endl;
    }
  }

  os << ind0 << "]," << std::endl
      << ind0 << "\"possible_duplicate_topic\": [" << std::endl;

  {
    TIndent ind1(ind0);
    bool first_time = true;

    for (auto &x : info.DiscardTopicMap) {
      if (!first_time) {
        os << "," << std::endl;
      }

      os << ind1 << "{" << std::endl;

      {
        const TAnomalyTracker::TTopicInfo &topic_info = x.second;
        TIndent ind2(ind1);
        os << ind2 << "\"topic\": \"" << x.first << "\"," << std::endl
            << ind2 << "\"min_timestamp\": " << topic_info.Interval.First
            << "," << std::endl
            << ind2 << "\"max_timestamp\": " << topic_info.Interval.Last << ","
            << std::endl
            << ind2 << "\"count\": " << topic_info.Count << std::endl;
      }

      os << ind1 << "}";
      first_time = false;
    }

    if (!first_time) {
      os << std::endl;
    }
  }

  os << ind0 << "]" << std::endl;
}

void TWebInterface::HandleGetDiscardsRequestCompact(std::ostream &os) {
  MongooseGetDiscardsRequest.Increment();
  uint64_t now = GetEpochSeconds();
  char time_buf[TIME_BUF_SIZE];
  FillTimeBuf(now, time_buf);
  TAnomalyTracker::TInfo current_unfinished;
  std::shared_ptr<const TAnomalyTracker::TInfo> latest_finished =
      AnomalyTracker.GetInfo(current_unfinished);
  os << "pid: " << getpid() << std::endl
      << "now: " << now << " " << time_buf << std::endl
      << "version: " << GetVersion() << std::endl
      << "report interval in seconds: " << AnomalyTracker.GetReportInterval()
      << std::endl << std::endl
      << "current (unfinished) reporting period:" << std::endl;
  WriteDiscardReportCompact(os, current_unfinished);

  if (latest_finished) {
    os << std::endl << "latest finished reporting period:" << std::endl;
    WriteDiscardReportCompact(os, *latest_finished);
  }
}

void TWebInterface::HandleGetDiscardsRequestJson(std::ostream &os) {
  MongooseGetDiscardsRequest.Increment();
  uint64_t now = GetEpochSeconds();
  TAnomalyTracker::TInfo current_unfinished;
  std::shared_ptr<const TAnomalyTracker::TInfo> latest_finished =
      AnomalyTracker.GetInfo(current_unfinished);
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"now\": " << now << "," << std::endl
        << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"version\": \"" << GetVersion() << "\"," << std::endl
        << ind1 << "\"interval\": " << AnomalyTracker.GetReportInterval()
        << "," << std::endl
        << ind1 << "\"unfinished_report\": {" << std::endl;

    {
      TIndent ind2(ind1);
      WriteDiscardReportJson(os, current_unfinished, ind2);
    }

    os << ind1 << "}";

    if (latest_finished) {
      os << "," << std::endl
          << ind1 << "\"finished_report\": {" << std::endl;

      {
        TIndent ind2(ind1);
        WriteDiscardReportJson(os, *latest_finished, ind2);
      }

      os << ind1 << "}";
    }

    os << std::endl;
  }

  os << ind0 << "}" << std::endl;
}

void TWebInterface::HandleMetadataFetchTimeRequestCompact(std::ostream &os) {
  MongooseGetMetadataFetchTimeRequest.Increment();
  uint64_t last_update_time = 0, last_modified_time = 0;
  MetadataTimestamp.GetTimes(last_update_time, last_modified_time);
  uint64_t now = GetEpochMilliseconds();
  char last_update_time_buf[TIME_BUF_SIZE],
      last_modified_time_buf[TIME_BUF_SIZE], now_time_buf[TIME_BUF_SIZE];
  FillTimeBuf(last_update_time / 1000, last_update_time_buf);
  FillTimeBuf(last_modified_time / 1000, last_modified_time_buf);
  FillTimeBuf(now / 1000, now_time_buf);
  os << "pid: " << getpid() << std::endl
      << "version: " << GetVersion() << std::endl
      << "now (milliseconds since epoch): " << now << " " << now_time_buf
      << std::endl
      << "metadata last updated at (milliseconds since epoch): "
      << last_update_time << " " << last_update_time_buf << std::endl
      << "metadata last modified at (milliseconds since epoch): "
      << last_modified_time << " " << last_modified_time_buf << std::endl;
}

void TWebInterface::HandleMetadataFetchTimeRequestJson(std::ostream &os) {
  MongooseGetMetadataFetchTimeRequest.Increment();
  uint64_t last_update_time = 0, last_modified_time = 0;
  MetadataTimestamp.GetTimes(last_update_time, last_modified_time);
  uint64_t now = GetEpochMilliseconds();
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"version\": \"" << GetVersion() << "\"," << std::endl
        << ind1 << "\"now\": " << now << "," << std::endl
        << ind1 << "\"last_updated\": " << last_update_time << "," << std::endl
        << ind1 << "\"last_modified\": " << last_modified_time << ","
        << std::endl;
  }

  os << ind0 << "}" << std::endl;
}

void TWebInterface::HandleMsgStatsRequestCompact(std::ostream &os) {
  MongooseGetMsgStatsRequest.Increment();
  std::vector<TMsgStateTracker::TTopicStatsItem> topic_stats;
  long new_count = 0;
  MsgStateTracker.GetStats(topic_stats, new_count);
  uint64_t now = GetEpochSeconds();
  char time_buf[TIME_BUF_SIZE];
  FillTimeBuf(now, time_buf);
  os << "pid: " << getpid() << std::endl
      << "now: " << now << " " << time_buf << std::endl
      << "version: " << GetVersion() << std::endl << std::endl;
  long total_send_wait = 0;
  long total_ack_wait = 0;

  for (const auto &item : topic_stats) {
    total_send_wait += item.second.SendWaitCount;
    total_ack_wait += item.second.AckWaitCount;
    os << "queued: " << std::setw(10)
        << (item.second.SendWaitCount + item.second.AckWaitCount)
        << "  send_wait: " << std::setw(10) << item.second.SendWaitCount
        << "  ack_wait: " << std::setw(10) << item.second.AckWaitCount
        << "  topic: [" << item.first << "]" << std::endl;
  }

  if (!topic_stats.empty()) {
    os << std::endl;
  }

  long total_queued = total_send_wait + total_ack_wait;
  os << std::setw(10) << total_queued << " total queued (send_wait + ack_wait)"
      << std::endl
      << std::setw(10) << total_send_wait << " total send_wait" << std::endl
      << std::setw(10) << total_ack_wait << " total ack_wait" << std::endl
      << std::setw(10) << new_count << " total new" << std::endl
      << std::setw(10) << total_queued + new_count
      << " total (all states: new + send_wait + ack_wait)" << std::endl;
}

void TWebInterface::HandleMsgStatsRequestJson(std::ostream &os) {
  MongooseGetMsgStatsRequest.Increment();
  std::vector<TMsgStateTracker::TTopicStatsItem> topic_stats;
  long new_count = 0;
  MsgStateTracker.GetStats(topic_stats, new_count);
  uint64_t now = GetEpochSeconds();
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"now\": " << now << "," << std::endl
        << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"version\": \"" << GetVersion() << "\"," << std::endl
        << ind1 << "\"sending\": [";

    {
      TIndent ind2(ind1);
      bool first_time = true;

      for (const auto &item : topic_stats) {
        if (!first_time) {
          os << "," << std::endl;
        }

        os << ind2 << "{" << std::endl;

        {
          TIndent ind3(ind2);
          os << ind3 << "\"topic\": \"" << item.first << "\"," << std::endl
              << ind3 << "\"send_wait\": " << item.second.SendWaitCount << ","
              << std::endl
              << ind3 << "\"ack_wait\": " << item.second.AckWaitCount
              << std::endl;
        }

        os << ind2 << "}";
      }

      os << std::endl;
    }

    os << ind1 << "]," << std::endl
        << ind1 << "\"new\": " << new_count << std::endl;
  }

  os << ind0 << "}" << std::endl;
}

void TWebInterface::HandleHttpRequest(mg_connection *conn,
    const mg_request_info *request_info, TRequestType &request_type) {
  /* For each request type handled below, set this as soon as the request type
     is identified.  Then the caller will have that information for error
     reporting even if an exception is thrown. */
  request_type = TRequestType::UNIMPLEMENTED_REQUEST_METHOD;

  if (!std::strcmp(request_info->request_method, "GET")) {
    static const char add_debug_topic_prefix[] =
        "/msg_debug/add_topic/";
    static const char del_debug_topic_prefix[] =
        "/msg_debug/del_topic/";
    static const size_t add_debug_topic_prefix_len =
        std::strlen(add_debug_topic_prefix);
    static const size_t del_debug_topic_prefix_len =
        std::strlen(del_debug_topic_prefix);

    if (!std::strcmp(request_info->uri, "/server_info/compact")) {
      request_type = TRequestType::GET_VERSION;
      std::ostringstream oss;
      HandleGetServerInfoRequestCompact(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/server_info/json")) {
      request_type = TRequestType::GET_VERSION;
      std::ostringstream oss;
      HandleGetServerInfoRequestJson(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/counters/compact")) {
      request_type = TRequestType::GET_COUNTERS;
      std::ostringstream oss;
      HandleGetCountersRequestCompact(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/counters/json")) {
      request_type = TRequestType::GET_COUNTERS;
      std::ostringstream oss;
      HandleGetCountersRequestJson(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/discards/compact")) {
      request_type = TRequestType::GET_DISCARDS;
      std::ostringstream oss;
      HandleGetDiscardsRequestCompact(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/discards/json")) {
      request_type = TRequestType::GET_DISCARDS;
      std::ostringstream oss;
      HandleGetDiscardsRequestJson(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri,
                            "/metadata_fetch_time/compact")) {
      request_type = TRequestType::GET_METADATA_FETCH_TIME;
      std::ostringstream oss;
      HandleMetadataFetchTimeRequestCompact(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri,
                            "/metadata_fetch_time/json")) {
      request_type = TRequestType::GET_METADATA_FETCH_TIME;
      std::ostringstream oss;
      HandleMetadataFetchTimeRequestJson(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/queues/compact")) {
      request_type = TRequestType::GET_MSG_STATS;
      std::ostringstream oss;
      HandleMsgStatsRequestCompact(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/queues/json")) {
      request_type = TRequestType::GET_MSG_STATS;
      std::ostringstream oss;
      HandleMsgStatsRequestJson(oss);
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/msg_debug/get_topics")) {
      request_type = TRequestType::MSG_DEBUG_GET_TOPICS;
      TDebugSetup::TSettings::TPtr settings = DebugSetup.GetSettings();
      assert(settings);
      const std::unordered_set<std::string> *topics =
          settings->GetDebugTopics();
      std::ostringstream oss;

      if (topics == nullptr) {
        oss << "all topics enabled" << std::endl;
      } else {
        if (topics->empty()) {
          oss << "no topics enabled" << std::endl;
        } else {
          for (const std::string &topic : *topics) {
            oss << "topic: [" << topic << "]" << std::endl;
          }
        }
      }

      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri,
                            "/msg_debug/add_all_topics")) {
      request_type = TRequestType::MSG_DEBUG_ADD_ALL_TOPICS;
      DebugSetup.SetDebugTopics(nullptr);
      std::string response("All topics enabled\n");
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/msg_debug/del_all_topics")) {
      request_type = TRequestType::MSG_DEBUG_DEL_ALL_TOPICS;
      DebugSetup.ClearDebugTopics();
      std::string response("All topics deleted\n");
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strcmp(request_info->uri, "/msg_debug/truncate_files")) {
      request_type = TRequestType::MSG_DEBUG_TRUNCATE_FILES;
      DebugSetup.TruncateDebugFiles();
      std::string response("Message debug files truncated\n");
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strncmp(request_info->uri, add_debug_topic_prefix,
                             add_debug_topic_prefix_len)) {
      request_type = TRequestType::MSG_DEBUG_ADD_TOPIC;
      const char *topic = request_info->uri + add_debug_topic_prefix_len;
      bool success = DebugSetup.AddDebugTopic(topic);
      std::string response(success ? "Added topic " : "Failed to add topic ");
      response += topic;
      response += '\n';
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else if (!std::strncmp(request_info->uri, del_debug_topic_prefix,
                             del_debug_topic_prefix_len)) {
      request_type = TRequestType::MSG_DEBUG_DEL_TOPIC;
      const char *topic = request_info->uri + del_debug_topic_prefix_len;
      bool success = DebugSetup.DelDebugTopic(topic);
      std::string response(success ? "Deleted topic " :
                                     "Failed to delete topic ");
      response += topic;
      response += '\n';
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else {
      request_type = std::strcmp(request_info->uri, "/") ?
          TRequestType::UNKNOWN_GET_REQUEST : TRequestType::TOP_LEVEL_PAGE;

      const std::string response("\
<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\"\n\
    \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">\n\
<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">\n\
  <head>\n\
    <title>Bruce</title>\n\
  </head>\n\
  <body>\n\
    <h1>Server Status</h1>\n\
    <div>\n\
      Get server info: [<a href=\"/server_info/compact\">compact</a>]\
[<a href=\"/server_info/json\">JSON</a>]<br/>\n\
      Get counter values: [<a href=\"/counters/compact\">compact</a>]\
[<a href=\"/counters/json\">JSON</a>]<br/>\n\
      Get discard info: [<a href=\"/discards/compact\">compact</a>]\
[<a href=\"/discards/json\">JSON</a>]<br/>\n\
      Get queued message info: [<a href=\"/queues/compact\">compact</a>]\
[<a href=\"/queues/json\">JSON</a>]<br/>\n\
      Get metadata fetch time: [<a href=\"/metadata_fetch_time/compact\">\
compact</a>][<a href=\"/metadata_fetch_time/json\">JSON</a>]<br/>\n\
    </div>\n\
    <h1>Server Management</h1>\n\
    <form action=\"/metadata_update\" method=\"post\">\n\
      <div>\n\
        <input type=\"submit\" value=\"Update Metadata\"/>\n\
      </div>\n\
    </form>\n\
  </body>\n\
</html>\n");
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    }
  } else if (!std::strcmp(request_info->request_method, "POST")) {
    if (!std::strcmp(request_info->uri, "/metadata_update")) {
      request_type = TRequestType::METADATA_UPDATE;
      MetadataUpdateRequestSem.Push();
      uint64_t now = GetEpochSeconds();
      char time_buf[TIME_BUF_SIZE];
      FillTimeBuf(now, time_buf);
      std::ostringstream oss;
      oss << "Metadata update initiated at " << now << " " << time_buf
          << std::endl;
      std::string response(oss.str());
      mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                      "Content-Length: %d\r\n\r\n",
                response.size());
      mg_write(conn, response.data(), response.size());
    } else {
      request_type = TRequestType::UNKNOWN_POST_REQUEST;
      mg_printf(conn, "HTTP/1.1 404 NOT FOUND\r\n"
                      "Content-Type: text/plain\r\n\r\n"
                      "[not found: try /metadata_update]");
    }
  } else {
    request_type = TRequestType::UNIMPLEMENTED_REQUEST_METHOD;
    mg_printf(conn, "HTTP/1.1 501 NOT IMPLEMENTED\r\n"
                    "Content-Type: text/plain\r\n\r\n"
                    "[request method %s not implemented]",
              request_info->request_method);
  }
}

void TWebInterface::DoStartHttpServer() {
  std::ostringstream oss;
  oss << Port;
  std::string port_str(oss.str());

  const char *opts[] = {
    "enable_directory_listing", "no",
    "listening_ports", port_str.c_str(),
    "num_threads", "1",
    nullptr
  };

  /* We want any threads created by Mongoose to have all signals blocked.
     Bruce's main thread handles signals. */
  const Signal::TSet block_all(Signal::TSet::Full);
  Signal::TMasker masker(*block_all);
  Start(opts);
}
