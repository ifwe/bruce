/* <bruce/web_request_handler.cc>

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

   Implements <bruce/web_request_handler.h>.
 */

#include <bruce/web_request_handler.h>

#include <cassert>
#include <cstring>
#include <iomanip>
#include <memory>
#include <string>

#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <bruce/build_id.h>
#include <bruce/util/time_util.h>
#include <server/counter.h>
#include <third_party/base64/base64.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Debug;
using namespace Bruce::Util;
using namespace Server;

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

void TWebRequestHandler::HandleGetServerInfoRequestPlain(std::ostream &os) {
  assert(this);
  uint64_t now = GetEpochSeconds();
  char time_buf[TIME_BUF_SIZE];
  FillTimeBuf(now, time_buf);
  os << "pid: " << getpid() << std::endl
      << "now: " << now << " " << time_buf << std::endl
      << "version: " << bruce_build_id << std::endl;
}

void TWebRequestHandler::HandleGetServerInfoRequestJson(std::ostream &os) {
  assert(this);
  uint64_t now = GetEpochSeconds();
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"now\": " << now << "," << std::endl
        << ind1 << "\"version\": \"" << bruce_build_id << "\"" << std::endl;
  }

  os << ind0 << "}" << std::endl;
}

void TWebRequestHandler::HandleGetCountersRequestPlain(std::ostream &os) {
  assert(this);
  TCounter::Sample();
  time_t sample_time = TCounter::GetSampleTime();
  time_t reset_time = TCounter::GetResetTime();
  char sample_time_buf[TIME_BUF_SIZE], reset_time_buf[TIME_BUF_SIZE];
  FillTimeBuf(sample_time, sample_time_buf);
  FillTimeBuf(reset_time, reset_time_buf);
  os << "now=" << sample_time << " " << sample_time_buf << std::endl
      << "since=" << reset_time << " " << reset_time_buf << std::endl
      << "pid=" << getpid() << std::endl
      << "version=" << bruce_build_id << std::endl
      << std::endl;

  for (const TCounter *counter = TCounter::GetFirstCounter();
       counter != nullptr;
       counter = counter->GetNextCounter()) {
    os << counter->GetCodeLocation() << "." << counter->GetName() << "="
        << counter->GetCount() << std::endl;
  }
}

void TWebRequestHandler::HandleGetCountersRequestJson(std::ostream &os) {
  assert(this);
  TCounter::Sample();
  time_t sample_time = TCounter::GetSampleTime();
  time_t reset_time = TCounter::GetResetTime();
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"now\": " << sample_time << "," << std::endl
        << ind1 << "\"since\": " << reset_time << "," << std::endl
        << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"version\": \"" << bruce_build_id << "\"," << std::endl
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

void TWebRequestHandler::HandleGetDiscardsRequestPlain(std::ostream &os,
    const TAnomalyTracker &tracker) {
  assert(this);
  uint64_t now = GetEpochSeconds();
  char time_buf[TIME_BUF_SIZE];
  FillTimeBuf(now, time_buf);
  TAnomalyTracker::TInfo current_unfinished;
  std::shared_ptr<const TAnomalyTracker::TInfo> latest_finished =
      tracker.GetInfo(current_unfinished);
  os << "pid: " << getpid() << std::endl
      << "now: " << now << " " << time_buf << std::endl
      << "version: " << bruce_build_id << std::endl
      << "report interval in seconds: " << tracker.GetReportInterval()
      << std::endl << std::endl
      << "current (unfinished) reporting period:" << std::endl;
  WriteDiscardReportPlain(os, current_unfinished);

  if (latest_finished) {
    os << std::endl << "latest finished reporting period:" << std::endl;
    WriteDiscardReportPlain(os, *latest_finished);
  }
}

void TWebRequestHandler::HandleGetDiscardsRequestJson(std::ostream &os,
    const TAnomalyTracker &tracker) {
  assert(this);
  uint64_t now = GetEpochSeconds();
  TAnomalyTracker::TInfo current_unfinished;
  std::shared_ptr<const TAnomalyTracker::TInfo> latest_finished =
      tracker.GetInfo(current_unfinished);
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"now\": " << now << "," << std::endl
        << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"version\": \"" << bruce_build_id << "\"," << std::endl
        << ind1 << "\"interval\": " << tracker.GetReportInterval() << ","
        << std::endl
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

void TWebRequestHandler::HandleMetadataFetchTimeRequestPlain(std::ostream &os,
    const TMetadataTimestamp &metadata_timestamp) {
  assert(this);
  uint64_t last_update_time = 0, last_modified_time = 0;
  metadata_timestamp.GetTimes(last_update_time, last_modified_time);
  uint64_t now = GetEpochMilliseconds();
  char last_update_time_buf[TIME_BUF_SIZE],
      last_modified_time_buf[TIME_BUF_SIZE], now_time_buf[TIME_BUF_SIZE];
  FillTimeBuf(last_update_time / 1000, last_update_time_buf);
  FillTimeBuf(last_modified_time / 1000, last_modified_time_buf);
  FillTimeBuf(now / 1000, now_time_buf);
  os << "pid: " << getpid() << std::endl
      << "version: " << bruce_build_id << std::endl
      << "now (milliseconds since epoch): " << now << " " << now_time_buf
      << std::endl
      << "metadata last updated at (milliseconds since epoch): "
      << last_update_time << " " << last_update_time_buf << std::endl
      << "metadata last modified at (milliseconds since epoch): "
      << last_modified_time << " " << last_modified_time_buf << std::endl;
}

void TWebRequestHandler::HandleMetadataFetchTimeRequestJson(std::ostream &os,
    const TMetadataTimestamp &metadata_timestamp) {
  assert(this);
  uint64_t last_update_time = 0, last_modified_time = 0;
  metadata_timestamp.GetTimes(last_update_time, last_modified_time);
  uint64_t now = GetEpochMilliseconds();
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"version\": \"" << bruce_build_id << "\"," << std::endl
        << ind1 << "\"now\": " << now << "," << std::endl
        << ind1 << "\"last_updated\": " << last_update_time << "," << std::endl
        << ind1 << "\"last_modified\": " << last_modified_time << ","
        << std::endl;
  }

  os << ind0 << "}" << std::endl;
}

void TWebRequestHandler::HandleQueueStatsRequestPlain(std::ostream &os,
    const TMsgStateTracker &tracker) {
  assert(this);
  std::vector<TMsgStateTracker::TTopicStatsItem> topic_stats;
  long new_count = 0;
  tracker.GetStats(topic_stats, new_count);
  uint64_t now = GetEpochSeconds();
  char time_buf[TIME_BUF_SIZE];
  FillTimeBuf(now, time_buf);
  os << "pid: " << getpid() << std::endl
      << "now: " << now << " " << time_buf << std::endl
      << "version: " << bruce_build_id << std::endl << std::endl;
  long total_batch = 0;
  long total_send_wait = 0;
  long total_ack_wait = 0;

  for (const auto &item : topic_stats) {
    total_batch += item.second.BatchingCount;
    total_send_wait += item.second.SendWaitCount;
    total_ack_wait += item.second.AckWaitCount;
    os << "batch: " << std::setw(10) << item.second.BatchingCount
        << "  send_wait: " << std::setw(10) << item.second.SendWaitCount
        << "  ack_wait: " << std::setw(10) << item.second.AckWaitCount
        << "  topic: [" << item.first << "]" << std::endl;
  }

  if (!topic_stats.empty()) {
    os << std::endl;
  }

  os << std::setw(10) << new_count << " total new" << std::endl
      << std::setw(10) << total_batch << " total batch" << std::endl
      << std::setw(10) << total_send_wait << " total send_wait" << std::endl
      << std::setw(10) << total_ack_wait << " total ack_wait" << std::endl
      << std::setw(10)
      << (new_count + total_batch + total_send_wait + total_ack_wait)
      << " total (all states: new + batch + send_wait + ack_wait)"
      << std::endl;
}

void TWebRequestHandler::HandleQueueStatsRequestJson(std::ostream &os,
    const TMsgStateTracker &tracker) {
  assert(this);
  std::vector<TMsgStateTracker::TTopicStatsItem> topic_stats;
  long new_count = 0;
  tracker.GetStats(topic_stats, new_count);
  uint64_t now = GetEpochSeconds();
  std::string indent_str;
  TIndent ind0(indent_str, TIndent::StartAt::Zero, 4);
  os << ind0 << "{" << std::endl;

  {
    TIndent ind1(ind0);
    os << ind1 << "\"now\": " << now << "," << std::endl
        << ind1 << "\"pid\": " << getpid() << "," << std::endl
        << ind1 << "\"version\": \"" << bruce_build_id << "\"," << std::endl
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
              << ind3 << "\"batch\": " << item.second.BatchingCount << ","
              << std::endl
              << ind3 << "\"send_wait\": " << item.second.SendWaitCount << ","
              << std::endl
              << ind3 << "\"ack_wait\": " << item.second.AckWaitCount
              << std::endl;
        }

        os << ind2 << "}";
        first_time = false;
      }

      os << std::endl;
    }

    os << ind1 << "]," << std::endl
        << ind1 << "\"new\": " << new_count << std::endl;
  }

  os << ind0 << "}" << std::endl;
}

void TWebRequestHandler::HandleGetDebugTopicsRequest(std::ostream &os,
    const Debug::TDebugSetup &debug_setup) {
  assert(this);
  TDebugSetup::TSettings::TPtr settings = debug_setup.GetSettings();
  assert(settings);
  const std::unordered_set<std::string> *topics = settings->GetDebugTopics();

  if (topics == nullptr) {
    os << "All message debug topics are currently enabled" << std::endl;
  } else if (topics->empty()) {
    os << "No message debug topics are currently enabled" << std::endl;
  } else {
    for (const std::string &topic : *topics) {
      os << "topic: [" << topic << "]" << std::endl;
    }
  }
}

void TWebRequestHandler::HandleDebugAddAllTopicsRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup) {
  assert(this);
  debug_setup.SetDebugTopics(nullptr);
  os << "All message debug topics enabled" << std::endl;
}

void TWebRequestHandler::HandleDebugDelAllTopicsRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup) {
  assert(this);
  debug_setup.ClearDebugTopics();
  os << "All message debug topics disabled" << std::endl;
}

void TWebRequestHandler::HandleDebugTruncateFilesRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup) {
  assert(this);
  debug_setup.TruncateDebugFiles();
  os << "Message debug files truncated" << std::endl;
}

void TWebRequestHandler::HandleDebugAddTopicRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup, const char *topic) {
  assert(this);

  if (debug_setup.AddDebugTopic(topic)) {
    os << "Enabled debug topic [" << topic << "]" << std::endl;
  } else {
    os << "Failed to enable debug topic [" << topic << "]" << std::endl;
  }
}

void TWebRequestHandler::HandleDebugDelTopicRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup, const char *topic) {
  assert(this);

  if (debug_setup.DelDebugTopic(topic)) {
    os << "Disabled debug topic [" << topic << "]" << std::endl;
  } else {
    os << "Failed to disable debug topic [" << topic << "]" << std::endl;
  }
}

void TWebRequestHandler::HandleMetadataUpdateRequest(std::ostream &os,
        Base::TEventSemaphore &update_request_sem) {
  assert(this);
  update_request_sem.Push();
  uint64_t now = GetEpochSeconds();
  char time_buf[TIME_BUF_SIZE];
  FillTimeBuf(now, time_buf);
  os << "Metadata update initiated at " << now << " " << time_buf
      << std::endl;
}

void TWebRequestHandler::WriteDiscardReportPlain(std::ostream &os,
    const TAnomalyTracker::TInfo &info) {
  assert(this);
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
      << "    bad topic msg count: " << info.BadTopicMsgCount << std::endl;
  bool first_time = true;

  for (const std::string &msg : info.MalformedMsgs) {
    if (first_time) {
      os << std::endl;
      first_time = false;
    }

    /* Message may contain binary data so we base64 encode it. */
    const unsigned char *msg_bytes =
        reinterpret_cast<const unsigned char *>(msg.data());
    std::string encoded_msg = base64_encode(msg_bytes, msg.size());
    os << "    recent malformed msg: " << encoded_msg.size() << "["
        << encoded_msg << "]" << std::endl;
  }

  first_time = true;

  for (const std::pair<int, size_t> &item : info.UnsupportedVersionMsgs) {
    if (first_time) {
      os << std::endl;
      first_time = false;
    }

    os << "    unsupported msg version: " << item.first << " count: "
        << item.second << std::endl;
  }

  first_time = true;

  for (const std::string &topic : info.BadTopics) {
    if (first_time) {
      os << std::endl;
      first_time = false;
    }

    os << "    recent bad topic: " << topic.size() << "[" << topic << "]"
        << std::endl;
  }

  first_time = true;

  for (const std::string &msg : info.LongMsgs) {
    if (first_time) {
      os << std::endl;
      first_time = false;
    }

    /* Message may contain binary data so we base64 encode it. */
    const unsigned char *msg_bytes =
        reinterpret_cast<const unsigned char *>(msg.data());
    std::string encoded_msg = base64_encode(msg_bytes, msg.size());
    os << "    recent too long msg: " << encoded_msg.size() << "["
        << encoded_msg << "]" << std::endl;
  }

  first_time = true;

  for (auto &x : info.RateLimitDiscardMap) {
    if (first_time) {
      os << std::endl;
      first_time = false;
    }

    os << "    rate limit discard topic: " << x.first.size() << "[" << x.first
        << "] count " << x.second << std::endl;
  }

  first_time = true;

  for (auto &x : info.DiscardTopicMap) {
    if (first_time) {
      os << std::endl;
      first_time = false;
    }

    const TAnomalyTracker::TTopicInfo &topic_info = x.second;
    os << "    discard topic: " << x.first.size() << "[" << x.first
        << "] begin [" << topic_info.Interval.First << "] end ["
        << topic_info.Interval.Last << "] count " << topic_info.Count
        << std::endl;
  }

  first_time = true;

  for (auto &x : info.DuplicateTopicMap) {
    if (first_time) {
      os << std::endl;
      first_time = false;
    }

    const TAnomalyTracker::TTopicInfo &topic_info = x.second;
    os << "    possible duplicate topic: " << x.first.size() << "[" << x.first
        << "] begin [" << topic_info.Interval.First << "] end ["
        << topic_info.Interval.Last << "] count " << topic_info.Count
        << std::endl;
  }
}

void TWebRequestHandler::WriteDiscardReportJson(std::ostream &os,
    const TAnomalyTracker::TInfo &info, Base::TIndent &ind0) {
  assert(this);
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

      /* Message may contain binary data so we base64 encode it. */
      const unsigned char *msg_bytes =
          reinterpret_cast<const unsigned char *>(msg.data());
      os << ind1 << "\"" << base64_encode(msg_bytes, msg.size()) << "\"";
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

      /* Message may contain binary data so we base64 encode it. */
      const unsigned char *msg_bytes =
          reinterpret_cast<const unsigned char *>(msg.data());
      os << ind1 << "\"" << base64_encode(msg_bytes, msg.size()) << "\"";
      first_time = false;
    }

    if (!first_time) {
      os << std::endl;
    }
  }

  os << ind0 << "]," << std::endl
      << ind0 << "\"rate_limit_discard\": [" << std::endl;

  {
    TIndent ind1(ind0);
    bool first_time = true;

    for (auto &x : info.RateLimitDiscardMap) {
      if (!first_time) {
        os << "," << std::endl;
      }

      os << ind1 << "{" << std::endl;

      {
        TIndent ind2(ind1);
        os << ind2 << "\"topic\": \"" << x.first << "\"," << std::endl
            << ind2 << "\"count\": " << x.second << std::endl;
      }

      os << ind1 << "}";
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

    for (auto &x : info.DuplicateTopicMap) {
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
