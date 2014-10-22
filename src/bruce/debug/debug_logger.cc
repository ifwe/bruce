/* <bruce/debug/debug_logger.cc>

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

   Implements <bruce/debug/debug_logger.h>.
 */

#include <bruce/debug/debug_logger.h>

#include <cerrno>
#include <cstring>

#include <boost/lexical_cast.hpp>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include <base/error_utils.h>
#include <base/no_default_case.h>
#include <bruce/util/msg_util.h>
#include <third_party/base64/base64.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Debug;
using namespace Bruce::Util;

static const char *ToBlurb(TDebugSetup::TLogId log_id) {
  const char *result = "";

  switch (log_id) {
    case TDebugSetup::TLogId::MSG_RECEIVE: {
      result = "msg receive";
      break;
    }
    case TDebugSetup::TLogId::MSG_SEND: {
      result = "msg send";
      break;
    }
    case TDebugSetup::TLogId::MSG_GOT_ACK: {
      result = "msg got ACK";
      break;
    }
    NO_DEFAULT_CASE;
  }

  return result;
}

void TDebugLogger::LogMsg(const TMsg &msg) {
  assert(this);

  if (DebugSetup.MySettingsAreOld(CachedSettingsVersion)) {
    Settings = DebugSetup.GetSettings();
    assert(Settings);
    LogFd = Settings->GetLogFileDescriptor(LogId);
    CachedSettingsVersion = Settings->GetVersion();
    CachedDebugTopics = Settings->GetDebugTopics();
    bool new_enabled_setting = Settings->LoggingIsEnabled();

    if (new_enabled_setting != LoggingEnabled) {
      if (new_enabled_setting) {
        EnableLogging();
      } else {
        DisableLogging();
      }
    }
  }

  if (!LoggingEnabled) {
    return;
  }

  if (((++MsgCount % 1024) == 0) &&
      (SecondsSinceEnabled() >= DebugSetup.GetKillSwitchLimitSeconds())) {
    /* Flip automatic kill switch if debug logging has been enabled for a long
       time.  We don't want to fill up the disk if someone forgets to turn it
       off after a debugging session. */
    DisableLogging();
    return;
  }

  RawData.clear();
  WriteKey(RawData, 0, msg);
  Encoded.clear();

  if (!RawData.empty()) {
    /* Base64 encode key in case it contains binary data. */
    Encoded = base64_encode(&RawData[0], RawData.size());
  }

  LogEntry = "ts: ";
  LogEntry += boost::lexical_cast<std::string>(msg.GetTimestamp());
  LogEntry += " topic: ";
  LogEntry += boost::lexical_cast<std::string>(msg.GetTopic().size());
  LogEntry += "[";
  LogEntry += msg.GetTopic();
  LogEntry += "] key: ";
  LogEntry += boost::lexical_cast<std::string>(Encoded.size());
  LogEntry += "[";
  LogEntry += Encoded;
  LogEntry += "] value: ";
  RawData.clear();
  WriteValue(RawData, 0, msg, AddTimestamp, UseOldOutputFormat);
  Encoded.clear();

  if (!RawData.empty()) {
    /* Base64 encode value in case it contains binary data. */
    Encoded = base64_encode(&RawData[0], RawData.size());
  }

  LogEntry += boost::lexical_cast<std::string>(Encoded.size());
  LogEntry += "[";
  LogEntry += Encoded;
  LogEntry += "]\n";

  if (!Settings->RequestLogBytes(LogEntry.size())) {
    /* Flip automatic kill switch if we can't log this message without
       exceeding the byte limit.  This is a safeguard to prevent filling up the
       disk. */
    DisableLogging();
    return;
  }

  ssize_t ret = write(LogFd, LogEntry.data(), LogEntry.size());

  if (ret < 0) {
    /* Fail gracefully. */
    char tmp_buf[256];
    const char *err_msg = Strerror(errno, tmp_buf, sizeof(tmp_buf));
    syslog(LOG_ERR, "Failed to write to debug logfile %s: %s", ToBlurb(LogId),
           err_msg);
    DisableLogging();
  }
}

void TDebugLogger::LogMsgList(const std::list<TMsg::TPtr> &msg_list) {
  assert(this);

  for (const TMsg::TPtr &msg_ptr : msg_list) {
    LogMsg(*msg_ptr);
  }
}

unsigned long TDebugLogger::Now() {
  struct timespec t;
  IfLt0(clock_gettime(CLOCK_MONOTONIC_RAW, &t));
  return t.tv_sec;
}

void TDebugLogger::DisableLogging() {
  assert(this);
  LogFd = -1;
  LoggingEnabled = false;
}

void TDebugLogger::EnableLogging() {
  assert(this);
  LoggingEnabledAt = Now();
  MsgCount = 0;
  LogFd = Settings->GetLogFileDescriptor(LogId);
  LoggingEnabled = (LogFd >= 0);
}
