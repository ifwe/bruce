/* <bruce/debug/debug_logger.h>

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

   Class for logging messages for debugging.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <string>
#include <vector>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/debug/debug_setup.h>
#include <bruce/msg.h>

namespace Bruce {

  namespace Debug {

    class TDebugLogger final {
      NO_COPY_SEMANTICS(TDebugLogger);

      public:
      TDebugLogger(const TDebugSetup &debug_setup, TDebugSetup::TLogId log_id,
                   bool add_timestamp, bool use_old_output_format)
          : DebugSetup(debug_setup),
            LogId(log_id),
            AddTimestamp(add_timestamp),
            UseOldOutputFormat(use_old_output_format),
            Settings(debug_setup.GetSettings()),
            LogFd(Settings->GetLogFileDescriptor(log_id)),
            CachedSettingsVersion(Settings->GetVersion()),
            CachedDebugTopics(Settings->GetDebugTopics()),
            LoggingEnabled(Settings->LoggingIsEnabled()),
            LoggingEnabledAt(Now()),
            MsgCount(0) {
      }

      void LogMsg(const TMsg &msg);

      void LogMsg(const TMsg::TPtr &msg_ptr) {
        assert(this);
        LogMsg(*msg_ptr);
      }

      void LogMsgList(const std::list<TMsg::TPtr> &msg_list);

      private:
      using TSettings = TDebugSetup::TSettings;

      /* Return a monotonically increasing timestamp indicating the number of
         seconds since some fixed point in the past (not necessarily the
         epoch). */
      static unsigned long Now();

      unsigned long SecondsSinceEnabled() const {
        assert(this);
        unsigned long t = Now();
        return (t >= LoggingEnabledAt) ? (t - LoggingEnabledAt): 0;
      }

      void DisableLogging();

      void EnableLogging();

      const TDebugSetup &DebugSetup;

      const TDebugSetup::TLogId LogId;

      const bool AddTimestamp;

      const bool UseOldOutputFormat;

      TSettings::TPtr Settings;

      int LogFd;

      size_t CachedSettingsVersion;

      const std::unordered_set<std::string> *CachedDebugTopics;

      bool LoggingEnabled;

      unsigned long LoggingEnabledAt;

      size_t MsgCount;

      std::vector<uint8_t> RawData;

      std::string Encoded;

      std::string LogEntry;
    };  // TDebugLogger

  }  // Debug

}  // Bruce
