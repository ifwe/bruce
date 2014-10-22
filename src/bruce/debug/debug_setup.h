/* <bruce/debug/debug_setup.h>

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

   Debugging stuff.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/opt.h>

namespace Bruce {

  namespace Debug {

    class TDebugSetup final {
      NO_COPY_SEMANTICS(TDebugSetup);

      public:
      enum class TLogId {
        MSG_RECEIVE = 0,
        MSG_SEND = 1,
        MSG_GOT_ACK = 2
      };  // TLogId

      static const size_t NUM_LOG_FILES = 3;

      static size_t ToIndex(TLogId id) {
        size_t index = static_cast<size_t>(id);
        assert(index < NUM_LOG_FILES);
        return index;
      }

      class TSettings final {
        NO_COPY_SEMANTICS(TSettings);

        public:

        using TPtr = std::shared_ptr<TSettings>;

        static bool EnableIsSpecified(
            const std::unordered_set<std::string> *topics) {
          return (topics == nullptr) || !topics->empty();
        }

        /* Return nullptr if all topics are enabled.  Otherwise return a
           pointer to the set of enabled topics. */
        const std::unordered_set<std::string> *GetDebugTopics() const {
          assert(this);
          return DebugTopics.IsKnown() ? &*DebugTopics : nullptr;
        }

        size_t GetVersion() const {
          assert(this);
          return Version;
        }

        bool LoggingIsEnabled() const {
          assert(this);
          return LoggingEnabled;
        }

        int GetLogFileDescriptor(TLogId log_id) const {
          assert(this);
          return LogFds[ToIndex(log_id)];
        }

        bool RequestLogBytes(size_t num_bytes) {
          assert(this);

          std::lock_guard<std::mutex> lock(Mutex);

          if (BytesRemaining < num_bytes) {
            return false;
          }

          BytesRemaining -= num_bytes;
          return true;
        }

        private:
        /* If 'debug_topics' is null, the created TSettings object will specify
           "all topics".  Otherwise, the contents of *debug_topics will be
           moved into the created TSettings object (leaving *debug_topics
           empty).  "No topics" is specified by providing an empty set. */
        static TPtr Create(size_t version,
                           std::unordered_set<std::string> *debug_topics,
                           const char *msg_receive_log_path,
                           const char *msg_send_log_path,
                           const char *msg_got_ack_log_path, size_t byte_limit,
                           bool truncate_files) {
          return TPtr(new TSettings(version, debug_topics,
                                    msg_receive_log_path, msg_send_log_path,
                                    msg_got_ack_log_path, byte_limit,
                                    truncate_files));
        }

        Base::TFd &GetLogFd(TLogId log_id) {
          assert(this);
          return LogFds[ToIndex(log_id)];
        }

        TSettings(size_t version,
            std::unordered_set<std::string> *debug_topics,
            const char *msg_receive_log_path, const char *msg_send_log_path,
            const char *msg_got_ack_log_path, size_t byte_limit,
            bool truncate_files);

        const size_t Version;

        const bool LoggingEnabled;

        Base::TFd LogFds[NUM_LOG_FILES];

        /* The absence of a set of strings means "all topics".  The presence of
           an empty set of strings means "no topics". */
        Base::TOpt<const std::unordered_set<std::string>> DebugTopics;

        /* Protects 'BytesRemaining'. */
        std::mutex Mutex;

        /* As a safeguard, we limit the number of bytes that can be logged. */
        size_t BytesRemaining;

        /* For access to static Create() method. */
        friend class TDebugSetup;
      };  // TSettings

      /* For convenience. */
      static const size_t MAX_LIMIT = std::numeric_limits<size_t>::max();

      TDebugSetup(const char *debug_dir, size_t kill_switch_limit_seconds,
                  size_t kill_switch_limit_bytes)
          : DebugDir(debug_dir),
            KillSwitchLimitSeconds(kill_switch_limit_seconds),
            KillSwitchLimitBytes(kill_switch_limit_bytes),
            SettingsVersion(0) {
        GetLogPath(TLogId::MSG_RECEIVE) =
            std::move(std::string(debug_dir) + "/msg_receive");
        GetLogPath(TLogId::MSG_SEND) =
            std::move(std::string(debug_dir) + "/msg_send");
        GetLogPath(TLogId::MSG_GOT_ACK) =
            std::move(std::string(debug_dir) + "/msg_got_ack");
        Settings = CreateInitialSettings();
        SettingsVersion = Settings->GetVersion();
      }

      const std::string &GetLogPath(TLogId log_id) const {
        assert(this);
        return LogPaths[ToIndex(log_id)];
      }

      size_t GetKillSwitchLimitSeconds() const {
        return KillSwitchLimitSeconds;
      }

      bool MySettingsAreOld(size_t my_version) const {
        assert(this);

        /* We can get away without grabbing 'Mutex' here. */
        return SettingsVersion != my_version;
      }

      TSettings::TPtr GetSettings() const {
        assert(this);
        std::lock_guard<std::mutex> lock(Mutex);
        assert(Settings);
        return Settings;
      }

      bool AddDebugTopic(const char *topic);

      bool DelDebugTopic(const char *topic);

      /* If 'debug_topics' is null, "all topics" is specified.  Otherwise, the
         contents of *debug_topics are moved and left empty.  "No topics" is
         specified by providing an empty set. */
      void SetDebugTopics(std::unordered_set<std::string> *debug_topics);

      void ClearDebugTopics() {
        assert(this);
        std::unordered_set<std::string> no_topics;
        SetDebugTopics(&no_topics);
      }

      void TruncateDebugFiles();

      private:
      std::string &GetLogPath(TLogId log_id) {
        assert(this);
        return LogPaths[ToIndex(log_id)];
      }

      void CreateDebugDir();

      void DeleteOldDebugFiles(const TSettings::TPtr &old_settings);

      void DeleteOldDebugFiles() {
        TSettings::TPtr no_settings;
        DeleteOldDebugFiles(no_settings);
      }

      TSettings::TPtr CreateInitialSettings() {
        DeleteOldDebugFiles();
        std::unordered_set<std::string> no_topics;
        return TSettings::Create(0, &no_topics,
                                 GetLogPath(TLogId::MSG_RECEIVE).c_str(),
                                 GetLogPath(TLogId::MSG_SEND).c_str(),
                                 GetLogPath(TLogId::MSG_GOT_ACK).c_str(),
                                 KillSwitchLimitBytes, true);
      }

      /* If 'debug_topics' is null, the created TSettings object will specify
         "all topics".  Otherwise, the contents of *debug_topics will be moved
         into the created TSettings object (leaving *debug_topics empty).  "No
         topics" is specified by providing an empty set.  Caller must hold
         'Mutex'. */
      void ReplaceSettings(std::unordered_set<std::string> *debug_topics) {
        assert(Settings);
        bool delete_old_data = TSettings::EnableIsSpecified(debug_topics) &&
                               !Settings->LoggingIsEnabled();

        if (delete_old_data) {
          DeleteOldDebugFiles(Settings);
        }

        Settings = TSettings::Create(Settings->GetVersion() + 1, debug_topics,
                                     GetLogPath(TLogId::MSG_RECEIVE).c_str(),
                                     GetLogPath(TLogId::MSG_SEND).c_str(),
                                     GetLogPath(TLogId::MSG_GOT_ACK).c_str(),
                                     KillSwitchLimitBytes, delete_old_data);
        SettingsVersion = Settings->GetVersion();
      }

      const std::string DebugDir;

      const size_t KillSwitchLimitSeconds;

      const size_t KillSwitchLimitBytes;

      std::string LogPaths[NUM_LOG_FILES];

      mutable std::mutex Mutex;

      TSettings::TPtr Settings;

      size_t SettingsVersion;
    };  // TDebugSetup

  }  // Debug

}  // Bruce
