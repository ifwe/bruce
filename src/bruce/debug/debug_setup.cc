/* <bruce/debug/debug_setup.cc>

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

   Implements <bruce/debug/debug_setup.h>.
 */

#include <bruce/debug/debug_setup.h>

#include <cerrno>
#include <cstdlib>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include <base/error_utils.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Debug;

static int OpenDebugFile(const char *path, bool truncate_file) {
  int flags = O_CREAT | O_APPEND | O_WRONLY;

  if (truncate_file) {
    flags |= O_TRUNC;
  }

  int fd = open(path, flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

  if (fd < 0) {
    /* Fail gracefully. */
    char tmp_buf[256];
    const char *msg = Strerror(errno, tmp_buf, sizeof(tmp_buf));
    syslog(LOG_ERR, "Failed to open debug logfile %s: %s", path, msg);
  }

  return fd;
}

TDebugSetup::TSettings::TSettings(size_t version,
    std::unordered_set<std::string> *debug_topics,
    const char *msg_receive_log_path, const char *msg_send_log_path,
    const char *msg_got_ack_log_path, size_t byte_limit, bool truncate_files)
    : Version(version),
      LoggingEnabled(EnableIsSpecified(debug_topics)),
      BytesRemaining(byte_limit) {
  if (debug_topics) {
    DebugTopics.MakeKnown(std::move(*debug_topics));
  }

  if (LoggingEnabled) {
    GetLogFd(TLogId::MSG_RECEIVE) = OpenDebugFile(msg_receive_log_path,
                                                  truncate_files);
    GetLogFd(TLogId::MSG_SEND) = OpenDebugFile(msg_send_log_path,
                                               truncate_files);
    GetLogFd(TLogId::MSG_GOT_ACK) = OpenDebugFile(msg_got_ack_log_path,
                                                  truncate_files);
  }
}

bool TDebugSetup::AddDebugTopic(const char *topic) {
  assert(this);

  std::lock_guard<std::mutex> lock(Mutex);
  assert(Settings);
  const std::unordered_set<std::string> *tptr = Settings->GetDebugTopics();

  if (tptr == nullptr) {
    /* "All topics" is already specified. */
    return false;
  }

  std::string to_add(topic);

  if (tptr->find(to_add) != tptr->end()) {
    /* 'topic' is already specified. */
    return false;
  }

  if (!Settings->LoggingIsEnabled()) {
    CreateDebugDir();
  }

  std::unordered_set<std::string> new_topics(*tptr);
  new_topics.insert(std::move(to_add));
  ReplaceSettings(&new_topics);
  return true;
}

bool TDebugSetup::DelDebugTopic(const char *topic) {
  assert(this);

  std::lock_guard<std::mutex> lock(Mutex);
  assert(Settings);
  const std::unordered_set<std::string> *tptr = Settings->GetDebugTopics();

  if (tptr == nullptr) {
    /* "All topics" is specified.  Implementing "all topics except { X, Y, Z }"
       semantics wouldn't be hard, but that feature isn't currently needed and
       I don't feel like implementing it.  Therefore ignore the request. */
    return false;
  }

  std::string to_del(topic);

  if (tptr->find(to_del) == tptr->end()) {
    /* 'topic' is already absent. */
    return false;
  }

  std::unordered_set<std::string> new_topics(*tptr);
  new_topics.erase(to_del);
  ReplaceSettings(&new_topics);
  return true;
}

void TDebugSetup::SetDebugTopics(
    std::unordered_set<std::string> *debug_topics) {
  assert(this);

  std::lock_guard<std::mutex> lock(Mutex);
  assert(Settings);

  if (!Settings->LoggingIsEnabled() &&
      TSettings::EnableIsSpecified(debug_topics)) {
    CreateDebugDir();
  }

  ReplaceSettings(debug_topics);
}

void TDebugSetup::TruncateDebugFiles() {
  assert(this);

  if (truncate(GetLogPath(TLogId::MSG_RECEIVE).c_str(), 0) < 0) {
    syslog(LOG_ERR, "Failed to truncate MSG_RECEIVE debug logfile");
  }

  if (truncate(GetLogPath(TLogId::MSG_SEND).c_str(), 0) < 0) {
    syslog(LOG_ERR, "Failed to truncate MSG_SEND debug logfile");
  }

  if (truncate(GetLogPath(TLogId::MSG_GOT_ACK).c_str(), 0) < 0) {
    syslog(LOG_ERR, "Failed to truncate MSG_GOT_ACK debug logfile");
  }
}

void TDebugSetup::CreateDebugDir() {
  assert(this);
  std::string cmd("/bin/mkdir -p ");
  cmd += DebugDir;

  if (std::system(cmd.c_str()) < 0) {
    syslog(LOG_ERR, "Failed to create debug directory [%s]", DebugDir.c_str());
    /* Keep running, with debug logfiles disabled. */
  }
}

static void SettingsFtruncate(const TDebugSetup::TSettings &settings) {
  int fd = settings.GetLogFileDescriptor(TDebugSetup::TLogId::MSG_RECEIVE);

  if (fd >= 0 && ftruncate(fd, 0)) {
    /* Fail gracefully. */
    char tmp_buf[256];
    const char *msg = Strerror(errno, tmp_buf, sizeof(tmp_buf));
    syslog(LOG_ERR, "Failed to truncate msg receive debug logfile: %s", msg);
  }

  fd = settings.GetLogFileDescriptor(TDebugSetup::TLogId::MSG_SEND);

  if (fd >= 0 && ftruncate(fd, 0)) {
    /* Fail gracefully. */
    char tmp_buf[256];
    const char *msg = Strerror(errno, tmp_buf, sizeof(tmp_buf));
    syslog(LOG_ERR, "Failed to truncate msg send debug logfile: %s", msg);
  }

  fd = settings.GetLogFileDescriptor(TDebugSetup::TLogId::MSG_GOT_ACK);

  if (fd >= 0 && ftruncate(fd, 0)) {
    /* Fail gracefully. */
    char tmp_buf[256];
    const char *msg = Strerror(errno, tmp_buf, sizeof(tmp_buf));
    syslog(LOG_ERR, "Failed to truncate msg got ACK debug logfile: %s",
           msg);
  }
}

void TDebugSetup::DeleteOldDebugFiles(const TSettings::TPtr &old_settings) {
  assert(this);

  /* Unlink the old files.  When we create new files to replace them, any
     threads still using the old file descriptors (and debug settings) will
     write to the unlinked files until they see that the debug settings have
     changed.  New debug data gets written to the new files, and is not mixed
     with any data associated with the previous debug settings. */
  unlink(GetLogPath(TLogId::MSG_RECEIVE).c_str());
  unlink(GetLogPath(TLogId::MSG_SEND).c_str());
  unlink(GetLogPath(TLogId::MSG_GOT_ACK).c_str());

  if (old_settings) {
    /* Now ftruncate the files we just unlinked through their still open file
       descriptors.  In case the old (soon to be discarded) file data is large,
       we want to get rid of it right away so bruce isn't occupying a ton of
       disk space with data no longer visible in the filesystem namespace.
     */
    SettingsFtruncate(*old_settings);
  }
}
