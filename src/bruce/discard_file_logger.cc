/* <bruce/discard_file_logger.cc>

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

   Implements <bruce/discard_file_logger.h>.
 */

#include <bruce/discard_file_logger.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>

#include <boost/lexical_cast.hpp>
#include <fcntl.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include <base/dir_iter.h>
#include <base/error_utils.h>
#include <base/gettid.h>
#include <base/no_default_case.h>
#include <bruce/util/msg_util.h>
#include <bruce/util/time_util.h>
#include <third_party/base64/base64.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Util;

static std::string ComposeLogEntry(TMsg::TTimestamp timestamp,
    const char *event, const char *info, const std::string &topic,
    const void *key, size_t key_size, const void *msg, size_t msg_size) {
  assert(event);
  assert(info);
  assert(key || (key_size == 0));
  assert(msg || (msg_size == 0));
  uint64_t now = GetEpochMilliseconds();
  std::string encoded_key;

  if (key_size) {
    encoded_key = base64_encode(reinterpret_cast<const unsigned char *>(key),
                                key_size);
  }

  std::string encoded_msg;

  if (msg_size) {
    encoded_msg = base64_encode(reinterpret_cast<const unsigned char *>(msg),
                                msg_size);
  }

  std::ostringstream os;
  os << "now: " << now << " ts: " << timestamp << " event: " << event
      << " info: " << info << " topic: " << topic.size() << "[" << topic
      << "] key: " << encoded_key.size() << "[" << encoded_key << "] msg: "
      << encoded_msg.size() << "[" << encoded_msg << "]\n";
  return os.str();
}

static void CreateDir(const char *dir) {
  assert(dir);
  std::string cmd("/bin/mkdir -p ");
  cmd += dir;

  if (std::system(cmd.c_str()) < 0) {
    syslog(LOG_ERR, "Failed to create discard log directory [%s]", dir);
    IfLt0(-1);  // this will throw
    /* TODO: Modify implementation to keep running, with discard file logging
       disabled. */
  }
}

TDiscardFileLogger::TDiscardFileLogger()
    : MaxMsgPrefixLen(std::numeric_limits<size_t>::max()),
      UseOldOutputFormat(false),
      Enabled(false),
      MaxFileSize(0),
      MaxArchiveSize(0) {
}

TDiscardFileLogger::~TDiscardFileLogger() noexcept {
  assert(this);

  try {
    Shutdown();
  } catch (const std::exception &x) {
    syslog(LOG_ERR, "Caught unexpected exception in TDiscardFileLogger "
           "destructor: %s", x.what());
    assert(false);
  } catch (...) {
    syslog(LOG_ERR, "Caught unexpected unknown exception in "
           "TDiscardFileLogger destructor");
    assert(false);
  }
}

void TDiscardFileLogger::Init(const char *log_path, uint64_t max_file_size,
    uint64_t max_archive_size, size_t max_msg_prefix_len,
    bool use_old_output_format) {
  assert(this);

  /* Will contain absolute path of directory containing logfile. */
  std::string log_dir;

  /* Will contain just the log filename (without the absolute path). */
  std::string log_filename;

  ParseLogPath(log_path, log_dir, log_filename);

  /* Absolute path of logfile. */
  std::string path(log_path);

  if (Enabled) {
    throw std::logic_error("Discard file logging already initialized");
  }

  CreateDir(log_dir.c_str());
  MaxMsgPrefixLen = max_msg_prefix_len;
  UseOldOutputFormat = use_old_output_format;

  /* Since we are executing during server initialization before any threads can
     create log entries, we modify our internal state without grabbing 'Mutex'.
   */

  LogFd = OpenLogPath(log_path);

  if (!LogFd.IsOpen()) {
    return;
  }

  ArchiveCleaner.reset(new TArchiveCleaner(max_archive_size, log_dir.c_str(),
      log_filename.c_str()));
  ArchiveCleaner->Start();
  LogPath = std::move(path);
  LogDir = std::move(log_dir);
  LogFilename = std::move(log_filename);
  MaxFileSize = max_file_size;
  MaxArchiveSize = max_archive_size;
  Enabled = true;
  CheckMaxFileSize(0);
  ArchiveCleaner->SendCleanRequest();
}

void TDiscardFileLogger::Shutdown() {
  assert(this);

  std::lock_guard<std::mutex> lock(Mutex);
  DisableLogging();

  if (ArchiveCleaner) {
    ArchiveCleaner->RequestShutdown();
    ArchiveCleaner->Join();
    ArchiveCleaner.reset();
  }
}

static const char *ReasonToBlurb(TDiscardFileLogger::TDiscardReason reason) {
  switch (reason) {
    case TDiscardFileLogger::TDiscardReason::Bug:
      break;
    case TDiscardFileLogger::TDiscardReason::FailedDeliveryAttemptLimit:
      return "DELIVERY_ATTEMPT_LIMIT";
    case TDiscardFileLogger::TDiscardReason::KafkaErrorAck:
      return "KAFKA_ERROR_ACK";
    case TDiscardFileLogger::TDiscardReason::ServerShutdown:
      return "SERVER_SHUTDOWN";
    case TDiscardFileLogger::TDiscardReason::NoAvailablePartitions:
      return "NO_AVAILABLE_PARTITIONS";
    case TDiscardFileLogger::TDiscardReason::RateLimit:
      return "RATE_LIMIT";
    case TDiscardFileLogger::TDiscardReason::FailedTopicAutocreate:
      return "TOPIC_AUTOCREATE_FAIL";
    NO_DEFAULT_CASE;
  }

  return "BUG";
}

void TDiscardFileLogger::LogDiscard(const TMsg &msg, TDiscardReason reason) {
  assert(this);

  if (!Enabled) {
    return;  // fast path for case where logging is disabled
  }

  std::vector<uint8_t> key_buf;
  WriteKey(key_buf, 0, msg);
  EnforceMaxPrefixLen(key_buf);
  std::vector<uint8_t> value_buf;
  WriteValue(value_buf, 0, msg, false, UseOldOutputFormat);
  EnforceMaxPrefixLen(value_buf);
  const uint8_t *key_buf_begin = key_buf.empty() ? nullptr : &key_buf[0];
  const uint8_t *value_buf_begin = value_buf.empty() ? nullptr : &value_buf[0];
  WriteToLog(ComposeLogEntry(msg.GetTimestamp(), "DISC", ReasonToBlurb(reason),
                 msg.GetTopic(), key_buf_begin, key_buf.size(),
                 value_buf_begin, value_buf.size()));
}

void TDiscardFileLogger::LogDuplicate(const TMsg &msg) {
  assert(this);

  if (!Enabled) {
    return;  // fast path for case where logging is disabled
  }

  std::vector<uint8_t> key_buf;
  WriteKey(key_buf, 0, msg);
  EnforceMaxPrefixLen(key_buf);
  std::vector<uint8_t> value_buf;
  WriteValue(value_buf, 0, msg, false, UseOldOutputFormat);
  EnforceMaxPrefixLen(value_buf);
  const uint8_t *key_buf_begin = key_buf.empty() ? nullptr : &key_buf[0];
  const uint8_t *value_buf_begin = value_buf.empty() ? nullptr : &value_buf[0];
  WriteToLog(ComposeLogEntry(msg.GetTimestamp(), "DUP", "NONE", msg.GetTopic(),
                 key_buf_begin, key_buf.size(), value_buf_begin,
                 value_buf.size()));
}

void TDiscardFileLogger::LogNoMemDiscard(TMsg::TTimestamp timestamp,
    const char *topic_begin, const char *topic_end, const void *key_begin,
    const void *key_end, const void *value_begin, const void *value_end) {
  assert(this);
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(key_begin || (key_end == key_begin));
  assert(key_end >= key_begin);
  assert(value_begin || (value_end == value_begin));
  assert(value_end >= value_begin);

  if (!Enabled) {
    return;  // fast path for case where logging is disabled
  }

  const std::string topic(topic_begin, topic_end);
  size_t key_size = reinterpret_cast<const uint8_t *>(key_end) -
      reinterpret_cast<const uint8_t *>(key_begin);
  size_t value_size = reinterpret_cast<const uint8_t *>(value_end) -
      reinterpret_cast<const uint8_t *>(value_begin);
  WriteToLog(ComposeLogEntry(timestamp, "DISC", "NO_MEM", topic, key_begin,
                 std::min(key_size, MaxMsgPrefixLen), value_begin,
                 std::min(value_size, MaxMsgPrefixLen)));
}

void TDiscardFileLogger::LogMalformedMsgDiscard(const void *msg_begin,
    const void *msg_end) {
  assert(this);

  if (!Enabled) {
    return;  // fast path for case where logging is disabled
  }

  size_t msg_size = reinterpret_cast<const uint8_t *>(msg_end) -
      reinterpret_cast<const uint8_t *>(msg_begin);
  WriteToLog(ComposeLogEntry(GetEpochMilliseconds(), "DISC", "MALFORMED",
                 std::string(), nullptr, 0, msg_begin, std::min(msg_size,
                 MaxMsgPrefixLen)));
}

void TDiscardFileLogger::LogUnsupportedApiKeyDiscard(const void *msg_begin,
    const void *msg_end, int api_key) {
  assert(this);

  if (!Enabled) {
    return;  // fast path for case where logging is disabled
  }

  std::ostringstream os;
  os << "API_KEY(" << api_key << ")";
  std::string info(os.str());
  size_t msg_size = reinterpret_cast<const uint8_t *>(msg_end) -
      reinterpret_cast<const uint8_t *>(msg_begin);
  WriteToLog(ComposeLogEntry(GetEpochMilliseconds(), "DISC", info.c_str(),
                 std::string(), nullptr, 0, msg_begin,
                 std::min(msg_size, MaxMsgPrefixLen)));
}

void TDiscardFileLogger::LogUnsupportedMsgVersionDiscard(const void *msg_begin,
    const void *msg_end, int version) {
  assert(this);

  if (!Enabled) {
    return;  // fast path for case where logging is disabled
  }

  std::ostringstream os;
  os << "VERSION(" << version << ")";
  std::string info(os.str());
  size_t msg_size = reinterpret_cast<const uint8_t *>(msg_end) -
      reinterpret_cast<const uint8_t *>(msg_begin);
  WriteToLog(ComposeLogEntry(GetEpochMilliseconds(), "DISC", info.c_str(),
                 std::string(), nullptr, 0, msg_begin,
                 std::min(msg_size, MaxMsgPrefixLen)));
}

void TDiscardFileLogger::LogBadTopicDiscard(TMsg::TTimestamp timestamp,
    const char *topic_begin, const char *topic_end, const void *key_begin,
    const void *key_end, const void *value_begin, const void *value_end) {
  assert(this);
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(key_begin || (key_end == key_begin));
  assert(key_end >= key_begin);
  assert(value_begin || (value_end == value_begin));
  assert(value_end >= value_begin);

  if (!Enabled) {
    return;  // fast path for case where logging is disabled
  }

  const std::string topic(topic_begin, topic_end);
  size_t key_size = reinterpret_cast<const uint8_t *>(key_end) -
      reinterpret_cast<const uint8_t *>(key_begin);
  size_t value_size = reinterpret_cast<const uint8_t *>(value_end) -
      reinterpret_cast<const uint8_t *>(value_begin);
  WriteToLog(ComposeLogEntry(timestamp, "DISC", "BAD_TOPIC", topic, key_begin,
                 std::min(key_size, MaxMsgPrefixLen), value_begin,
                 std::min(value_size, MaxMsgPrefixLen)));
}

void TDiscardFileLogger::LogBadTopicDiscard(const TMsg &msg) {
  assert(this);

  if (!Enabled) {
    return;  // fast path for case where logging is disabled
  }

  std::vector<uint8_t> key_buf;
  WriteKey(key_buf, 0, msg);
  EnforceMaxPrefixLen(key_buf);
  std::vector<uint8_t> value_buf;
  WriteValue(value_buf, 0, msg, false, UseOldOutputFormat);
  EnforceMaxPrefixLen(value_buf);
  const uint8_t *key_buf_begin = key_buf.empty() ? nullptr : &key_buf[0];
  const uint8_t *value_buf_begin = value_buf.empty() ? nullptr : &value_buf[0];
  WriteToLog(ComposeLogEntry(msg.GetTimestamp(), "DISC", "BAD_TOPIC",
                 msg.GetTopic(), key_buf_begin, key_buf.size(),
                 value_buf_begin, value_buf.size()));
}

void TDiscardFileLogger::LogLongMsgDiscard(const TMsg &msg) {
  assert(this);

  if (!Enabled) {
    return;  // fast path for case where logging is disabled
  }

  std::vector<uint8_t> key_buf;
  WriteKey(key_buf, 0, msg);
  EnforceMaxPrefixLen(key_buf);
  std::vector<uint8_t> value_buf;
  WriteValue(value_buf, 0, msg, false, UseOldOutputFormat);
  EnforceMaxPrefixLen(value_buf);
  const uint8_t *key_buf_begin = key_buf.empty() ? nullptr : &key_buf[0];
  const uint8_t *value_buf_begin = value_buf.empty() ? nullptr : &value_buf[0];
  WriteToLog(ComposeLogEntry(msg.GetTimestamp(), "DISC", "LONG_MSG",
                 msg.GetTopic(), key_buf_begin, key_buf.size(),
                 value_buf_begin, value_buf.size()));
}

TDiscardFileLogger::TArchiveCleaner::TArchiveCleaner(uint64_t max_archive_size,
    const char *log_dir, const char *log_filename)
    : MaxArchiveSize(max_archive_size),
      LogDir(log_dir),
      LogFilename(log_filename) {
}

TDiscardFileLogger::TArchiveCleaner::~TArchiveCleaner() noexcept {
  /* This will shut down the thread if something unexpected happens. */
  ShutdownOnDestroy();
}

void TDiscardFileLogger::TArchiveCleaner::Run() {
  assert(this);
  int tid = static_cast<int>(Gettid());
  syslog(LOG_INFO, "Discard log cleaner thread %d started", tid);
  bool caught_fatal_exception = false;

  try {
    DoRun();
  } catch (const std::exception &x) {
    caught_fatal_exception = true;
    syslog(LOG_ERR, "Fatal error in discard log cleaner thread %d: %s", tid,
           x.what());
  } catch (...) {
    caught_fatal_exception = true;
    syslog(LOG_ERR, "Fatal unknown error in discard log cleaner thread %d",
           tid);
  }

  syslog(LOG_INFO, "Discard log cleaner thread %d finished %s", tid,
         caught_fatal_exception ? "on error" : "normally");
}

struct TOldLogFileInfo {
  std::string AbsolutePath;

  /* File size in bytes. */
  uint64_t Size;

  /* Epoch milliseconds value obtained from filename. */
  uint64_t EpochMilliseconds;

  TOldLogFileInfo() = default;

  TOldLogFileInfo(std::string &&absolute_path, uint64_t size,
      uint64_t epoch_ms)
      : AbsolutePath(std::move(absolute_path)),
        Size(size),
        EpochMilliseconds(epoch_ms) {
  }

  TOldLogFileInfo(const TOldLogFileInfo &) = default;

  TOldLogFileInfo(TOldLogFileInfo &&that)
      : AbsolutePath(std::move(that.AbsolutePath)),
        Size(that.Size),
        EpochMilliseconds(that.EpochMilliseconds) {
  }

  TOldLogFileInfo& operator=(const TOldLogFileInfo &) = default;

  TOldLogFileInfo& operator=(TOldLogFileInfo &&that) {
    assert(this);

    if (this != &that) {
      AbsolutePath = std::move(that.AbsolutePath);
      Size = that.Size;
      EpochMilliseconds = that.EpochMilliseconds;
    }

    return *this;
  }
};  // TOldLogFileInfo

static std::vector<TOldLogFileInfo>
GetOldLogFileSizes(const char *log_dir, const char *log_filename) {
  assert(log_dir);
  std::vector<TOldLogFileInfo> result;
  size_t name_len = std::strlen(log_filename);
  std::string file_path;

  for (TDirIter iter(log_dir); iter; ++iter) {
    if (iter.GetKind() != TDirIter::File) {
      continue;
    }

    const char *name = iter.GetName();
    size_t i = 0;

    for (i = 0; (i < name_len) && (name[i] == log_filename[i]); ++i);

    if ((i < name_len) || (name[i] != '.')) {
      /* If 'log_filename' is "foo", then we are looking for files named
         "foo.N", where N is a string of digits interpreted as an integer
         number of milliseconds since the epoch.  In this case, we found a
         nonmatching file because either ('log_filename' is not a prefix of
         'name') or ('log_filename' is a prefix of name but the next character
         of 'name' is not '.'). */
      continue;
    }

    if (name[++i] == '\0') {
      /* We were expecting a string of digits after the '.', but there are no
         more characters. */
      continue;
    }

    size_t digits_start = i;
    bool all_digits = true;

    for (; name[i]; ++i) {
      if (!std::isdigit(name[i])) {
        all_digits = false;
        break;
      }
    }

    if (!all_digits) {
      /* This filename contains a nondigit character after the '.'. */
      continue;
    }

    /* 'name[i]' shouldn't contain any leading zeros, but just in case someone
       messed with the filenames, we will skip leading zeros.  A leading zero
       would be bad because it would cause the digit string to be interpreted
       as an octal value when converted to an integer. */
    for (i = digits_start; name[i] == '0'; ++i);

    /* Initializing to 0 handles the case where the digit string is either a
       single zero or a longer string consisting only of zeroes. */
    uint64_t epoch_ms = 0;

    if (name[i]) {
      /* 'name' contains at least one nonzero digit.  Convert the digit string
         (minus any leading zeroes) to an integer value. */
      try {
        epoch_ms = boost::lexical_cast<uint64_t>(&name[i]);
      } catch (const boost::bad_lexical_cast &) {
        syslog(LOG_WARNING, "Failed to extract timestamp from apparently "
               "valid old discard logfile name %s", name);
        continue;
      }
    }

    file_path = log_dir;
    file_path += "/";
    file_path += name;
    struct stat stat_buf;
    int ret = stat(file_path.c_str(), &stat_buf);

    if (ret < 0) {
      if (errno != ENOENT) {
        char buf[256];
        Strerror(errno, buf, sizeof(buf));
        syslog(LOG_WARNING, "Failed to stat() old discard logfile %s: %s",
               file_path.c_str(), buf);
      }

      continue;
    }

    if (!S_ISREG(stat_buf.st_mode)) {
      /* It's not a regular file, so skip it. */
      continue;
    }

    /* We found a file whose name conforms to our old logfile naming scheme, so
       append it to the result vector. */
    result.push_back(TOldLogFileInfo(std::move(file_path), stat_buf.st_size,
                                     epoch_ms));
  }

  return std::move(result);
}

bool TDiscardFileLogger::TArchiveCleaner::HandleCleanRequest() {
  assert(this);
  std::vector<TOldLogFileInfo> old_log_files;

  try {
    old_log_files = GetOldLogFileSizes(LogDir.c_str(), LogFilename.c_str());
  } catch (const std::system_error &x) {
    syslog(LOG_WARNING, "Discard log cleaner thread got fatal error while "
           "examining log directory: %s", x.what());
    return false;
  }

  uint64_t total_size = 0;

  for (const TOldLogFileInfo &info : old_log_files) {
    total_size += info.Size;
  }

  if (total_size <= MaxArchiveSize) {
    return true;
  }

  /* Sort old logfiles from oldest to newest, according to the timestamp values
     extracted from their names. */
  std::sort(old_log_files.begin(), old_log_files.end(),
      [](const TOldLogFileInfo &x, const TOldLogFileInfo &y) {
        return x.EpochMilliseconds < y.EpochMilliseconds;
      });

  /* Delete old logfiles, from oldest to newest, until total size limit is no
     longer violated. */
  for (size_t i = 0; i < old_log_files.size(); ++i) {
    const TOldLogFileInfo &info = old_log_files[i];

    if (unlink(info.AbsolutePath.c_str()) && (errno != ENOENT)) {
      char buf[256];
      Strerror(errno, buf, sizeof(buf));
      syslog(LOG_WARNING, "Failed to unlink old discard logfile %s: %s",
             info.AbsolutePath.c_str(), buf);
    } else {
      total_size -= info.Size;

      if (total_size <= MaxArchiveSize) {
        return true;
      }
    }
  }

  assert(total_size > MaxArchiveSize);
  syslog(LOG_WARNING, "Discard log cleaner failed to delete old logfiles in "
         "directory %s", LogDir.c_str());
  return false;
}

void TDiscardFileLogger::TArchiveCleaner::DoRun() {
  assert(this);
  std::array<struct pollfd, 2> events;
  struct pollfd &shutdown_request_event = events[0];
  struct pollfd &clean_request_event = events[1];
  shutdown_request_event.fd = GetShutdownRequestFd();
  shutdown_request_event.events = POLLIN;
  clean_request_event.fd = CleanRequestSem.GetFd();
  clean_request_event.events = POLLIN;

  for (; ; ) {
    for (auto &item : events) {
      item.revents = 0;
    }

    int ret = IfLt0(poll(&events[0], events.size(), -1));
    assert(ret > 0);

    if (shutdown_request_event.revents) {
      syslog(LOG_INFO, "Discard log cleaner thread got shutdown request");
      break;
    }

    assert(clean_request_event.revents);
    syslog(LOG_INFO,
           "Discard log cleaner thread start handling clean request");
    CleanRequestSem.Pop();

    if (!HandleCleanRequest()) {
      syslog(LOG_WARNING, "Discard log cleaner thread shutting down due to "
             "fatal error handling clean request");
      break;
    }

    syslog(LOG_INFO,
           "Discard log cleaner thread finish handling clean request");
  }
}

void TDiscardFileLogger::ParseLogPath(const char *log_path,
    std::string &log_dir, std::string &log_filename) {
  size_t len = std::strlen(log_path);

  if (len == 0) {
    THROW_ERROR(TInvalidDiscardLogPath);
  }

  if (log_path[0] != '/') {
    THROW_ERROR(TDiscardLogPathMustBeAbsolute);
  }

  /* By standard UNIX convention, a path with a trailing '/' is interpreted
     specifically as a directory.  Since the log path specifies a regular file,
     we treat this case as an error. */
  if (log_path[len - 1] == '/') {
    THROW_ERROR(TDiscardLogPathIsDir);
  }

  /* Below is a partial reimplementation of the dirname() library function.
     We avoid dirname() because it's not guaranteed to be thread safe. */

  size_t last_slash = len - 1;

  /* On loop termination, 'last_slash' is the index of the last '/' character
     in 'log_path'.  The loop is guaranteed to terminate because we already
     verified that (log_path[0] == '/'). */
  for (; log_path[last_slash] != '/'; --last_slash);

  size_t dir_last_char = last_slash;

  /* On loop termination, 'dir_last_char' will either be the index of the last
     nonslash character preceding log_path[last_slash], or 0 if no such
     nonslash character exists.  In either case, the prefix of 'log_path' whose
     last character is at index 'dir_last_char' specifies an absolute path for
     the directory containing the file given by 'log_path'.  This loop is
     necessary because by convention, adjacent '/' characters in a pathname are
     coalesced.  For instance, '/foo/bar' and '/foo//bar' specify the same
     path. */
  for (; dir_last_char && (log_path[dir_last_char] == '/'); --dir_last_char);

  /* Assign absolute path of directory containing logfile. */
  log_dir.assign(log_path, dir_last_char + 1);

  /* Assign log filename (everything after the last '/' character). */
  size_t filename_prefix_len = last_slash + 1;
  log_filename.assign(&log_path[filename_prefix_len],
                      len - filename_prefix_len);
}

const uint8_t *TDiscardFileLogger::EnforceMaxPrefixLen(const void *msg_begin,
    const void *msg_end) {
  assert(this);
  const uint8_t *p1 = reinterpret_cast<const uint8_t *>(msg_begin);
  const uint8_t *p2 = reinterpret_cast<const uint8_t *>(msg_end);
  assert(p2 >= p1);
  size_t msg_size = p2 - p1;
  return p1 + std::min(msg_size, MaxMsgPrefixLen);
}

void TDiscardFileLogger::EnforceMaxPrefixLen(std::vector<uint8_t> &msg) {
  assert(this);

  if (msg.size() > MaxMsgPrefixLen) {
    msg.resize(MaxMsgPrefixLen);
  }
}

TFd TDiscardFileLogger::OpenLogPath(const char *log_path) {
  assert(log_path);
  TFd fd = open(log_path, O_CREAT | O_APPEND | O_WRONLY,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

  if (!fd.IsOpen()) {
    char buf[256];
    Strerror(errno, buf, sizeof(buf));
    syslog(LOG_WARNING, "Disabling discard logfile mechanism due to failure "
           "to open discard logfile %s for append: %s", log_path, buf);
    DisableLogging();
  }

  return std::move(fd);
}

void TDiscardFileLogger::DisableLogging() {
  assert(this);
  Enabled = false;
  LogFd.Reset();
}

/* See if writing an entry of size 'next_entry_size' in bytes would cause the
   logfile size to exceed the limit.  If so, rename the logfile, create a new
   one in its place, and return true.  Otherwise return false. */
bool TDiscardFileLogger::CheckMaxFileSize(uint64_t next_entry_size) {
  assert(this);
  assert(Enabled);
  assert(ArchiveCleaner);
  assert(LogFd.IsOpen());
  struct stat stat_buf;
  IfLt0(fstat(LogFd, &stat_buf));

  if (!S_ISREG(stat_buf.st_mode)) {
    syslog(LOG_WARNING, "Disabling discard file logging because logfile is "
           "not a regular file: mode is 0x%lx",
           static_cast<unsigned long>(stat_buf.st_mode));
    DisableLogging();
    return false;
  }

  if ((static_cast<uint64_t>(stat_buf.st_size) <= MaxFileSize) &&
      ((MaxFileSize - stat_buf.st_size) >= next_entry_size)) {
    return false;
  }

  uint64_t epoch_ms = GetEpochMilliseconds();
  std::string rename_path(LogPath);
  rename_path += ".";
  rename_path += boost::lexical_cast<std::string>(epoch_ms);
  LogFd.Reset();
  int ret = rename(LogPath.c_str(), rename_path.c_str());

  if (ret < 0) {
    char buf[256];
    Strerror(errno, buf, sizeof(buf));
    syslog(LOG_WARNING, "Disabling discard file logging on failure to rename "
           "logfile: %s", buf);
    DisableLogging();
    return false;
  }

  LogFd = OpenLogPath(LogPath.c_str());
  return LogFd.IsOpen();
}

void TDiscardFileLogger::WriteToLog(const std::string &log_entry) {
  assert(this);

  if (log_entry.empty()) {
    return;
  }

  std::lock_guard<std::mutex> lock(Mutex);

  /* No other thread can change 'Enabled' while we hold 'Mutex'.  Even if we
     just checked 'Enabled' before grabbing 'Mutex', test it again in case it
     changed. */
  if (!Enabled) {
    return;
  }

  assert(LogFd.IsOpen());
  assert(ArchiveCleaner);

  if (CheckMaxFileSize(log_entry.size())) {
    if (ArchiveCleaner->GetShutdownWaitFd().IsReadable()) {
      syslog(LOG_WARNING, "Disabling discard file logging because discard log "
             "cleaner thread shut down unexpectedly");
      DisableLogging();
      return;
    }

    ArchiveCleaner->SendCleanRequest();
  }

  ssize_t ret = IfLt0(write(LogFd, log_entry.data(), log_entry.size()));

  if (static_cast<size_t>(ret) < log_entry.size()) {
    syslog(LOG_ERR, "write() to discard logfile returned short count: "
           "expected %lu actual %lu",
           static_cast<unsigned long>(log_entry.size()),
           static_cast<unsigned long>(ret));
  }
}
