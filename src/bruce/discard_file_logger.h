/* <bruce/discard_file_logger.h>

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

   Class for logging discards to a file.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/thrower.h>
#include <bruce/msg.h>
#include <bruce/util/worker_thread.h>

namespace Bruce {

  /* Class for writing discarded and possibly duplicated messages to a logfile
     for debugging.  Also handles logfile rotation.  Doing logfile rotation
     within bruce rather than relying on logrotate eliminates any possibility
     of a log entry getting split across files.  Deleting old logfiles is done
     in a separate thread, since this may be a slow operation and we don't want
     the input thread to get delayed. */
  class TDiscardFileLogger final {
    NO_COPY_SEMANTICS(TDiscardFileLogger);

    public:
    DEFINE_ERROR(TDiscardLogPathMustBeAbsolute, std::runtime_error,
                 "Discard log path must be absolute");

    DEFINE_ERROR(TDiscardLogPathIsDir, std::runtime_error,
                 "Discard log path must not specify a directory");

    DEFINE_ERROR(TInvalidDiscardLogPath, std::runtime_error,
                 "Invalid discard log path");

    enum class TDiscardReason {
      Bug,
      FailedDeliveryAttemptLimit,
      KafkaErrorAck,
      ServerShutdown,
      NoAvailablePartitions,
      RateLimit,
      FailedTopicAutocreate
    };

    TDiscardFileLogger();

    ~TDiscardFileLogger() noexcept;

    /* Must be called only during server startup _before_ any log entries can
       be written, since this initializes the internal state of the singleton
       without any thread synchronization.  If this method is never called, the
       singleton will still be in a sane state, but logging will be disabled.

       'log_path' specifies the absolute pathname of the logfile.
       'max_file_size' specifies the maximum logfile size in bytes.  When the
       logfile size reaches a point where the next log entry would cause it to
       exceed 'max_file_size', a new logfile is started as follows, assuming
       that the logfile name is '/var/log/bruce/discard.log':

           1.  The current logfile is renamed.  Suppose the current time in
               milliseconds since the epoch is 1393271771000.  Then the logfile
               will be renamed to '/var/log/bruce/discard.log.1393271771000'.

           2.  A new file '/var/log/bruce/discard.log' is created.  The next
               log entry goes to this file.

       'max_archive_size' specifies a limit on the combined size of old
       logfiles, which are created as described above.  Each time a new logfile
       is started, a thread is awakened which scans the logfile directory
       ('/var/log/bruce' in the above example) for old logfiles.  In the above
       example, old logfiles are recognized as files whose names match the
       pattern 'discard.log.N' where 'N' is a string of digits interpreted as
       an integer number of milliseconds since the epoch.  If the combined size
       of all old logfiles exceeds 'max_archive_size', then old logfiles are
       deleted in order from oldest to newest until 'max_archive_size' is no
       longer exceeded. */
    void Init(const char *log_path, uint64_t max_file_size,
              uint64_t max_archive_size, size_t max_msg_prefix_len,
              bool use_old_output_format);

    /* Call this to disable logging and shut down the thread that deletes old
       logfiles. */
    void Shutdown();

    /* Write a log entry indicating that 'msg' is being discarded for the
       reason given by 'reason'. */
    void LogDiscard(const TMsg &msg, TDiscardReason reason);

    /* Same as above, except message is passed by smart pointer. */
    void LogDiscard(const TMsg::TPtr &msg_ptr, TDiscardReason reason) {
      assert(this);
      LogDiscard(*msg_ptr, reason);
    }

    /* Write a log entry indicating that 'msg' may get duplicated due to loss
       of communication with a broker before an ACK was received. */
    void LogDuplicate(const TMsg &msg);

    /* Same as above, except message is passed by smart pointer. */
    void LogDuplicate(const TMsg::TPtr &msg_ptr) {
      assert(this);
      LogDuplicate(*msg_ptr);
    }

    /* Write a log entry indicating that a message is being discarded due to
       lack of buffer space.  'timestamp' gives the message timestamp.
       'topic_begin' points to the first byte of the topic, and 'topic_end'
       points one position past the last byte of the topic.  Likewise,
       'body_begin' and 'body_end' specify the message body. */
    void LogNoMemDiscard(TMsg::TTimestamp timestamp, const char *topic_begin,
        const char *topic_end, const void *key_begin, const void *key_end,
        const void *value_begin, const void *value_end);

    /* Write a log entry indicating that a malformed message is being
       discarded.  'msg_begin' points to the first byte of the message.
       'msg_end' points one position past the last byte of the message. */
    void LogMalformedMsgDiscard(const void *msg_begin, const void *msg_end);

    /* Write a log entry indicating that a message is being discarded due to
       unsupported API key.  'msg_begin' points to the first byte of the
       message.  'msg_end' points one position past the last byte of the
       message.  'version' gives the unsupported message version. */
    void LogUnsupportedApiKeyDiscard(const void *msg_begin,
        const void *msg_end, int api_key);

    /* Write a log entry indicating that a message is being discarded due to
       unsupported message version.  'msg_begin' points to the first byte of
       the message.  'msg_end' points one position past the last byte of the
       message.  'version' gives the unsupported message version. */
    void LogUnsupportedMsgVersionDiscard(const void *msg_begin,
        const void *msg_end, int version);

    /* Write a log entry indicating that a message is being discarded due to an
       invalid topic.  'timestamp' gives the message timestamp.  'topic_begin'
       points to the first byte of the topic, and 'topic_end' points one
       position past the last byte of the topic.  Likewise, 'body_begin' and
       'body_end' specify the message body. */
    void LogBadTopicDiscard(TMsg::TTimestamp timestamp,
        const char *topic_begin, const char *topic_end, const void *key_begin,
        const void *key_end, const void *value_begin, const void *value_end);

    /* Write a log entry indicating that 'msg' is being discarded due to an
       invalid topic. */
    void LogBadTopicDiscard(const TMsg &msg);

    /* Same as above, except message is passed by smart pointer. */
    void LogBadTopicDiscard(const TMsg::TPtr &msg_ptr) {
      assert(this);
      LogBadTopicDiscard(*msg_ptr);
    }

    /* Write a log entry indicating that 'msg' is being discarded due to
       excessive message length. */
    void LogLongMsgDiscard(const TMsg &msg);

    /* Same as above, except message is passed by smart pointer. */
    void LogLongMsgDiscard(const TMsg::TPtr &msg_ptr) {
      assert(this);
      LogLongMsgDiscard(*msg_ptr);
    }

    private:
    /* Thread for deleting old logfiles gets awakened each time a new logfile
       is started. */
    class TArchiveCleaner final : public Util::TWorkerThread {
      NO_COPY_SEMANTICS(TArchiveCleaner);

      public:
      TArchiveCleaner(uint64_t max_archive_size, const char *log_dir,
                      const char *log_filename);

      virtual ~TArchiveCleaner() noexcept;

      /* Awaken thread.  It will then scan the log directory and delete old
         logfiles as necessary. */
      void SendCleanRequest() {
        assert(this);
        CleanRequestSem.Push();
      }

      protected:

      virtual void Run() override;

      private:

      bool HandleCleanRequest();

      void DoRun();

      const uint64_t MaxArchiveSize;

      const std::string LogDir;

      const std::string LogFilename;

      Base::TEventSemaphore CleanRequestSem;
    };  // TArchiveCleaner

    static void ParseLogPath(const char *log_path, std::string &log_dir,
        std::string &log_filename);

    const uint8_t *EnforceMaxPrefixLen(const void *msg_begin,
        const void *msg_end);

    void EnforceMaxPrefixLen(std::vector<uint8_t> &msg);

    Base::TFd OpenLogPath(const char *log_path);

    void DisableLogging();

    bool CheckMaxFileSize(uint64_t next_entry_size);

    void WriteToLog(const std::string &log_entry);

    size_t MaxMsgPrefixLen;

    bool UseOldOutputFormat;

    /* Protects everything below.  However, reads of boolean 'Enabled' value
       may occur without acquiring 'Mutex' */
    std::mutex Mutex;

    /* Indicates whether logging is enabled.  Threads may read this value
       without holding 'Mutex', but value is never modified without holding
       'Mutex'. */
    bool Enabled;

    /* Thread that deletes old logfiles. */
    std::unique_ptr<TArchiveCleaner> ArchiveCleaner;

    /* Absolute pathname of logfile.  For instance,
       '/var/log/bruce/discard.log'. */
    std::string LogPath;

    /* Absolute path of directory containing logfile.  For instance,
       '/var/log/bruce'. */
    std::string LogDir;

    /* Name of logfile.  For instance, 'discard.log'. */
    std::string LogFilename;

    /* Upper bound on size in bytes of logfile. */
    uint64_t MaxFileSize;

    /* Upper bound on combined size in bytes of old logfiles.  May be briefly
       violated until log cleaning thread runs. */
    uint64_t MaxArchiveSize;

    /* Descriptor for logfile. */
    Base::TFd LogFd;
  };  // TDiscardFileLogger

}  // Bruce
