/* <bruce/anomaly_tracker.cc>

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

   Implements <bruce/anomaly_tracker.h>.
 */

#include <bruce/anomaly_tracker.h>

#include <cstring>
#include <utility>
#include <vector>

#include <syslog.h>

#include <base/error_utils.h>
#include <capped/blob.h>
#include <capped/reader.h>
#include <server/counter.h>

using namespace Base;
using namespace Bruce;
using namespace Capped;

SERVER_COUNTER(NoDiscardQuery);

/* 's' is the most recently seen item in 'lru_list' (maintained in LRU order).
   Update 'lru_list' accordingly, deleting the least recently seen item as
   necessary to prevent 'lru_list' from exceeding 'max_list_size' items. */
static void UpdateLruList(std::string &&s, std::list<std::string> &lru_list,
    size_t max_list_size) {
  assert(lru_list.size() <= max_list_size);

  /* A linear search here is ok as long as max_list_size is reasonably
     small. */
  auto iter = std::find(lru_list.begin(), lru_list.end(), s);

  if (iter == lru_list.end()) {
    /* The most recently seen item is at the front. */
    lru_list.push_front(std::move(s));

    if (lru_list.size() > max_list_size) {
      /* Limit list size by deleting least recently seen item. */
      lru_list.pop_back();
    }
  } else {
    /* Move the item to the front of the list, since it is now the most
       recently seen. */
    lru_list.splice(lru_list.begin(), lru_list, iter);
  }

  assert(lru_list.size() <= max_list_size);
}

size_t TAnomalyTracker::GetNoDiscardQueryCount() {
  Server::TCounter::Sample();
  return NoDiscardQuery.GetCount();
}

void TAnomalyTracker::TrackDiscard(TMsg &msg, TDiscardReason reason) {
  assert(this);
  uint64_t now = ClockFn();
  DiscardFileLogger.LogDiscard(msg, reason);
  const std::string &msg_topic = msg.GetTopic();
  std::string topic(msg_topic);

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  UpdateTopicMap(FillingReport->DiscardTopicMap, std::move(topic),
                 msg.GetTimestamp());

  if (reason == TDiscardReason::RateLimit) {
    TRateLimitMap &rmap = FillingReport->RateLimitDiscardMap;
    auto iter = rmap.find(msg_topic);

    if (iter == rmap.end()) {
      /* First rate limit discard for topic in current reporting period. */
      rmap[msg_topic] = 1;
    } else {
      ++iter->second;
    }
  }
}

void TAnomalyTracker::TrackDuplicate(const TMsg &msg) {
  assert(this);
  uint64_t now = ClockFn();
  DiscardFileLogger.LogDuplicate(msg);
  std::string topic(msg.GetTopic());

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  UpdateTopicMap(FillingReport->DuplicateTopicMap, std::move(topic),
                 msg.GetTimestamp());
}

void TAnomalyTracker::TrackNoMemDiscard(TMsg::TTimestamp timestamp,
    const char *topic_begin, const char *topic_end, const void *key_begin,
    const void *key_end, const void *value_begin, const void *value_end) {
  assert(this);
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(key_begin || (key_end == key_begin));
  assert(key_end >= key_begin);
  assert(value_begin || (value_end == value_begin));
  assert(value_end >= value_begin);
  uint64_t now = ClockFn();
  DiscardFileLogger.LogNoMemDiscard(timestamp, topic_begin, topic_end,
      key_begin, key_end, value_begin, value_end);
  std::string topic(topic_begin, topic_end);

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  UpdateTopicMap(FillingReport->DiscardTopicMap, std::move(topic), timestamp);
}

void TAnomalyTracker::TrackMalformedMsgDiscard(const void *prefix_begin,
    const void *prefix_end) {
  assert(this);
  uint64_t now = ClockFn();
  DiscardFileLogger.LogMalformedMsgDiscard(prefix_begin, prefix_end);
  prefix_end = EnforceMaxPrefixLen(prefix_begin, prefix_end);
  std::string msg_prefix(reinterpret_cast<const char *>(prefix_begin),
                         reinterpret_cast<const char *>(prefix_end));

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  ++FillingReport->MalformedMsgCount;
  UpdateLruList(std::move(msg_prefix), FillingReport->MalformedMsgs,
                MAX_MALFORMED_MSGS);
}

void TAnomalyTracker::TrackUnsupportedApiKeyDiscard(
    const void *prefix_begin, const void *prefix_end, int api_key) {
  assert(this);
  uint64_t now = ClockFn();
  DiscardFileLogger.LogUnsupportedApiKeyDiscard(prefix_begin, prefix_end,
      api_key);

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  ++FillingReport->UnsupportedApiKeyMsgCount;
}

void TAnomalyTracker::TrackUnsupportedMsgVersionDiscard(
    const void *prefix_begin, const void *prefix_end, int version) {
  assert(this);
  uint64_t now = ClockFn();
  DiscardFileLogger.LogUnsupportedMsgVersionDiscard(prefix_begin, prefix_end,
      version);

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  ++FillingReport->UnsupportedVersionMsgCount;
  auto result = FillingReport->UnsupportedVersionMsgs.insert(
      std::make_pair(version, 1));

  if (!result.second) {
    ++result.first->second;
  }
}

void TAnomalyTracker::TrackBadTopicDiscard(TMsg::TTimestamp timestamp,
    const char *topic_begin, const char *topic_end, const void *key_begin,
    const void *key_end, const void *value_begin, const void *value_end) {
  assert(this);
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(key_begin || (key_end == key_begin));
  assert(key_end >= key_begin);
  assert(value_begin || (value_end == value_begin));
  assert(value_end >= value_begin);
  uint64_t now = ClockFn();
  DiscardFileLogger.LogBadTopicDiscard(timestamp, topic_begin, topic_end,
      key_begin, key_end, value_begin, value_end);
  std::string topic(topic_begin, topic_end);

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  ++FillingReport->BadTopicMsgCount;
  UpdateLruList(std::move(topic), FillingReport->BadTopics, MAX_BAD_TOPICS);
}

void TAnomalyTracker::TrackBadTopicDiscard(TMsg &msg) {
  assert(this);
  uint64_t now = ClockFn();
  DiscardFileLogger.LogBadTopicDiscard(msg);
  std::string topic(msg.GetTopic());

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  ++FillingReport->BadTopicMsgCount;
  UpdateLruList(std::move(topic), FillingReport->BadTopics, MAX_BAD_TOPICS);
}

void TAnomalyTracker::TrackLongMsgDiscard(TMsg &msg) {
  assert(this);

  std::string topic(msg.GetTopic());
  size_t value_size = msg.GetValueSize();
  std::vector<uint8_t> tmp_buf(topic.size() + 1 + value_size);
  std::memcpy(&tmp_buf[0], topic.data(), topic.size());
  tmp_buf[topic.size()] = ' ';

  TReader reader(&msg.GetKeyAndValue());
  reader.Skip(msg.GetKeySize());
  reader.Read(&tmp_buf[topic.size() + 1], value_size);

  const uint8_t *msg_begin = &tmp_buf[0];
  const uint8_t *msg_end = EnforceMaxPrefixLen(msg_begin,
      msg_begin + tmp_buf.size());
  std::string tmp_msg_prefix(reinterpret_cast<const char *>(msg_begin),
                             reinterpret_cast<const char *>(msg_end));
  uint64_t now = ClockFn();
  DiscardFileLogger.LogLongMsgDiscard(msg);

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  UpdateLruList(std::move(tmp_msg_prefix), FillingReport->LongMsgs,
                MAX_LONG_MSGS);
  UpdateTopicMap(FillingReport->DiscardTopicMap, std::move(topic),
                 msg.GetTimestamp());
}

std::shared_ptr<const TAnomalyTracker::TInfo>
TAnomalyTracker::GetInfo(TInfo &filling_report_copy) const {
  assert(this);
  uint64_t now = ClockFn();
  LastGetInfoTime.store(now);

  std::lock_guard<std::mutex> lock(Mutex);
  AdvanceReportPeriod(now);
  filling_report_copy = *FillingReport;
  return LastFullReport;
}

void TAnomalyTracker::CheckGetInfoRate() const {
  assert(this);
  uint64_t now = ClockFn();
  uint64_t last_get_info_time = LastGetInfoTime.load();

  /* Deal with possibly nonmonotonic clock. */
  uint64_t delta = (now >= last_get_info_time) ?
      (now - last_get_info_time) : 0;

  if (delta > ReportInterval) {
    NoDiscardQuery.Increment();
    syslog(LOG_WARNING, "No discard query in last %llu seconds: possible loss "
           "of discard info", static_cast<unsigned long long>(delta));
  }
}

const uint8_t *TAnomalyTracker::EnforceMaxPrefixLen(const void *msg_begin,
    const void *msg_end) {
  assert(this);
  assert(msg_end >= msg_begin);
  const uint8_t *begin = reinterpret_cast<const uint8_t *>(msg_begin);
  const uint8_t *end = reinterpret_cast<const uint8_t *>(msg_end);
  size_t msg_size = end - begin;
  return begin + std::min(msg_size, MaxMsgPrefixLen);
}

void TAnomalyTracker::AdvanceReportPeriod(uint64_t now) const {
  assert(this);
  assert(FillingReport);
  assert((FillingReport->GetReportId() == 0) || LastFullReport);

  if (ReportInterval == 0) {
    /* Interval reporting is disabled. */
    return;
  }

  size_t current_id = FillingReport->GetReportId();
  uint64_t current_start = FillingReport->GetStartTime();

  /* Deal with possibly nonmonotonic clock. */
  uint64_t now_offset = (now >= current_start) ? now - current_start : 0;

  /* Compute how many increments we must advance the current report ID by. */
  size_t period_offset = now_offset / ReportInterval;

  switch (period_offset) {
    case 0: {
      /* Still in same reporting period: nothing to do. */
      break;
    }
    case 1: {
      /* The reporting period has advanced by 1 since the last time this method
         was called.  FillingReport becomes LastFullReport, and we then
         initialize FillingReport with a new empty report. */
      LastFullReport.reset(FillingReport.release());
      FillingReport.reset(new TInfo(current_id + 1,
                                    current_start + ReportInterval));
      break;
    }
    default: {
      /* The reporting period has advanced by more than 1 since the last time
         this method was called.  FillingReport gets discarded and replaced
         with an empty report for the current reporting period.  LastFullReport
         gets replaced with an empty report for the previous reporting period.
         If we get here, then the discard monitoring script isn't monitoring
         bruce frequently enough. */
      size_t prev_period_offset = period_offset - 1;
      size_t prev_id = current_id + prev_period_offset;
      uint64_t prev_start = current_start +
                          (prev_period_offset * ReportInterval);
      LastFullReport.reset(new TInfo(prev_id, prev_start));
      FillingReport.reset(new TInfo(prev_id + 1, prev_start + ReportInterval));
      break;
    }
  }

  assert(FillingReport);
  assert((FillingReport->GetReportId() == 0) || LastFullReport);
}

void TAnomalyTracker::UpdateTopicMap(TMap &topic_map, std::string &&topic,
    TMsg::TTimestamp timestamp) {
  assert(this);
  auto result = topic_map.insert(
      std::make_pair(std::move(topic), TTopicInfo(timestamp)));

  if (result.second) {
    /* First entry for that topic: new map item inserted. */
    return;
  }

  /* There has already been at least one entry for that topic.  Update the info
     for the existing map item. */
  TTopicInfo &info = result.first->second;
  TInterval &interval = info.Interval;
  ++info.Count;

  if (timestamp < interval.First) {
    interval.First = timestamp;
  } else if (interval.Last < timestamp) {
    interval.Last = timestamp;
  }
}
