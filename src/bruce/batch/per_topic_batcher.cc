/* <bruce/batch/per_topic_batcher.cc>

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

   Implements <bruce/batch/per_topic_batcher.h>.
 */

#include <bruce/batch/per_topic_batcher.h>

#include <utility>

#include <syslog.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;

TPerTopicBatcher::TPerTopicBatcher(const std::shared_ptr<TConfig> &config)
    : Config(config) {
}

TPerTopicBatcher::TPerTopicBatcher(std::shared_ptr<TConfig> &&config)
    : Config(std::move(config)) {
}

std::list<std::list<TMsg::TPtr>>
TPerTopicBatcher::AddMsg(TMsg::TPtr &&msg, TMsg::TTimestamp now) {
  assert(this);
  assert(msg);
  const std::string &topic = msg->GetTopic();
  auto iter = BatchMap.find(topic);

  if (iter == BatchMap.end()) {
    auto result = BatchMap.insert(
        std::make_pair(topic,
            TBatchMapEntry(Config->Get(topic), ExpiryTracker.end())));
    assert(result.second);
    iter = result.first;
  }

  std::list<std::list<TMsg::TPtr>> complete_topic_batches;
  TBatchMapEntry &entry = iter->second;
  TSingleTopicBatcher &batcher = entry.Batcher;

  if (batcher.BatchingIsEnabled()) {
    TOpt<TMsg::TTimestamp> opt_nct_initial = batcher.GetNextCompleteTime();

    if (opt_nct_initial.IsKnown() !=
        (entry.ExpiryRef != ExpiryTracker.end())) {
      assert(false);
      syslog(LOG_ERR,
             "Bug!!!  Topic batcher state out of sync with ExpiryRef: %d",
             static_cast<int>(opt_nct_initial.IsKnown()));
    }

    TMsg::TTimestamp expiry = 0;

    if (entry.ExpiryRef != ExpiryTracker.end()) {
      expiry = entry.ExpiryRef->GetExpiry();

      if (opt_nct_initial.IsUnknown() || (*opt_nct_initial != expiry)) {
        assert(false);
        TMsg::TTimestamp nct_initial =
            (opt_nct_initial.IsKnown()) ? *opt_nct_initial : 0;
        syslog(LOG_ERR, "Bug!!!  Topic batcher time limit does not match "
               "expiry tracker time limit: %lu %lu %lu",
               static_cast<unsigned long>(opt_nct_initial.IsKnown()),
               static_cast<unsigned long>(nct_initial),
               static_cast<unsigned long>(expiry));
      }
    }

    std::list<TMsg::TPtr> complete_batch = batcher.AddMsg(std::move(msg), now);
    TOpt<TMsg::TTimestamp> opt_nct_final = batcher.GetNextCompleteTime();
    bool remove_old_expiry = false;
    bool add_new_expiry = false;

    if (entry.ExpiryRef != ExpiryTracker.end()) {
      if (opt_nct_final.IsKnown()) {
        if (*opt_nct_final != expiry) {
          remove_old_expiry = true;
          add_new_expiry = true;
        }
      } else {
        remove_old_expiry = true;
      }
    } else if (opt_nct_final.IsKnown()) {
      add_new_expiry = true;
    }

    if (remove_old_expiry) {
      ExpiryTracker.erase(entry.ExpiryRef);
      entry.ExpiryRef = ExpiryTracker.end();
    }

    if (add_new_expiry) {
      auto result = ExpiryTracker.insert(
          TBatchExpiryRecord(*opt_nct_final, topic));
      assert(result.second);
      entry.ExpiryRef = result.first;
    }

    if (!complete_batch.empty()) {
      complete_topic_batches.push_back(std::move(complete_batch));
    }

    if (msg) {
      complete_batch.push_back(std::move(msg));
      complete_topic_batches.push_back(std::move(complete_batch));
    }
  }

  std::list<std::list<TMsg::TPtr>> batch_list = GetCompleteBatches(now);

  if (!complete_topic_batches.empty()) {
    batch_list.splice(batch_list.end(), std::move(complete_topic_batches));
  }

  return std::move(batch_list);
}

std::list<std::list<TMsg::TPtr>>
TPerTopicBatcher::GetCompleteBatches(TMsg::TTimestamp now) {
  assert(this);
  std::list<std::list<TMsg::TPtr>> result;

  for (TExpiryRef iter = ExpiryTracker.begin();
       (iter != ExpiryTracker.end()) && (iter->GetExpiry() <= now); ) {
    TExpiryRef curr = iter;
    ++iter;

    auto map_iter = BatchMap.find(curr->GetTopic());

    if (map_iter == BatchMap.end()) {
      assert(false);
      syslog(LOG_ERR, "Bug!!! BatchMap lookup failed in "
             "TPerTopicBatcher::GetCompleteBatches()");
      continue;
    }

    TBatchMapEntry &map_entry = map_iter->second;

    if (map_entry.ExpiryRef != curr) {
      assert(false);
      syslog(LOG_ERR, "Bug!!! TBatchMapEntry ExpiryRef is incorrect in "
             "TPerTopicBatcher::GetCompleteBatches()");
    }

    assert(!map_entry.Batcher.IsEmpty());
    result.push_back(map_entry.Batcher.TakeBatch());
    map_entry.ExpiryRef = ExpiryTracker.end();
    ExpiryTracker.erase(curr);
  }

  return std::move(result);
}

TOpt<TMsg::TTimestamp> TPerTopicBatcher::GetNextCompleteTime() const {
  assert(this);

  if (ExpiryTracker.empty()) {
    return TOpt<TMsg::TTimestamp>();
  }

  return TOpt<TMsg::TTimestamp>(ExpiryTracker.begin()->GetExpiry());
}

std::list<std::list<TMsg::TPtr>> TPerTopicBatcher::GetAllBatches() {
  assert(this);
  std::list<std::list<TMsg::TPtr>> result;
  std::list<TMsg::TPtr> batch;

  for (auto &item : BatchMap) {
    TBatchMapEntry &entry = item.second;
    batch = std::move(entry.Batcher.TakeBatch());

    if (!batch.empty()) {
      result.push_back(std::move(batch));
    }

    entry.ExpiryRef = ExpiryTracker.end();
  }

  ExpiryTracker.clear();
  return std::move(result);
}

std::list<TMsg::TPtr> TPerTopicBatcher::DeleteTopic(const std::string &topic) {
  assert(this);
  auto iter = BatchMap.find(topic);

  if (iter == BatchMap.end()) {
    return std::list<TMsg::TPtr>();
  }

  TBatchMapEntry &entry = iter->second;
  std::list<TMsg::TPtr> batch = entry.Batcher.TakeBatch();
  TExpiryRef ref = entry.ExpiryRef;

  if (ref != ExpiryTracker.end()) {
    assert(ref->GetTopic() == topic);
    assert(!batch.empty());
    ExpiryTracker.erase(ref);
  }

  BatchMap.erase(iter);
  return std::move(batch);
}

bool TPerTopicBatcher::SanityCheck() const {
  assert(this);

  for (const auto &map_item : BatchMap) {
    const std::string &topic = map_item.first;
    const TBatchMapEntry &entry = map_item.second;
    TExpiryRef expiry_iter = ExpiryTracker.begin();

    for (; expiry_iter != ExpiryTracker.end(); ++expiry_iter) {
      if (expiry_iter->GetTopic() == topic) {
        break;
      }
    }

    TOpt<TMsg::TTimestamp> opt_time_limit =
        entry.Batcher.GetNextCompleteTime();

    if (opt_time_limit.IsKnown()) {
      if ((expiry_iter == ExpiryTracker.end()) ||
          (entry.ExpiryRef != expiry_iter)) {
        return false;
      }

      if (entry.ExpiryRef->GetExpiry() != *opt_time_limit) {
        return false;
      }
    } else if ((entry.ExpiryRef != ExpiryTracker.end()) ||
               (expiry_iter != ExpiryTracker.end())) {
      return false;
    }
  }

  std::set<std::string> topic_set;

  for (const TBatchExpiryRecord &rec : ExpiryTracker) {
    if (BatchMap.find(rec.GetTopic()) == BatchMap.end()) {
      return false;
    }

    auto result = topic_set.insert(rec.GetTopic());

    if (!result.second) {
      return false;
    }
  }

  return true;
}
