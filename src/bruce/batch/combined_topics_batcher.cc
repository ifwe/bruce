/* <bruce/batch/combined_topics_batcher.cc>

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

   Implements <bruce/batch/combined_topics_batcher.h>.
 */

#include <bruce/batch/combined_topics_batcher.h>

#include <limits>
#include <utility>

#include <base/no_default_case.h>
#include <bruce/batch/batch_config.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;
using namespace Bruce::Util;

TCombinedTopicsBatcher::TCombinedTopicsBatcher(const TConfig &config)
    : CoreState(config.BatchConfig),
      TopicFilter(config.TopicFilter),
      ExcludeTopicFilter(config.ExcludeTopicFilter) {
}

bool TCombinedTopicsBatcher::BatchingIsEnabled() const {
  assert(this);
  return (ExcludeTopicFilter || !TopicFilter->empty()) &&
         CoreState.BatchingIsEnabled();
}

bool TCombinedTopicsBatcher::BatchingIsEnabled(
    const std::string &topic) const {
  assert(this);

  if (!CoreState.BatchingIsEnabled()) {
    return false;
  }

  if (TopicFilter->empty()) {
    return ExcludeTopicFilter;
  }

  return ((TopicFilter->find(topic) == TopicFilter->end()) ==
          ExcludeTopicFilter);
}

std::list<std::list<TMsg::TPtr>>
TCombinedTopicsBatcher::AddMsg(TMsg::TPtr &&msg, TMsg::TTimestamp now) {
  assert(this);
  assert(msg);
  const std::string &topic = msg->GetTopic();

  if (!BatchingIsEnabled(topic)) {
    TOpt<TMsg::TTimestamp> opt_nct = GetNextCompleteTime();

    if (opt_nct.IsKnown() && (now >= *opt_nct)) {
      return TakeBatch();
    }

    return std::list<std::list<TMsg::TPtr>>();
  }

  switch (CoreState.ProcessNewMsg(now, msg)) {
    case TBatcherCore::TAction::LeaveMsgAndReturnBatch: {
      break;
    }
    case TBatcherCore::TAction::ReturnBatchAndTakeMsg: {
      std::list<std::list<TMsg::TPtr>> result = TopicMap.Get();
      TopicMap.Put(std::move(msg));
      return std::move(result);
    }
    case TBatcherCore::TAction::TakeMsgAndReturnBatch: {
      TopicMap.Put(std::move(msg));
      break;
    }
    case TBatcherCore::TAction::TakeMsgAndLeaveBatch: {
      TopicMap.Put(std::move(msg));
      return std::list<std::list<TMsg::TPtr>>();
    }
    NO_DEFAULT_CASE;
  }

  return TopicMap.Get();
}

std::list<std::list<TMsg::TPtr>>
TCombinedTopicsBatcher::TakeBatch() {
  assert(this);
  std::list<std::list<TMsg::TPtr>> result = TopicMap.Get();
  CoreState.ClearState();
  return std::move(result);
}
