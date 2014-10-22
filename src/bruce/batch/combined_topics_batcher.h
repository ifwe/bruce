/* <bruce/batch/combined_topics_batcher.h>

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

   Class for batching messages separated by topic, but with a single
   TBatchConfig for all topics.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/batch/batcher_core.h>
#include <bruce/msg.h>
#include <bruce/util/topic_map.h>

namespace Bruce {

  namespace Batch {

    class TCombinedTopicsBatcher final {
      NO_COPY_SEMANTICS(TCombinedTopicsBatcher);

      public:
      using TTopicFilter = std::unordered_set<std::string>;

      class TConfig final {
        public:
        TConfig()
            : TopicFilter(new TTopicFilter),
              ExcludeTopicFilter(false) {
        }

        TConfig(const TBatchConfig &batch_config,
            const std::shared_ptr<TTopicFilter> &topic_filter,
            bool exclude_topic_filter)
            : BatchConfig(batch_config),
              TopicFilter(topic_filter),
              ExcludeTopicFilter(exclude_topic_filter) {
        }

        TConfig(const TBatchConfig &batch_config,
            std::shared_ptr<TTopicFilter> &&topic_filter,
            bool exclude_topic_filter)
            : BatchConfig(batch_config),
              TopicFilter(std::move(topic_filter)),
              ExcludeTopicFilter(exclude_topic_filter) {
        }

        TConfig(const TConfig &) = default;

        TConfig(TConfig &&) = default;

        TConfig& operator=(const TConfig &) = default;

        TConfig& operator=(TConfig &&) = default;

        private:
        TBatchConfig BatchConfig;

        std::shared_ptr<TTopicFilter> TopicFilter;

        bool ExcludeTopicFilter;

        friend class TCombinedTopicsBatcher;
      };  // TConfig

      explicit TCombinedTopicsBatcher(const TConfig &config);

      TConfig GetConfig() const {
        assert(this);
        return TConfig(CoreState.GetConfig(), TopicFilter, ExcludeTopicFilter);
      }

      bool IsEmpty() const {
        assert(this);
        assert(CoreState.IsEmpty() == TopicMap.IsEmpty());
        return CoreState.IsEmpty();
      }

      /* A true return value indicates that batching is enabled for at least
         one topic. */
      bool BatchingIsEnabled() const;

      /* Return true if batching is enabled for the given topic. */
      bool BatchingIsEnabled(const std::string &topic) const;

      std::list<std::list<TMsg::TPtr>>
      AddMsg(TMsg::TPtr &&msg, TMsg::TTimestamp now);

      Base::TOpt<TMsg::TTimestamp> GetNextCompleteTime() const {
        assert(this);
        return CoreState.GetNextCompleteTime();
      }

      /* Empty out the batcher, and return all messages it contained, grouped
         by topic. */
      std::list<std::list<TMsg::TPtr>> TakeBatch();

      private:
      TBatcherCore CoreState;

      std::shared_ptr<TTopicFilter> TopicFilter;

      bool ExcludeTopicFilter;

      /* Messages are stored here, grouped by topic. */
      Util::TTopicMap TopicMap;
    };  // TCombinedTopicsBatcher

  }  // Batch

}  // Bruce
