/* <bruce/util/topic_map.h>

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

   Utility class for grouping messages by topic.
 */

#pragma once

#include <cassert>
#include <list>
#include <string>
#include <unordered_map>

#include <base/no_copy_semantics.h>
#include <bruce/msg.h>

namespace Bruce {

  namespace Util {

    class TTopicMap final {
      NO_COPY_SEMANTICS(TTopicMap);

      public:
      TTopicMap() = default;

      TTopicMap(TTopicMap &&) = default;

      TTopicMap &operator=(TTopicMap &&) = default;

      /* A true value indicates that map contains no messages. */
      bool IsEmpty() const {
        assert(this);
        return TopicHash.empty();
      }

      void Clear() {
        assert(this);
        TopicHash.clear();
      }

      /* Put single message. */
      void Put(TMsg::TPtr &&msg);

      /* Put batch of messages that all have same topic.  Caller is trusted to
         make sure all messages in batch have same topic. */
      void Put(std::list<TMsg::TPtr> &&batch);

      void Put(std::list<std::list<TMsg::TPtr>> &&batch_list);

      /* Remove all messages for the given topic and return them in a list.
         Returned list will be empty if no messages for topic were found. */
      std::list<TMsg::TPtr> Get(const std::string &topic);

      /* Remove all messages, grouped by topic. */
      std::list<std::list<TMsg::TPtr>> Get();

      private:
      std::list<TMsg::TPtr> &PutCommon(const std::string &topic);

      /* Key is topic.  Value is list of messages for topic. */
      std::unordered_map<std::string, std::list<TMsg::TPtr>> TopicHash;
    };  // TTopicMap

  }  // Util

}  // Bruce
