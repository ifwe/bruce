/* <bruce/util/topic_map.cc>

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

   Implements <bruce/util/topic_map.h>.
 */

#include <bruce/util/topic_map.h>

#include <algorithm>
#include <utility>

using namespace Bruce;
using namespace Bruce::Util;

void TTopicMap::Put(TMsg::TPtr &&msg) {
  assert(this);
  assert(msg);
  std::list<TMsg::TPtr> &msg_list = PutCommon(msg->GetTopic());
  msg_list.push_back(std::move(msg));
}

void TTopicMap::Put(std::list<TMsg::TPtr> &&batch) {
  assert(this);
  assert(!batch.empty());
  std::list<TMsg::TPtr> &msg_list = PutCommon(batch.front()->GetTopic());
  msg_list.splice(msg_list.end(), std::move(batch));
}

void TTopicMap::Put(std::list<std::list<TMsg::TPtr>> &&batch_list) {
  assert(this);

  for (std::list<TMsg::TPtr> &batch : batch_list) {
    Put(std::move(batch));
  }

  batch_list.clear();
}

std::list<TMsg::TPtr> TTopicMap::Get(const std::string &topic) {
  assert(this);
  std::list<TMsg::TPtr> result;
  auto iter = TopicHash.find(topic);

  if (iter != TopicHash.end()) {
    result = std::move(iter->second);
    TopicHash.erase(iter);
  }

  return std::move(result);
}

std::list<std::list<TMsg::TPtr>> TTopicMap::Get() {
  assert(this);
  std::list<std::list<TMsg::TPtr>> result;

  for (auto &item : TopicHash) {
    if (!item.second.empty()) {
      result.push_back(std::move(item.second));
    }
  }

  TopicHash.clear();
  return std::move(result);
}

std::list<TMsg::TPtr> &TTopicMap::PutCommon(const std::string &topic) {
  assert(this);

  /* We can eliminate this call to find() without affecting observed behavior.
     However, the common case should be a successful lookup, and then we avoid
     creating a temporary topic string while doing the insert. */
  auto iter = TopicHash.find(topic);

  if (iter != TopicHash.end()) {
    return iter->second;
  }

  auto result =
      TopicHash.insert(std::make_pair(topic, std::list<TMsg::TPtr>()));
  return result.first->second;
}
