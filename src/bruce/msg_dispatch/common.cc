/* <bruce/msg_dispatch/common.cc>

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

   Implements <bruce/msg_dispatch/common.h>.
 */

#include <bruce/msg_dispatch/common.h>

#include <algorithm>

using namespace Bruce;
using namespace Bruce::MsgDispatch;

void Bruce::MsgDispatch::EmptyAllTopics(TAllTopics &all_topics,
    std::list<std::list<TMsg::TPtr>> &dest) {
  for (auto &topic_elem : all_topics) {
    for (auto &partition_elem : topic_elem.second) {
      std::list<TMsg::TPtr> &msg_set = partition_elem.second.Contents;
      assert(!msg_set.empty());
      dest.push_back(std::move(msg_set));
    }
  }

  all_topics.clear();
}
