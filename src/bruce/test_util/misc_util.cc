/* <bruce/test_util/misc_util.cc>

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

   Implements <bruce/test_util/misc_util.h>.
 */

#include <bruce/test_util/misc_util.h>

#include <cassert>
#include <algorithm>

#include <bruce/msg_creator.h>
#include <capped/reader.h>

using namespace Bruce;
using namespace Bruce::TestUtil;
using namespace Capped;

TMsg::TPtr TTestMsgCreator::NewMsg(const std::string &topic,
    const std::string &value, TMsg::TTimestamp timestamp, bool set_processed) {
  assert(this);
  TMsg::TPtr msg = TMsgCreator::CreateAnyPartitionMsg(timestamp, topic.data(),
      topic.data() + topic.size(), nullptr, 0, value.data(), value.size(),
      false, *Pool, MsgStateTracker);

  if (set_processed) {
    SetProcessed(msg);
  }

  return std::move(msg);
}

bool Bruce::TestUtil::KeyEquals(const TMsg::TPtr &msg, const char *key) {
  TReader reader(&msg->GetKeyAndValue());
  std::vector<char> buf(msg->GetKeySize());
  reader.Read(&buf[0], buf.size());
  std::string key_str(&buf[0], &buf[0] + buf.size());
  return (key_str == key);
}

bool Bruce::TestUtil::ValueEquals(const TMsg::TPtr &msg, const char *value) {
  TReader reader(&msg->GetKeyAndValue());
  reader.Skip(msg->GetKeySize());  // skip key
  std::vector<char> buf(msg->GetValueSize());
  reader.Read(&buf[0], buf.size());
  std::string value_str(&buf[0], &buf[0] + buf.size());
  return (value_str == value);
}

std::list<TMsg::TPtr>
Bruce::TestUtil::SetProcessed(std::list<TMsg::TPtr> &&msg_list) {
  for (const TMsg::TPtr &msg_ptr : msg_list) {
    assert(msg_ptr);
    SetProcessed(msg_ptr);
  }

  return std::move(msg_list);
}

std::list<std::list<TMsg::TPtr>>
Bruce::TestUtil::SetProcessed(
    std::list<std::list<TMsg::TPtr>> &&msg_list_list) {
  for (const std::list<TMsg::TPtr> &msg_list : msg_list_list) {
    for (const TMsg::TPtr &msg_ptr : msg_list) {
      assert(msg_ptr);
      SetProcessed(msg_ptr);
    }
  }

  return std::move(msg_list_list);
}
