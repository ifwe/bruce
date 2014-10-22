/* <bruce/mock_kafka_server/cmd_bucket.cc>

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

   Implements <bruce/mock_kafka_server/cmd_bucket.h>.
 */

#include <bruce/mock_kafka_server/cmd_bucket.h>

#include <cassert>

#include <base/debug_log.h>

using namespace Bruce;
using namespace Bruce::MockKafkaServer;

void TCmdBucket::Put(const TCmd &cmd) {
  assert(this);
  DEBUG_LOG("append cmd to queue");

  std::lock_guard<std::mutex> lock(Mutex);
  CmdQueue.push_back(std::make_pair(++SequenceNumCounter, cmd));
}

bool TCmdBucket::CopyOut(size_t &sequence_num, TCmd &out_cmd) const {
  assert(this);
  std::lock_guard<std::mutex> lock(Mutex);

  if (CmdQueue.empty()) {
    return false;
  }

  const std::pair<size_t, TCmd> &first_item = CmdQueue.front();
  out_cmd = first_item.second;
  sequence_num = first_item.first;
  return true;
}

bool TCmdBucket::Remove(size_t sequence_num) {
  assert(this);
  bool ret = true;

  {
    std::lock_guard<std::mutex> lock(Mutex);

    if (CmdQueue.empty()) {
      ret = false;
    }

    if (sequence_num != CmdQueue.front().first) {
      ret = false;
    }

    CmdQueue.pop_front();
  }

  if (ret) {
    DEBUG_LOG("remove cmd from queue");
  }

  return ret;
}
