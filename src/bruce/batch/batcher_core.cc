/* <bruce/batch/batcher_core.cc>

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

   Implements <bruce/batch/batcher_core.h>.
 */

#include <bruce/batch/batcher_core.h>

#include <algorithm>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Batch;

TBatcherCore::TBatcherCore()
    : MinTimestamp(std::numeric_limits<TMsg::TTimestamp>::max()),
      MsgCount(0),
      ByteCount(0) {
}

TBatcherCore::TBatcherCore(const TBatchConfig &config)
    : Config(config),
      MinTimestamp(std::numeric_limits<TMsg::TTimestamp>::max()),
      MsgCount(0),
      ByteCount(0) {
}

TOpt<TMsg::TTimestamp> TBatcherCore::GetNextCompleteTime() const {
  assert(this);

  if (IsEmpty() || !TimeLimitIsEnabled(Config)) {
    return TOpt<TMsg::TTimestamp>();
  }

  return TOpt<TMsg::TTimestamp>(MinTimestamp + Config.TimeLimit);
}

TBatcherCore::TAction
TBatcherCore::ProcessNewMsg(TMsg::TTimestamp now, const TMsg::TPtr &msg) {
  assert(this);
  assert(msg);

  if (!Bruce::Batch::BatchingIsEnabled(Config)) {
    return TAction::LeaveMsgAndReturnBatch;
  }

  TMsg::TTimestamp timestamp = msg->GetTimestamp();
  size_t body_size = msg->GetKeyAndValue().Size();

  if (ByteCountLimitIsEnabled(Config) && (body_size >= Config.ByteCount)) {
    ClearState();
    return TAction::LeaveMsgAndReturnBatch;
  }

  if (TestByteCountExceeded(body_size)) {
    ClearState();

    if (TestMsgCount(true) || TestTimeLimit(now, timestamp)) {
      return TAction::LeaveMsgAndReturnBatch;
    }

    UpdateState(timestamp, body_size);
    return TAction::ReturnBatchAndTakeMsg;
  }

  UpdateState(timestamp, body_size);

  if (TestAllLimits(now)) {
    ClearState();
    return TAction::TakeMsgAndReturnBatch;
  }

  return TAction::TakeMsgAndLeaveBatch;
}

void TBatcherCore::ClearState() {
  assert(this);
  MinTimestamp = std::numeric_limits<TMsg::TTimestamp>::max();
  MsgCount = 0;
  ByteCount = 0;
}

bool TBatcherCore::TestTimeLimit(TMsg::TTimestamp now,
    TMsg::TTimestamp new_msg_timestamp) const {
  assert(this);

  if (!TimeLimitIsEnabled(Config)) {
    return false;
  }

  TMsg::TTimestamp min_ts = std::min(MinTimestamp, new_msg_timestamp);
  return (now >= static_cast<TMsg::TTimestamp>(min_ts + Config.TimeLimit));
}

bool TBatcherCore::TestMsgCount(bool adding_msg) const {
  assert(this);

  if (!MsgCountLimitIsEnabled(Config)) {
    return false;
  }

  size_t to_add = adding_msg ? 1 : 0;
  return ((MsgCount + to_add) >= Config.MsgCount);
}

bool TBatcherCore::TestByteCount(size_t bytes_to_add) const {
  assert(this);

  if (!ByteCountLimitIsEnabled(Config)) {
    return false;
  }

  return ((ByteCount + bytes_to_add) >= Config.ByteCount);
}

bool TBatcherCore::TestByteCountExceeded(size_t bytes_to_add) const {
  assert(this);

  if (!ByteCountLimitIsEnabled(Config)) {
    return false;
  }

  return ((ByteCount + bytes_to_add) > Config.ByteCount);
}

void TBatcherCore::UpdateState(TMsg::TTimestamp timestamp, size_t body_size) {
  assert(this);
  MinTimestamp = std::min(MinTimestamp, timestamp);
  ++MsgCount;
  ByteCount += body_size;
}
