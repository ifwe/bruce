/* <bruce/msg_rate_limiter.cc>

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

   Implements <bruce/msg_rate_limiter.h>.
 */

#include <cassert>

#include <bruce/msg_rate_limiter.h>

using namespace Bruce;
using namespace Bruce::Conf;

bool TMsgRateLimiter::WouldExceedLimit(const std::string &topic,
    uint64_t timestamp) {
  assert(this);

  if (!IsEnabled) {
    /* Fast path for case where rate limiting is completely disabled. */
    return false;
  }

  TTopicState &state = GetTopicState(topic, timestamp);
  ++state.Count;
  return state.Enable && (state.Count > state.MaxCount);
}

bool TMsgRateLimiter::RateLimitingIsEnabled(const Conf::TTopicRateConf &conf) {
  if (conf.GetDefaultTopicConfig().MaxCount.IsKnown()) {
    return true;
  }

  const TTopicRateConf::TTopicMap &m = conf.GetTopicConfigs();

  for (const auto &elem : m) {
    if (elem.second.MaxCount.IsKnown()) {
      return true;
    }
  }

  return false;
}

TMsgRateLimiter::TTopicState &
TMsgRateLimiter::GetTopicState(const std::string &topic, uint64_t timestamp) {
  assert(this);
  auto iter = TopicStateMap.find(topic);

  if (iter != TopicStateMap.end()) {
    TTopicState &state = iter->second;

    if (timestamp >= (state.IntervalStart + state.Interval)) {
      size_t interval_delta =
          (timestamp - state.IntervalStart) / state.Interval;
      state.IntervalStart += (interval_delta * state.Interval);
      state.Count = 0;
    }

    return state;
  }

  const TTopicRateConf::TTopicMap &m = Conf.GetTopicConfigs();
  auto map_iter = m.find(topic);
  const TTopicRateConf::TConf &conf = (map_iter == m.end()) ?
      Conf.GetDefaultTopicConfig() : map_iter->second;
  TTopicState &state = TopicStateMap[topic];
  state.Enable = conf.MaxCount.IsKnown();

  if (state.Enable) {
    state.Interval = conf.Interval;
    state.MaxCount = *conf.MaxCount;
    state.IntervalStart = timestamp;
  }

  return state;
}
