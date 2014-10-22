/* <bruce/conf/topic_rate_conf.cc>

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

   Implements <bruce/conf/topic_rate_conf.h>.
 */

#include <bruce/conf/topic_rate_conf.h>

#include <utility>

#include <bruce/util/misc_util.h>

using namespace Bruce;
using namespace Bruce::Conf;
using namespace Bruce::Util;

std::string TTopicRateConf::TBuilder::TDuplicateNamedConfig::CreateMsg(
    const std::string &config_name) {
  std::string msg("Topic rate limiting config contains duplicate named "
                  "config: [");
  msg += config_name;
  msg += "]";
  return std::move(msg);
}

std::string TTopicRateConf::TBuilder::TUnknownDefaultTopicConfig::CreateMsg(
    const std::string &config_name) {
  std::string msg("Topic rate limiting config defaultTopic definition "
                  "references unknown named config: [");
  msg += config_name;
  msg += "]";
  return std::move(msg);
}

std::string TTopicRateConf::TBuilder::TDuplicateTopicConfig::CreateMsg(
    const std::string &topic) {
  std::string msg("Topic rate limiting config contains duplicate "
                  "specification for topic [");
  msg += topic;
  msg += "]";
  return std::move(msg);
}

std::string TTopicRateConf::TBuilder::TZeroRateLimitInterval::CreateMsg(
    const std::string &topic) {
  std::string msg("Topic rate limiting config contains interval of zero for "
                  "topic [");
  msg += topic;
  msg += "]";
  return std::move(msg);
}

std::string TTopicRateConf::TBuilder::TUnknownTopicConfig::CreateMsg(
    const std::string &topic, const std::string &config_name) {
  std::string msg("Topic rate limiting config for topic [");
  msg += topic;
  msg += "] references unknown named config: [";
  msg += config_name;
  msg += "]";
  return std::move(msg);
}

void TTopicRateConf::TBuilder::Reset() {
  assert(this);
  BuildResult = TTopicRateConf();
  GotDefaultTopic = false;
}

void TTopicRateConf::TBuilder::AddBoundedNamedConfig(const std::string &name,
    size_t interval, size_t max_count) {
  assert(this);

  if (interval == 0) {
    throw TZeroRateLimitInterval(name);
  }

  auto result =
      NamedConfigs.insert(std::make_pair(name, TConf(interval, max_count)));

  if (!result.second) {
    throw TDuplicateNamedConfig(name);
  }
}

void TTopicRateConf::TBuilder::AddUnlimitedNamedConfig(
    const std::string &name) {
  assert(this);
  auto result =
      NamedConfigs.insert(std::make_pair(name, TConf()));

  if (!result.second) {
    throw TDuplicateNamedConfig(name);
  }
}

void TTopicRateConf::TBuilder::SetDefaultTopicConfig(
    const std::string &config_name) {
  assert(this);

  if (GotDefaultTopic) {
    throw TDuplicateDefaultTopicConfig();
  }

  auto iter = NamedConfigs.find(config_name);

  if (iter == NamedConfigs.end()) {
    throw TUnknownDefaultTopicConfig(config_name);
  }

  BuildResult.DefaultTopicConfig = iter->second;
  GotDefaultTopic = true;
}

void TTopicRateConf::TBuilder::SetTopicConfig(const std::string &topic,
    const std::string &config_name) {
  assert(this);

  if (BuildResult.TopicConfigs.find(topic) != BuildResult.TopicConfigs.end()) {
    throw TDuplicateTopicConfig(topic);
  }

  auto iter = NamedConfigs.find(config_name);

  if (iter == NamedConfigs.end()) {
    throw TUnknownTopicConfig(topic, config_name);
  }

  BuildResult.TopicConfigs.insert(std::make_pair(topic, iter->second));
}

TTopicRateConf TTopicRateConf::TBuilder::Build() {
  assert(this);

  if (!GotDefaultTopic) {
    throw TMissingDefaultTopic();
  }

  NamedConfigs.clear();
  GotDefaultTopic = false;
  TTopicRateConf result = std::move(BuildResult);
  BuildResult = TTopicRateConf();
  return std::move(result);
}
