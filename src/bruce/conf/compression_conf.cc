/* <bruce/conf/compression_conf.cc>

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

   Implements <bruce/conf/compression_conf.h>.
 */

#include <bruce/conf/compression_conf.h>

#include <utility>

#include <bruce/util/misc_util.h>

using namespace Bruce;
using namespace Bruce::Conf;
using namespace Bruce::Util;

bool TCompressionConf::StringToType(const char *s, TCompressionType &result) {
  assert(s);

  if (StringsMatchNoCase(s, "none")) {
    result = TCompressionType::None;
    return true;
  }

  if (StringsMatchNoCase(s, "snappy")) {
    result = TCompressionType::Snappy;
    return true;
  }

  return false;
}

std::string TCompressionConf::TBuilder::TDuplicateNamedConfig::CreateMsg(
    const std::string &config_name) {
  std::string msg("Compression config contains duplicate named config: [");
  msg += config_name;
  msg += "]";
  return std::move(msg);
}

std::string TCompressionConf::TBuilder::TUnknownDefaultTopicConfig::CreateMsg(
    const std::string &config_name) {
  std::string msg("Compression config defaultTopic definition references "
                  "unknown named config: [");
  msg += config_name;
  msg += "]";
  return std::move(msg);
}

std::string TCompressionConf::TBuilder::TDuplicateTopicConfig::CreateMsg(
    const std::string &topic) {
  std::string msg("Compression config contains duplicate specification for "
                  "topic [");
  msg += topic;
  msg += "]";
  return std::move(msg);
}

std::string TCompressionConf::TBuilder::TUnknownTopicConfig::CreateMsg(
    const std::string &topic, const std::string &config_name) {
  std::string msg("Compression config for topic [");
  msg += topic;
  msg += "] references unknown named config: [";
  msg += config_name;
  msg += "]";
  return std::move(msg);
}

void TCompressionConf::TBuilder::Reset() {
  assert(this);
  BuildResult = TCompressionConf();
  GotDefaultTopic = false;
}

void TCompressionConf::TBuilder::AddNamedConfig(const std::string &name,
    TCompressionType type, size_t min_size) {
  assert(this);

  if (type == TCompressionType::None) {
    min_size = 0;
  }

  auto result =
      NamedConfigs.insert(std::make_pair(name, TConf(type, min_size)));

  if (!result.second) {
    throw TDuplicateNamedConfig(name);
  }
}

void TCompressionConf::TBuilder::SetSizeThresholdPercent(
    size_t size_threshold_percent) {
  assert(this);

  if (GotSizeThresholdPercent) {
    throw TDuplicateSizeThresholdPercent();
  }

  if (size_threshold_percent > 100) {
    throw TBadSizeThresholdPercent();
  }

  BuildResult.SizeThresholdPercent = size_threshold_percent;
  GotSizeThresholdPercent = true;
}

void TCompressionConf::TBuilder::SetDefaultTopicConfig(
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

void TCompressionConf::TBuilder::SetTopicConfig(const std::string &topic,
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

TCompressionConf TCompressionConf::TBuilder::Build() {
  assert(this);

  if (!GotDefaultTopic) {
    throw TMissingDefaultTopic();
  }

  NamedConfigs.clear();
  GotSizeThresholdPercent = false;
  GotDefaultTopic = false;
  TCompressionConf result = std::move(BuildResult);
  BuildResult = TCompressionConf();
  return std::move(result);
}
