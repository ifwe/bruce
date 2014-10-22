/* <bruce/batch/batch_config_builder.cc>

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

   Implements <bruce/batch/batch_config_builder.h>.
 */

#include <bruce/batch/batch_config_builder.h>

#include <utility>

using namespace Bruce;
using namespace Bruce::Batch;
using namespace Bruce::Conf;

TBatchConfigBuilder::TBatchConfigBuilder()
    : DefaultTopicConfigSpecified(false),
      DefaultTopicSkipBrokerBatching(false),
      BrokerBatchConfigSpecified(false),
      ProduceRequestDataLimitSpecified(false),
      ProduceRequestDataLimit(0),
      MessageMaxBytesSpecified(false),
      MessageMaxBytes(0) {
}

bool TBatchConfigBuilder::AddTopic(const std::string &topic,
    const TBatchConfig *config) {
  assert(this);

  if (PerTopicMap.find(topic) != PerTopicMap.end()) {
    return false;
  }

  assert(BrokerBatchEnableTopics.find(topic) == BrokerBatchEnableTopics.end());
  assert(BrokerBatchDisableTopics.find(topic) ==
         BrokerBatchDisableTopics.end());
  TBatchConfig conf;

  if (config == nullptr) {
    BrokerBatchDisableTopics.insert(topic);
  } else {
    conf = *config;

    if (BatchingIsEnabled(conf)) {
      PerTopicBatchingTopics.insert(topic);
    } else {
      BrokerBatchEnableTopics.insert(topic);
    }
  }

  PerTopicMap.insert(std::make_pair(topic, conf));
  return true;
}

bool TBatchConfigBuilder::SetDefaultTopic(const TBatchConfig *config) {
  assert(this);

  if (DefaultTopicConfigSpecified) {
    return false;
  }

  DefaultTopicSkipBrokerBatching = (config == nullptr);
  DefaultTopicConfig = (config == nullptr) ? TBatchConfig() : *config;
  DefaultTopicConfigSpecified = true;
  return true;
}

bool TBatchConfigBuilder::SetBrokerConfig(const TBatchConfig *config) {
  assert(this);

  if (BrokerBatchConfigSpecified) {
    return false;
  }

  BrokerBatchConfig = (config == nullptr) ? TBatchConfig() : *config;
  BrokerBatchConfigSpecified = true;
  return true;
}

bool TBatchConfigBuilder::SetProduceRequestDataLimit(size_t limit) {
  assert(this);

  if (ProduceRequestDataLimitSpecified) {
    return false;
  }

  ProduceRequestDataLimit = limit;
  ProduceRequestDataLimitSpecified = true;
  return true;
}

bool TBatchConfigBuilder::SetMessageMaxBytes(size_t limit) {
  assert(this);

  if (MessageMaxBytesSpecified) {
    return false;
  }

  MessageMaxBytes = limit;
  MessageMaxBytesSpecified = true;
  return true;
}

TGlobalBatchConfig TBatchConfigBuilder::Build() {
  assert(this);
  std::shared_ptr<TPerTopicBatcher::TConfig> per_topic_config(
      new TPerTopicBatcher::TConfig(DefaultTopicConfig,
                                    std::move(PerTopicMap)));
  std::unordered_set<std::string> topic_filter;
  bool exclude_topic_filter = false;

  if (BatchingIsEnabled(BrokerBatchConfig)) {
    exclude_topic_filter = !DefaultTopicSkipBrokerBatching &&
                           !BatchingIsEnabled(DefaultTopicConfig);
    if (exclude_topic_filter) {
      topic_filter = std::move(PerTopicBatchingTopics);

      for (const std::string &topic : BrokerBatchDisableTopics) {
        topic_filter.insert(topic);
      }
    } else {
      topic_filter = std::move(BrokerBatchEnableTopics);
    }
  }

  TGlobalBatchConfig build_result(std::move(per_topic_config),
      TCombinedTopicsBatcher::TConfig(BrokerBatchConfig,
          std::shared_ptr<std::unordered_set<std::string>>(
              new std::unordered_set<std::string>(std::move(topic_filter))),
          exclude_topic_filter),
      ProduceRequestDataLimit, MessageMaxBytes);
  Clear();
  return std::move(build_result);
}

static TBatchConfig ToBatchConfig(const TBatchConf::TBatchValues &values) {
  size_t time_limit = values.OptTimeLimit.IsKnown() ? *values.OptTimeLimit : 0;
  size_t msg_count = values.OptMsgCount.IsKnown() ? *values.OptMsgCount : 0;
  size_t byte_count = values.OptByteCount.IsKnown() ? *values.OptByteCount : 0;
  return TBatchConfig(time_limit, msg_count, byte_count);
}

TGlobalBatchConfig TBatchConfigBuilder::BuildFromConf(const TBatchConf &conf) {
  assert(this);
  SetProduceRequestDataLimit(conf.GetProduceRequestDataLimit());
  SetMessageMaxBytes(conf.GetMessageMaxBytes());
  TBatchConfig config = ToBatchConfig(conf.GetCombinedTopicsConfig());
  SetBrokerConfig(conf.CombinedTopicsBatchingIsEnabled() ? &config : nullptr);
  const TBatchConfig *cp = nullptr;

  switch (conf.GetDefaultTopicAction()) {
    case TBatchConf::TTopicAction::PerTopic: {
      config = ToBatchConfig(conf.GetDefaultTopicConfig());
      cp = &config;
      break;
    }
    case TBatchConf::TTopicAction::CombinedTopics: {
      config = TBatchConfig();
      cp = &config;
      break;
    }
    case TBatchConf::TTopicAction::Disable: {
      break;
    }
  }

  SetDefaultTopic(cp);
  const TBatchConf::TTopicMap &m = conf.GetTopicConfigs();

  for (const std::pair<std::string, TBatchConf::TTopicConf> &item : m) {
    cp = nullptr;

    switch (item.second.Action) {
      case TBatchConf::TTopicAction::PerTopic: {
        config = ToBatchConfig(item.second.BatchValues);
        cp = &config;
        break;
      }
      case TBatchConf::TTopicAction::CombinedTopics: {
        config = TBatchConfig();
        cp = &config;
        break;
      }
      case TBatchConf::TTopicAction::Disable: {
        break;
      }
    }

    AddTopic(item.first, cp);
  }

  return Build();
}

void TBatchConfigBuilder::Clear() {
  assert(this);
  PerTopicMap.clear();
  DefaultTopicConfigSpecified = false;
  DefaultTopicConfig = TBatchConfig();
  DefaultTopicSkipBrokerBatching = false;
  BrokerBatchConfigSpecified = false;
  BrokerBatchConfig = TBatchConfig();
  BrokerBatchEnableTopics.clear();
  BrokerBatchDisableTopics.clear();
  ProduceRequestDataLimitSpecified = false;
  ProduceRequestDataLimit = 0;
  MessageMaxBytesSpecified = false;
  MessageMaxBytes = 0;
}
