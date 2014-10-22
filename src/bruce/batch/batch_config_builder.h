/* <bruce/batch/batch_config_builder.h>

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

   Batching config builder class.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <base/no_copy_semantics.h>
#include <bruce/batch/batch_config.h>
#include <bruce/batch/combined_topics_batcher.h>
#include <bruce/batch/global_batch_config.h>
#include <bruce/batch/per_topic_batcher.h>
#include <bruce/conf/batch_conf.h>

namespace Bruce {

  namespace Batch {

    class TBatchConfigBuilder final {
      NO_COPY_SEMANTICS(TBatchConfigBuilder);

      public:
      TBatchConfigBuilder();

      /* A null value for 'config' indicates "skip combined topics batching and
         send immediately".  If 'config' is not null and
         BatchingIsEnabled(*config) evaluates to false, then this topic will
         use combined topics batching.  Return true on success, or false if
         config for topic has already been added. */
      bool AddTopic(const std::string &topic, const TBatchConfig *config);

      /* A null input value indicates "skip combined topics batching and send
         immediately".  If 'config' is not null and
         BatchingIsEnabled(*config) evaluates to false, then the default topic
         will use combined topics batching.  Return true on success, or false
         if default topic config has already been specified. */
      bool SetDefaultTopic(const TBatchConfig *config);

      /* Set config for combined topics batching.  A null input value indicates
         "broker-level batching is disabled".  Likewise, a non-null input value
         such that BatchingIsEnabled(*config) evaluates to false will cause
         combined topics batching to be disabled.  Return true if config was
         set, or false if config has already been specified. */
      bool SetBrokerConfig(const TBatchConfig *config);

      /* Set produce request data limit.  This is the maximum data (key +
         value) size to send in a single produce request. */
      bool SetProduceRequestDataLimit(size_t limit);

      /* This should be set to the same value as message.max.bytes in the Kafka
         broker config file.  It prevents Bruce from attempting to create a
         compressed message set large enough to cause the broker to return a
         MessageSizeTooLarge error. */
      bool SetMessageMaxBytes(size_t message_max_bytes);

      TGlobalBatchConfig Build();

      TGlobalBatchConfig BuildFromConf(const Conf::TBatchConf &conf);

      void Clear();

      private:
      std::unordered_map<std::string, TBatchConfig> PerTopicMap;

      bool DefaultTopicConfigSpecified;

      TBatchConfig DefaultTopicConfig;

      bool DefaultTopicSkipBrokerBatching;

      bool BrokerBatchConfigSpecified;

      TBatchConfig BrokerBatchConfig;

      std::unordered_set<std::string> BrokerBatchEnableTopics;

      std::unordered_set<std::string> BrokerBatchDisableTopics;

      std::unordered_set<std::string> PerTopicBatchingTopics;

      bool ProduceRequestDataLimitSpecified;

      size_t ProduceRequestDataLimit;

      bool MessageMaxBytesSpecified;

      size_t MessageMaxBytes;
    };  // TBatchConfigBuilder

  }  // Batch

}  // Bruce
