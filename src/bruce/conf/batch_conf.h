/* <bruce/conf/batch_conf.h>

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

   Class representing batching configuration obtained from Bruce's config file.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/conf/conf_error.h>

namespace Bruce {

  namespace Conf {

    class TBatchConf {
      public:
      class TBuilder;

      enum class TTopicAction {
        PerTopic,
        CombinedTopics,
        Disable
      };  // TTopicAction

      /* For each member 'm' below, m.IsUnknown() indicates that "disable" was
         specified in the config file. */
      struct TBatchValues {
        Base::TOpt<size_t> OptTimeLimit;

        Base::TOpt<size_t> OptMsgCount;

        Base::TOpt<size_t> OptByteCount;
      };  // TBatchValues

      struct TTopicConf {
        TTopicAction Action;

        TBatchValues BatchValues;

        TTopicConf()
            : Action(TTopicAction::Disable) {
        }

        TTopicConf(TTopicAction action,
            const TBatchValues &batch_values)
            : Action(action),
              BatchValues(batch_values) {
        }

        TTopicConf(const TTopicConf &) = default;

        TTopicConf &operator=(const TTopicConf &) = default;
      };  // TTopicConf

      using TTopicMap = std::unordered_map<std::string, TTopicConf>;

      static bool StringToTopicAction(const char *s, TTopicAction &result);

      static bool StringToTopicAction(const std::string &s,
          TTopicAction &result) {
        return StringToTopicAction(s.c_str(), result);
      }

      TBatchConf()
          : ProduceRequestDataLimit(0),
            MessageMaxBytes(0),
            CombinedTopicsBatchingEnabled(false),
            DefaultTopicAction(TTopicAction::Disable) {
      }

      TBatchConf(const TBatchConf &) = default;

      TBatchConf(TBatchConf &&) = default;

      TBatchConf &operator=(const TBatchConf &) = default;

      TBatchConf &operator=(TBatchConf &&) = default;

      size_t GetProduceRequestDataLimit() const {
        assert(this);
        return ProduceRequestDataLimit;
      }

      size_t GetMessageMaxBytes() const {
        assert(this);
        return MessageMaxBytes;
      }

      bool CombinedTopicsBatchingIsEnabled() const {
        assert(this);
        return CombinedTopicsBatchingEnabled;
      }

      const TBatchValues &GetCombinedTopicsConfig() const {
        assert(this);
        return CombinedTopicsConfig;
      }

      TTopicAction GetDefaultTopicAction() const {
        assert(this);
        return DefaultTopicAction;
      }

      const TBatchValues &GetDefaultTopicConfig() const {
        assert(this);
        return DefaultTopicConfig;
      }

      const TTopicMap &GetTopicConfigs() const {
        assert(this);
        return TopicConfigs;
      }

      private:
      size_t ProduceRequestDataLimit;

      size_t MessageMaxBytes;

      bool CombinedTopicsBatchingEnabled;

      TBatchValues CombinedTopicsConfig;

      TTopicAction DefaultTopicAction;

      TBatchValues DefaultTopicConfig;

      TTopicMap TopicConfigs;
    };  // TBatchConf

    class TBatchConf::TBuilder {
      NO_COPY_SEMANTICS(TBuilder);

      public:
      /* Exception base class. */
      class TErrorBase : public TConfError {
        protected:
        explicit TErrorBase(std::string &&msg)
            : TConfError(std::move(msg)) {
        }
      };  // TErrorBase

      class TDuplicateNamedConfig final : public TErrorBase {
        public:
        explicit TDuplicateNamedConfig(const std::string &config_name)
            : TErrorBase(CreateMsg(config_name)) {
        }

        private:
        static std::string CreateMsg(const std::string &config_name);
      };  // TDuplicateNamedConfig

      class TDuplicateProduceRequestDataLimit final : public TErrorBase {
        public:
        TDuplicateProduceRequestDataLimit()
            : TErrorBase("Batching config contains duplicate "
                         "produceRequestDataLimit definition") {
        }
      };  // TDuplicateProduceRequestDataLimit

      class TDuplicateMessageMaxBytes final : public TErrorBase {
        public:
        TDuplicateMessageMaxBytes()
            : TErrorBase("Batching config contains duplicate messageMaxBytes "
                         "definition") {
        }
      };  // TDuplicateMessageMaxBytes

      class TDuplicateCombinedTopicsConfig final : public TErrorBase {
        public:
        TDuplicateCombinedTopicsConfig()
            : TErrorBase("Batching config contains duplicate combinedTopics "
                         "definition") {
        }
      };  // TDuplicateCombinedTopicsConfig

      class TUnknownCombinedTopicsConfig final : public TErrorBase {
        public:
        explicit TUnknownCombinedTopicsConfig(const std::string &config_name)
            : TErrorBase(CreateMsg(config_name)) {
        }

        private:
        static std::string CreateMsg(const std::string &config_name);
      };  // TUnknownCombinedTopicsConfig

      class TDuplicateDefaultTopicConfig final : public TErrorBase {
        public:
        TDuplicateDefaultTopicConfig()
            : TErrorBase("Batching config contains duplicate defaultTopic "
                         "definition") {
        }
      };  // TDuplicateDefaultTopicConfig

      class TUnknownDefaultTopicConfig final : public TErrorBase {
        public:
        explicit TUnknownDefaultTopicConfig(const std::string &config_name)
            : TErrorBase(CreateMsg(config_name)) {
        }

        private:
        static std::string CreateMsg(const std::string &config_name);
      };  // TUnknownDefaultTopicConfig

      class TDuplicateTopicConfig final : public TErrorBase {
        public:
        explicit TDuplicateTopicConfig(const std::string &topic)
            : TErrorBase(CreateMsg(topic)) {
        }

        private:
        static std::string CreateMsg(const std::string &topic);
      };  // TDuplicateTopicConfig

      class TUnknownTopicConfig final : public TErrorBase {
        public:
        TUnknownTopicConfig(const std::string &topic,
            const std::string &config_name)
            : TErrorBase(CreateMsg(topic, config_name)) {
        }

        private:
        static std::string CreateMsg(const std::string &topic,
            const std::string &config_name);
      };  // TUnknownTopicConfig

      class TMissingProduceRequestDataLimit final : public TErrorBase {
        public:
        TMissingProduceRequestDataLimit()
            : TErrorBase("Batching config is missing produceRequestDataLimit "
                         "definition") {
        }
      };  // TMissingProduceRequestDataLimit

      class TMissingMessageMaxBytes final : public TErrorBase {
        public:
        TMissingMessageMaxBytes()
            : TErrorBase("Batching config is missing messageMaxBytes "
                         "definition") {
        }
      };  // TMissingMessageMaxBytes

      class TMissingCombinedTopics final : public TErrorBase {
        public:
        TMissingCombinedTopics()
            : TErrorBase("Batching config is missing combinedTopics "
                         "definition") {
        }
      };  // TMissingCombinedTopics

      class TMissingDefaultTopic final : public TErrorBase {
        public:
        TMissingDefaultTopic()
            : TErrorBase("Batching config is missing defaultTopic "
                         "definition") {
        }
      };  // TMissingDefaultTopic

      TBuilder()
          : GotProduceRequestDataLimit(false),
            GotMessageMaxBytes(false),
            GotCombinedTopics(false),
            GotDefaultTopic(false) {
      }

      void Reset();

      void AddNamedConfig(const std::string &name, const TBatchValues &values);

      /* A value of 0 for 'limit' means "disable batch combining". */
      void SetProduceRequestDataLimit(size_t limit);

      void SetMessageMaxBytes(size_t message_max_bytes);

      void SetCombinedTopicsConfig(bool enabled,
          const std::string *config_name);

      void SetDefaultTopicConfig(TTopicAction action,
          const std::string *config_name);

      void SetTopicConfig(const std::string &topic, TTopicAction action,
          const std::string *config_name);

      TBatchConf Build();

      private:
      std::unordered_map<std::string, TBatchValues> NamedConfigs;

      TBatchConf BuildResult;

      bool GotProduceRequestDataLimit;

      bool GotMessageMaxBytes;

      bool GotCombinedTopics;

      bool GotDefaultTopic;
    };  // TBatchConf::TBuilder

  }  // Conf

}  // Bruce
