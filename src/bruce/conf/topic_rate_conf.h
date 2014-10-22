/* <bruce/conf/topic_rate_conf.h>

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

   Class representing per-topic message rate limiting configuration obtained
   from Bruce's config file.
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

    class TTopicRateConf {
      public:
      class TBuilder;

      struct TConf {
        /* This number must be > 0.  It specifies a time interval length in
           milliseconds for rate limit enforcement. */
        size_t Interval;

        /* Optional maximum # of allowed messages for a given topic within
           'Interval' above.  Messages that would cause the maximum to be
           exceeded are discarded.  If the optional value is in the unknown
           state then this indicates no maximum (i.e. infinite limit). */
        Base::TOpt<size_t> MaxCount;

        /* Default constructor specifies no limit. */
        TConf()
            : Interval(1) {
        }

        TConf(size_t interval, size_t max_count)
            : Interval(interval),
              MaxCount(max_count) {
        }

        TConf(const TConf &) = default;

        TConf &operator=(const TConf &) = default;
      };  // TConf

      using TTopicMap = std::unordered_map<std::string, TConf>;

      TTopicRateConf() = default;

      TTopicRateConf(const TTopicRateConf &) = default;

      TTopicRateConf(TTopicRateConf &&) = default;

      TTopicRateConf &operator=(const TTopicRateConf &) = default;

      TTopicRateConf &operator=(TTopicRateConf &&) = default;

      const TConf &GetDefaultTopicConfig() const {
        assert(this);
        return DefaultTopicConfig;
      }

      const TTopicMap &GetTopicConfigs() const {
        assert(this);
        return TopicConfigs;
      }

      private:
      TConf DefaultTopicConfig;

      TTopicMap TopicConfigs;
    };  // TTopicRateConf

    class TTopicRateConf::TBuilder {
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

      class TZeroRateLimitInterval final : public TErrorBase {
        public:
        explicit TZeroRateLimitInterval(const std::string &config_name)
            : TErrorBase(CreateMsg(config_name)) {
        }

        private:
        static std::string CreateMsg(const std::string &config_name);
      };  // TZeroRateLimitInterval

      class TDuplicateDefaultTopicConfig final : public TErrorBase {
        public:
        TDuplicateDefaultTopicConfig()
            : TErrorBase("Topic rate limiting config contains duplicate "
                         "defaultTopic definition") {
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

      class TMissingDefaultTopic final : public TErrorBase {
        public:
        TMissingDefaultTopic()
            : TErrorBase("Topic rate limiting config is missing defaultTopic "
                         "definition") {
        }
      };  // TMissingDefaultTopic

      TBuilder()
          : GotDefaultTopic(false) {
      }

      void Reset();

      /* Add a named config with a finite maximum count. */
      void AddBoundedNamedConfig(const std::string &name, size_t interval,
          size_t max_count);

      /* Add a named config with an unlimited maximum count. */
      void AddUnlimitedNamedConfig(const std::string &name);

      void SetDefaultTopicConfig(const std::string &config_name);

      void SetTopicConfig(const std::string &topic,
          const std::string &config_name);

      TTopicRateConf Build();

      private:
      std::unordered_map<std::string, TConf> NamedConfigs;

      TTopicRateConf BuildResult;

      bool GotDefaultTopic;
    };  // TTopicRateConf::TBuilder

  }  // Conf

}  // Bruce
