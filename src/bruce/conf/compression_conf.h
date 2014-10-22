/* <bruce/conf/compression_conf.h>

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

   Class representing compression configuration obtained from Bruce's config
   file.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include <base/no_copy_semantics.h>
#include <bruce/conf/compression_type.h>
#include <bruce/conf/conf_error.h>

namespace Bruce {

  namespace Conf {

    class TCompressionConf {
      public:
      class TBuilder;

      struct TConf {
        TCompressionType Type;

        /* Minimum total size of uncompressed message bodies required for
           compression to be used. */
        size_t MinSize;

        TConf()
            : Type(TCompressionType::None),
              MinSize(0) {
        }

        TConf(TCompressionType type, size_t min_size)
            : Type(type),
              MinSize(min_size) {
        }

        TConf(const TConf &) = default;

        TConf &operator=(const TConf &) = default;
      };  // TConf

      using TTopicMap = std::unordered_map<std::string, TConf>;

      static bool StringToType(const char *s, TCompressionType &result);

      static bool StringToType(const std::string &s,
          TCompressionType &result) {
        return StringToType(s.c_str(), result);
      }

      TCompressionConf()
          : SizeThresholdPercent(100) {
      }

      TCompressionConf(const TCompressionConf &) = default;

      TCompressionConf(TCompressionConf &&) = default;

      TCompressionConf &operator=(const TCompressionConf &) = default;

      TCompressionConf &operator=(TCompressionConf &&) = default;

      size_t GetSizeThresholdPercent() const {
        assert(this);
        return SizeThresholdPercent;
      }

      const TConf &GetDefaultTopicConfig() const {
        assert(this);
        return DefaultTopicConfig;
      }

      const TTopicMap &GetTopicConfigs() const {
        assert(this);
        return TopicConfigs;
      }

      private:
      size_t SizeThresholdPercent;

      TConf DefaultTopicConfig;

      TTopicMap TopicConfigs;
    };  // TCompressionConf

    class TCompressionConf::TBuilder {
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

      class TDuplicateSizeThresholdPercent final : public TErrorBase {
        public:
        TDuplicateSizeThresholdPercent()
            : TErrorBase("Compression config contains duplicate "
                         "sizeThresholdPercent definition") {
        }
      };  // TDuplicateSizeThresholdPercent

      class TBadSizeThresholdPercent final : public TErrorBase {
        public:
        TBadSizeThresholdPercent()
            : TErrorBase("Compression config contains bad "
                         "sizeThresholdPercent value: must be <= 100") {
        }
      };  // TBadSizeThresholdPercent

      class TDuplicateDefaultTopicConfig final : public TErrorBase {
        public:
        TDuplicateDefaultTopicConfig()
            : TErrorBase("Compression config contains duplicate defaultTopic "
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

      class TMissingDefaultTopic final : public TErrorBase {
        public:
        TMissingDefaultTopic()
            : TErrorBase("Compression config is missing defaultTopic "
                         "definition") {
        }
      };  // TMissingDefaultTopic

      TBuilder()
          : GotSizeThresholdPercent(false),
            GotDefaultTopic(false) {
      }

      void Reset();

      void AddNamedConfig(const std::string &name, TCompressionType type,
          size_t min_size);

      void SetSizeThresholdPercent(size_t size_threshold_percent);

      void SetDefaultTopicConfig(const std::string &config_name);

      void SetTopicConfig(const std::string &topic,
          const std::string &config_name);

      TCompressionConf Build();

      private:
      std::unordered_map<std::string, TConf> NamedConfigs;

      TCompressionConf BuildResult;

      bool GotSizeThresholdPercent;

      bool GotDefaultTopic;
    };  // TCompressionConf::TBuilder

  }  // Conf

}  // Bruce
