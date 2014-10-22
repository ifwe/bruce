/* <bruce/conf/conf.h>

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

   Class representing configuration obtained from Bruce's config file.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <string>
#include <vector>

#include <netinet/in.h>

#include <base/no_copy_semantics.h>
#include <bruce/conf/batch_conf.h>
#include <bruce/conf/compression_conf.h>
#include <bruce/conf/compression_type.h>
#include <bruce/conf/conf_error.h>
#include <bruce/conf/topic_rate_conf.h>
#include <bruce/util/host_and_port.h>

/* Third party XML parser stuff. */
namespace pugi {

  class xml_document;

  struct xml_parse_result;

  class xml_node;

  class xml_attribute;

}  // pugi

namespace Bruce {

  namespace Conf {

    class TConf {
      public:
      class TBuilder;

      using TBroker = Util::THostAndPort;

      TConf() = default;

      TConf(const TConf &) = default;

      TConf(TConf &&) = default;

      TConf &operator=(const TConf &) = default;

      TConf &operator=(TConf &&) = default;

      const TBatchConf &GetBatchConf() const {
        assert(this);
        return BatchConf;
      }

      const TCompressionConf &GetCompressionConf() const {
        assert(this);
        return CompressionConf;
      }

      const TTopicRateConf &GetTopicRateConf() const {
        assert(this);
        return TopicRateConf;
      }

      const std::vector<TBroker> &GetInitialBrokers() const {
        assert(this);
        return InitialBrokers;
      }

      private:
      TBatchConf BatchConf;

      TCompressionConf CompressionConf;

      TTopicRateConf TopicRateConf;

      std::vector<TBroker> InitialBrokers;
    };  // TConf

    class TConf::TBuilder {
      NO_COPY_SEMANTICS(TBuilder);

      public:
      /* Exception base class. */
      class TErrorBase : public TConfError {
        protected:
        explicit TErrorBase(std::string &&msg)
            : TConfError(std::move(msg)) {
        }
      };  // TErrorBase

      class TConfigFileOpenFailed final : public TErrorBase {
        public:
        explicit TConfigFileOpenFailed(const std::string &filename)
            : TErrorBase(CreateMsg(filename)) {
        }

        private:
        static std::string CreateMsg(const std::string &filename);
      };  // TConfigFileOpenFailed

      class TConfigFileReadFailed final : public TErrorBase {
        public:
        explicit TConfigFileReadFailed(const std::string &filename)
            : TErrorBase(CreateMsg(filename)) {
        }

        private:
        static std::string CreateMsg(const std::string &filename);
      };  // TConfigFileReadFailed

      class TConfigFileIsEmpty final : public TErrorBase {
        public:
        TConfigFileIsEmpty()
            : TErrorBase("Config file is empty") {
        }
      };  // TConfigFileIsEmpty

      class TConfigFileParseError final : public TErrorBase {
        public:
        explicit TConfigFileParseError(size_t line_num, const char *details)
            : TErrorBase(CreateMsg(line_num, details)) {
        }

        private:
        static std::string CreateMsg(size_t line_num, const char *details);
      };  // TConfigFileParseError

      class TXmlDocumentError final : public TErrorBase {
        public:
        explicit TXmlDocumentError(const char *msg)
            : TErrorBase(msg) {
        }

        explicit TXmlDocumentError(const std::string &msg)
            : TXmlDocumentError(msg.c_str()) {
        }
      };  // TXmlDocumentError

      TBuilder();

      ~TBuilder() noexcept;

      TConf Build(const char *config_filename);

      void Reset();

      private:
      enum { DEFAULT_BROKER_PORT = 9092 };

      static void ThrowOnUnexpectedContent(const pugi::xml_node &node);

      static void ThrowOnDuplicateElem(const pugi::xml_node &duplicate_elem);

      static void ThrowOnMissingElem(const pugi::xml_node &parent_elem,
          const char *missing_elem_name);

      static void ThrowOnUnexpectedAttr(const pugi::xml_node &elem,
          const pugi::xml_attribute &attr);

      static void ThrowOnDuplicateAttr(const pugi::xml_node &elem,
          const pugi::xml_attribute &attr);

      static void ThrowOnMissingAttr(const pugi::xml_node &elem,
          const char *missing_attr_name);

      static void ThrowOnBadAttrValue(const pugi::xml_node &elem,
          const pugi::xml_attribute &attr);

      static void ThrowOnDisableExpected(const pugi::xml_node &elem,
          const pugi::xml_attribute &attr);

      static void ThrowOnUnexpectedNonemptyConfig(const pugi::xml_node &elem,
          const char *attr_name);

      static void ThrowIfNotLeaf(const pugi::xml_node &elem);

      bool GetUnsigned(size_t &result, const pugi::xml_node &elem,
          const pugi::xml_attribute &attr, bool allow_k,
          const char *alternate_keyword);

      bool GetBool(const pugi::xml_node &elem,
          const pugi::xml_attribute &attr);

      bool ProcessSingleUnsignedValueElem(size_t &result,
          const pugi::xml_node &node, const char *attr_name, bool allow_k,
          const char *alternate_keyword);

      std::string GetBatchingConfigName(const pugi::xml_node &config_node);

      void ProcessSingleBatchingNamedConfig(const pugi::xml_node &config_node);

      void ProcessBatchingNamedConfigs(const pugi::xml_node &batching_elem);

      void ProcessBatchCombinedTopics(const pugi::xml_node &node);

      void ProcessTopicBatchConfig(const pugi::xml_node &topic_elem,
          bool is_default, TBatchConf::TTopicAction &action,
          std::string &config);

      void ProcessBatchSingleTopicConfig(const pugi::xml_node &node);

      void ProcessBatchTopicConfigs(const pugi::xml_node &topic_configs_elem);

      void ProcessBatchingElem(const pugi::xml_node &batching_elem);

      std::string ProcessCompressionTopicConfig(
          const pugi::xml_node &topic_elem, bool is_default);

      void ProcessCompressionSingleTopicConfig(const pugi::xml_node &node);

      void ProcessCompressionTopicConfigsElem(
          const pugi::xml_node &topic_configs_elem);

      void ProcessSingleCompressionNamedConfig(
          const pugi::xml_node &config_node);

      void ProcessCompressionNamedConfigs(
          const pugi::xml_node &compression_elem);

      void ProcessCompressionElem(const pugi::xml_node &compression_elem);

      void ProcessSingleTopicRateNamedConfig(
          const pugi::xml_node &config_node);

      void ProcessTopicRateNamedConfigs(const pugi::xml_node &topic_rate_elem);

      std::string ProcessTopicRateTopicConfig(const pugi::xml_node &topic_elem,
          bool is_default);

      void ProcessTopicRateSingleTopicConfig(const pugi::xml_node &node);

      void ProcessTopicRateTopicConfigsElem(
          const pugi::xml_node &topic_configs_elem);

      void ProcessTopicRateElem(const pugi::xml_node &topic_rate_elem);

      void ProcessInitialBrokersElem(
          const pugi::xml_node &initial_brokers_elem);

      void ProcessRootElem(const pugi::xml_node &root_elem);

      void ReadConfigFile(const char *config_filename,
          std::vector<char> &file_contents);

      void ThrowOnParseError(const std::vector<char> &file_contents,
          const pugi::xml_parse_result &result);

      void ParseXml(const std::vector<char> &file_contents);

      pugi::xml_document *XmlDoc;

      TConf BuildResult;

      TBatchConf::TBuilder BatchingConfBuilder;

      TCompressionConf::TBuilder CompressionConfBuilder;

      TTopicRateConf::TBuilder TopicRateConfBuilder;

      bool GotBatchingElem;

      bool GotCompressionElem;

      bool GotTopicRateElem;

      bool GotInitialBrokersElem;
    };  // TConf::TBuilder

  }  // Conf

}  // Bruce
