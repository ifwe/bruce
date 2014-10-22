/* <bruce/conf/conf.cc>

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

   Implements <bruce/conf/conf.h>.
 */

#include <bruce/conf/conf.h>

#include <cctype>
#include <cstring>
#include <fstream>
#include <sstream>

#include <boost/lexical_cast.hpp>

#include <base/no_default_case.h>
#include <base/opt.h>
#include <bruce/util/misc_util.h>
#include <third_party/pugixml-1.2/src/pugixml.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Conf;
using namespace Bruce::Util;
using namespace pugi;

static const char *NodeTypeToString(xml_node_type type) {
  switch (type) {
    case node_null:
      break;
    case node_document:
      return "DOCUMENT";
    case node_element:
      return "ELEMENT";
    case node_pcdata:
      return "PCDATA";
    case node_cdata:
      return "CDATA";
    case node_comment:
      return "COMMENT";
    case node_pi:
      return "PROCESSING INSTRUCTION";
    case node_declaration:
      return "DECLARATION";
    case node_doctype:
      return "DOCTYPE";
    NO_DEFAULT_CASE;
  }

  return "NULL";
}

std::string TConf::TBuilder::TConfigFileOpenFailed::CreateMsg(
    const std::string &filename) {
  std::string msg("Failed to open config file [");
  msg += filename;
  msg += "]";
  return std::move(msg);
}

std::string TConf::TBuilder::TConfigFileReadFailed::CreateMsg(
    const std::string &filename) {
  std::string msg("Failed to read config file [");
  msg += filename;
  msg += "]";
  return std::move(msg);
}

std::string TConf::TBuilder::TConfigFileParseError::CreateMsg(size_t line_num,
    const char *details) {
  assert(details);
  std::string msg("Parse error on line ");
  msg += boost::lexical_cast<std::string>(line_num);
  msg += " of config file: [";
  msg += details;
  msg += "]";
  return std::move(msg);
}

TConf::TBuilder::TBuilder()
    : XmlDoc(nullptr),
      GotBatchingElem(false),
      GotCompressionElem(false),
      GotInitialBrokersElem(false) {
}

TConf::TBuilder::~TBuilder() noexcept {
  delete XmlDoc;
}

TConf TConf::TBuilder::Build(const char *config_filename) {
  assert(this);
  assert(config_filename);
  Reset();
  std::vector<char> file_contents;
  ReadConfigFile(config_filename, file_contents);
  ParseXml(file_contents);
  xml_node node = XmlDoc->first_child();

  if (!node) {
    throw TXmlDocumentError("Empty XML document");
  }

  if ((node.type() != node_declaration) || std::strcmp(node.name(), "xml")) {
    throw TXmlDocumentError("Missing XML declaration at start of document");
  }

  xml_attribute attr = node.attribute("encoding");

  if (!attr) {
    throw TXmlDocumentError("XML declaration is missing 'encoding' attribute");
  }

  if (!StringsMatchNoCase(attr.value(), "US-ASCII")) {
    throw TXmlDocumentError("XML document encoding must be US-ASCII");
  }

  node = node.next_sibling();

  if (!node || (node.type() != node_element)) {
    throw TXmlDocumentError("XML document missing root element");
  }

  if (std::strcmp(node.name(), "bruceConfig")) {
    throw TXmlDocumentError(
        std::string("Unknown XML document root element [") + node.name() +
        "]: [bruceConfig] expected");
  }

  if (node.next_sibling()) {
    throw TXmlDocumentError(
        "Unexpected XML content after root element [bruceConfig]");
  }

  ProcessRootElem(node);
  TConf result = std::move(BuildResult);
  Reset();
  return std::move(result);
}

void TConf::TBuilder::Reset() {
  assert(this);
  delete XmlDoc;
  XmlDoc = nullptr;
  BuildResult = TConf();
  BatchingConfBuilder.Reset();
  CompressionConfBuilder.Reset();
  GotBatchingElem = false;
  GotCompressionElem = false;
  GotTopicRateElem = false;
  GotInitialBrokersElem = false;
}

static void TraceNodeToRoot(const xml_node &node, std::string &result) {
  result.clear();

  if (!node) {
    return;
  }

  xml_node current = node;
  xml_node next;

  for (; ; ) {
    next = current.parent();

    if (!next) {
      break;
    }

    result += "[";
    result += current.name();
    result += "]";
    current = next;
  }
}

void TConf::TBuilder::ThrowOnUnexpectedContent(const xml_node &node) {
  std::string msg("Unexpected XML content: type [");
  msg += NodeTypeToString(node.type());
  msg += "] name [";
  msg += node.name();
  msg += "] value [";
  msg += node.value();
  msg += "]: ";
  std::string trace;
  TraceNodeToRoot(node.parent(), trace);
  msg += trace;
  throw TXmlDocumentError(msg);
}

void TConf::TBuilder::ThrowOnDuplicateElem(const xml_node &duplicate_elem) {
  std::string msg("Duplicate XML element: ");
  std::string trace;
  TraceNodeToRoot(duplicate_elem, trace);
  msg += trace;
  throw TXmlDocumentError(msg);
}

void TConf::TBuilder::ThrowOnMissingElem(const xml_node &parent_elem,
    const char *missing_elem_name) {
  std::string msg("Missing XML element [");
  msg += missing_elem_name;
  msg += "]: ";
  std::string trace;
  TraceNodeToRoot(parent_elem, trace);
  msg += trace;
  throw TXmlDocumentError(msg);
}

void TConf::TBuilder::ThrowOnUnexpectedAttr(const xml_node &elem,
    const xml_attribute &attr) {
  std::string msg("Unexpected XML attribute [");
  msg += attr.name();
  msg += "]: ";
  std::string trace;
  TraceNodeToRoot(elem, trace);
  msg += trace;
  throw TXmlDocumentError(msg);
}

void TConf::TBuilder::ThrowOnDuplicateAttr(const xml_node &elem,
    const xml_attribute &attr) {
  std::string msg("Duplicate XML attribute [");
  msg += attr.name();
  msg += "]: ";
  std::string trace;
  TraceNodeToRoot(elem, trace);
  msg += trace;
  throw TXmlDocumentError(msg);
}

void TConf::TBuilder::ThrowOnMissingAttr(const xml_node &elem,
    const char *missing_attr_name) {
  std::string msg("Missing XML attribute [");
  msg += missing_attr_name;
  msg += "]: ";
  std::string trace;
  TraceNodeToRoot(elem, trace);
  msg += trace;
  throw TXmlDocumentError(msg);
}

void TConf::TBuilder::ThrowOnBadAttrValue(const xml_node &elem,
    const xml_attribute &attr) {
  std::string msg("XML attribute [");
  msg += attr.name();
  msg += "] has invalid value [";
  msg += attr.value();
  msg += "]: ";
  std::string trace;
  TraceNodeToRoot(elem, trace);
  msg += trace;
  throw TXmlDocumentError(msg);
}

void TConf::TBuilder::ThrowOnDisableExpected(const xml_node &elem,
    const xml_attribute &attr) {
  std::string msg("XML attribute [");
  msg += attr.name();
  msg += "] has invalid 0 value: specify \"disable\" instead: ";
  std::string trace;
  TraceNodeToRoot(elem, trace);
  msg += trace;
  throw TXmlDocumentError(msg);
}

void TConf::TBuilder::ThrowOnUnexpectedNonemptyConfig(const xml_node &elem,
    const char *attr_name) {
  std::string msg("XML attribute [");
  msg += attr_name;
  msg += "] has unexpected nonempty value: either specify empty value or "
         "delete attribute: ";
  std::string trace;
  TraceNodeToRoot(elem, trace);
  msg += trace;
  throw TXmlDocumentError(msg);
}

void TConf::TBuilder::ThrowIfNotLeaf(const xml_node &elem) {
  if (elem.first_child()) {
    std::string msg("XML element should be a leaf: ");
    std::string trace;
    TraceNodeToRoot(elem, trace);
    msg += trace;
    throw TXmlDocumentError(msg);
  }
}

bool TConf::TBuilder::GetUnsigned(size_t &result, const xml_node &elem,
    const xml_attribute &attr, bool allow_k, const char *alternate_keyword) {
  assert(this);
  size_t num = 0;
  std::string value = attr.value();
  TrimWhitespace(value);

  if (alternate_keyword && StringsMatchNoCase(value, alternate_keyword)) {
    return false;
  }

  if (value.empty()) {
    ThrowOnBadAttrValue(elem, attr);
  }

  bool got_k = false;

  if (allow_k && (std::tolower(value[value.size() - 1]) == 'k')) {
    got_k = true;
    value.pop_back();
    TrimWhitespace(value);
  }

  try {
    num = boost::lexical_cast<size_t>(value);
  } catch (const boost::bad_lexical_cast &) {
    ThrowOnBadAttrValue(elem, attr);
  }

  size_t final_num = num;

  if (got_k) {
    final_num <<= 10;

    /* check for overflow */
    if ((final_num >> 10) != num) {
      ThrowOnBadAttrValue(elem, attr);
    }
  }

  if (alternate_keyword && !std::strcmp(alternate_keyword, "disable") &&
      (final_num == 0)) {
    ThrowOnDisableExpected(elem, attr);
  }

  result = final_num;
  return true;
}

bool TConf::TBuilder::GetBool(const xml_node &elem,
    const xml_attribute &attr) {
  assert(this);
  std::string value = attr.value();
  TrimWhitespace(value);

  if (StringsMatchNoCase(value, "true")) {
    return true;
  }

  if (!StringsMatchNoCase(value, "false")) {
    ThrowOnBadAttrValue(elem, attr);
  }

  return false;
}

bool TConf::TBuilder::ProcessSingleUnsignedValueElem(size_t &result,
    const xml_node &node, const char *attr_name, bool allow_k,
    const char *alternate_keyword) {
  assert(this);
  ThrowIfNotLeaf(node);
  bool got_value = false;
  bool got_alternate_keyword = false;

  for (xml_attribute attr = node.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (std::strcmp(attr.name(), attr_name)) {
      ThrowOnUnexpectedAttr(node, attr);
    }

    if (got_value) {
      ThrowOnDuplicateAttr(node, attr);
    }

    if (!GetUnsigned(result, node, attr, allow_k, alternate_keyword)) {
      got_alternate_keyword = true;
    }

    got_value = true;
  }

  if (!got_value) {
    ThrowOnMissingAttr(node, attr_name);
  }

  return !got_alternate_keyword;
}

std::string TConf::TBuilder::GetBatchingConfigName(
    const xml_node &config_node) {
  assert(this);
  bool got_config_name = false;
  std::string config_name;

  for (xml_attribute attr = config_node.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (std::strcmp(attr.name(), "name")) {
      ThrowOnUnexpectedAttr(config_node, attr);
    }

    if (got_config_name) {
      ThrowOnDuplicateAttr(config_node, attr);
    }

    config_name = attr.value();
    got_config_name = true;
  }

  if (!got_config_name) {
    ThrowOnMissingAttr(config_node, "name");
  }

  TrimWhitespace(config_name);
  return std::move(config_name);
}

void TConf::TBuilder::ProcessSingleBatchingNamedConfig(
    const xml_node &config_node) {
  assert(this);
  std::string config_name = GetBatchingConfigName(config_node);
  bool got_time_elem = false;
  bool got_messages_elem = false;
  bool got_bytes_elem = false;
  bool time_elem_enable = false;
  bool messages_elem_enable = false;
  bool bytes_elem_enable = false;
  size_t time_value = 0;
  size_t messages_value = 0;
  size_t bytes_value = 0;

  for (xml_node node = config_node.first_child();
       node;
       node = node.next_sibling()) {
    if (!std::strcmp(node.name(), "time")) {
      if (got_time_elem) {
        ThrowOnDuplicateElem(node);
      }

      time_elem_enable = ProcessSingleUnsignedValueElem(time_value, node,
          "value", false, "disable");
      got_time_elem = true;
    } else if (!std::strcmp(node.name(), "messages")) {
      if (got_messages_elem) {
        ThrowOnDuplicateElem(node);
      }

      messages_elem_enable = ProcessSingleUnsignedValueElem(messages_value,
          node, "value", true, "disable");
      got_messages_elem = true;
    } else if (!std::strcmp(node.name(), "bytes")) {
      if (got_bytes_elem) {
        ThrowOnDuplicateElem(node);
      }

      bytes_elem_enable = ProcessSingleUnsignedValueElem(bytes_value, node,
          "value", true, "disable");
      got_bytes_elem = true;
    } else {
      ThrowOnUnexpectedContent(node);
    }
  }

  if (!got_time_elem) {
    ThrowOnMissingElem(config_node, "time");
  }

  if (!got_messages_elem) {
    ThrowOnMissingElem(config_node, "messages");
  }

  if (!got_bytes_elem) {
    ThrowOnMissingElem(config_node, "bytes");
  }

  if (!time_elem_enable && !messages_elem_enable && !bytes_elem_enable) {
    std::string msg("Named batching config [");
    msg += config_name;
    msg += "] must not have a setting of [disable] for all values";
    throw TXmlDocumentError(msg);
  }

  TBatchConf::TBatchValues values;

  if (time_elem_enable) {
    values.OptTimeLimit.MakeKnown(time_value);
  }

  if (messages_elem_enable) {
    values.OptMsgCount.MakeKnown(messages_value);
  }

  if (bytes_elem_enable) {
    values.OptByteCount.MakeKnown(bytes_value);
  }

  BatchingConfBuilder.AddNamedConfig(config_name, values);
}

void TConf::TBuilder::ProcessBatchingNamedConfigs(
    const xml_node &batching_elem) {
  assert(this);

  for (xml_node node = batching_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (!std::strcmp(node.name(), "namedConfigs")) {
      for (xml_node node2 = node.next_sibling();
           node2;
           node2 = node2.next_sibling()) {
        if (!std::strcmp(node2.name(), "namedConfigs")) {
          ThrowOnDuplicateElem(node2);
        }
      }

      for (xml_node config_node = node.first_child();
           config_node;
           config_node = config_node.next_sibling()) {
        if (std::strcmp(config_node.name(), "config")) {
          ThrowOnUnexpectedContent(config_node);
        }

        ProcessSingleBatchingNamedConfig(config_node);
      }

      break;
    }
  }
}

void TConf::TBuilder::ProcessBatchCombinedTopics(const xml_node &node) {
  assert(this);
  ThrowIfNotLeaf(node);
  bool got_enable_attr = false;
  bool got_config_attr = false;
  bool enable = false;
  std::string config;

  for (xml_attribute attr = node.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (!std::strcmp(attr.name(), "enable")) {
      if (got_enable_attr) {
        ThrowOnDuplicateAttr(node, attr);
      }

      enable = GetBool(node, attr);
      got_enable_attr = true;
    } else if (!std::strcmp(attr.name(), "config")) {
      if (got_config_attr) {
        ThrowOnDuplicateAttr(node, attr);
      }

      config = attr.value();
      TrimWhitespace(config);
      got_config_attr = true;
    } else {
      ThrowOnUnexpectedAttr(node, attr);
    }
  }

  if (!got_enable_attr) {
    ThrowOnMissingAttr(node, "enable");
  }

  if (enable && !got_config_attr) {
    ThrowOnMissingAttr(node, "config");
  }

  BatchingConfBuilder.SetCombinedTopicsConfig(enable, &config);
}

void TConf::TBuilder::ProcessTopicBatchConfig(const xml_node &topic_elem,
    bool is_default, TBatchConf::TTopicAction &action, std::string &config) {
  assert(this);
  ThrowIfNotLeaf(topic_elem);
  bool got_action_attr = false;
  bool got_config_attr = false;
  std::string action_str;
  config.clear();

  for (xml_attribute attr = topic_elem.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (!std::strcmp(attr.name(), "action")) {
      if (got_action_attr) {
        ThrowOnDuplicateAttr(topic_elem, attr);
      }

      action_str = attr.value();
      TrimWhitespace(action_str);

      if (!TBatchConf::StringToTopicAction(action_str, action)) {
        ThrowOnBadAttrValue(topic_elem, attr);
      }

      got_action_attr = true;
    } else if (!std::strcmp(attr.name(), "config")) {
      if (got_config_attr) {
        ThrowOnDuplicateAttr(topic_elem, attr);
      }

      config = attr.value();
      TrimWhitespace(config);
      got_config_attr = true;
    } else if (is_default || std::strcmp(attr.name(), "name")) {
      ThrowOnUnexpectedAttr(topic_elem, attr);
    }
  }

  if (!got_action_attr) {
    ThrowOnMissingAttr(topic_elem, "action");
  }

  switch (action) {
    case TBatchConf::TTopicAction::PerTopic: {
      if (!got_config_attr) {
        ThrowOnMissingAttr(topic_elem, "config");
      }

      break;
    }
    case TBatchConf::TTopicAction::CombinedTopics: {
      if (got_config_attr && !config.empty()) {
        ThrowOnUnexpectedNonemptyConfig(topic_elem, "config");
      }

      break;
    }
    case TBatchConf::TTopicAction::Disable: {
      break;
    }
    NO_DEFAULT_CASE;
  }
}

void TConf::TBuilder::ProcessBatchSingleTopicConfig(const xml_node &node) {
  assert(this);
  std::string topic_name;
  bool got_name = false;

  for (xml_attribute attr = node.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (!std::strcmp(attr.name(), "name")) {
      if (got_name) {
        ThrowOnDuplicateAttr(node, attr);
      }

      topic_name = attr.value();
      got_name = true;
    }
  }

  TBatchConf::TTopicAction action = TBatchConf::TTopicAction::Disable;
  std::string config;
  ProcessTopicBatchConfig(node, false, action, config);
  BatchingConfBuilder.SetTopicConfig(topic_name, action, &config);
}

void TConf::TBuilder::ProcessBatchTopicConfigs(
    const xml_node &topic_configs_elem) {
  assert(this);

  for (xml_node node = topic_configs_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (std::strcmp(node.name(), "topic")) {
      ThrowOnUnexpectedContent(node);
    }

    ThrowIfNotLeaf(node);
    ProcessBatchSingleTopicConfig(node);
  }
}

void TConf::TBuilder::ProcessBatchingElem(const xml_node &batching_elem) {
  assert(this);
  ProcessBatchingNamedConfigs(batching_elem);
  std::string config;
  bool got_produce_request_data_limit = false;
  bool got_message_max_bytes = false;
  bool got_combined_topics = false;
  bool got_default_topic = false;
  bool got_topic_configs = false;

  for (xml_node node = batching_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (!std::strcmp(node.name(), "produceRequestDataLimit")) {
      if (got_produce_request_data_limit) {
        ThrowOnDuplicateElem(node);
      }

      size_t limit = 0;

      if (!ProcessSingleUnsignedValueElem(limit, node, "value", true,
          nullptr)) {
        limit = 0;
      }

      BatchingConfBuilder.SetProduceRequestDataLimit(limit);
      got_produce_request_data_limit = true;
    } else if (!std::strcmp(node.name(), "messageMaxBytes")) {
      if (got_message_max_bytes) {
        ThrowOnDuplicateElem(node);
      }

      size_t message_max_bytes = 0;

      if (!ProcessSingleUnsignedValueElem(message_max_bytes, node, "value",
                                          true, nullptr)) {
        message_max_bytes = 0;
      }

      BatchingConfBuilder.SetMessageMaxBytes(message_max_bytes);
      got_message_max_bytes = true;
    } else if (!std::strcmp(node.name(), "combinedTopics")) {
      if (got_combined_topics) {
        ThrowOnDuplicateElem(node);
      }

      ProcessBatchCombinedTopics(node);
      got_combined_topics = true;
    } else if (!std::strcmp(node.name(), "defaultTopic")) {
      if (got_default_topic) {
        ThrowOnDuplicateElem(node);
      }

      TBatchConf::TTopicAction action = TBatchConf::TTopicAction::Disable;
      ProcessTopicBatchConfig(node, true, action, config);
      BatchingConfBuilder.SetDefaultTopicConfig(action, &config);
      got_default_topic = true;
    } else if (!std::strcmp(node.name(), "topicConfigs")) {
      if (got_topic_configs) {
        ThrowOnDuplicateElem(node);
      }

      ProcessBatchTopicConfigs(node);
      got_topic_configs = true;
    } else if (std::strcmp(node.name(), "namedConfigs")) {
      ThrowOnUnexpectedContent(node);
    }
  }

  BuildResult.BatchConf = BatchingConfBuilder.Build();
}

std::string TConf::TBuilder::ProcessCompressionTopicConfig(
    const xml_node &topic_elem, bool is_default) {
  assert(this);
  ThrowIfNotLeaf(topic_elem);
  bool got_config_attr = false;
  std::string config;

  for (xml_attribute attr = topic_elem.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (!std::strcmp(attr.name(), "config")) {
      if (got_config_attr) {
        ThrowOnDuplicateAttr(topic_elem, attr);
      }

      config = attr.value();
      TrimWhitespace(config);
      got_config_attr = true;
    } else if (is_default || std::strcmp(attr.name(), "name")) {
      ThrowOnUnexpectedAttr(topic_elem, attr);
    }
  }

  if (!got_config_attr) {
    ThrowOnMissingAttr(topic_elem, "config");
  }

  return std::move(config);
}

void TConf::TBuilder::ProcessCompressionSingleTopicConfig(
    const xml_node &node) {
  assert(this);
  std::string topic_name;
  bool got_name = false;

  for (xml_attribute attr = node.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (!std::strcmp(attr.name(), "name")) {
      if (got_name) {
        ThrowOnDuplicateAttr(node, attr);
      }

      topic_name = attr.value();
      got_name = true;
    }
  }

  std::string config_name = ProcessCompressionTopicConfig(node, false);
  CompressionConfBuilder.SetTopicConfig(topic_name, config_name);
}

void TConf::TBuilder::ProcessCompressionTopicConfigsElem(
    const xml_node &topic_configs_elem) {
  assert(this);

  for (xml_node node = topic_configs_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (std::strcmp(node.name(), "topic")) {
      ThrowOnUnexpectedContent(node);
    }

    ThrowIfNotLeaf(node);
    ProcessCompressionSingleTopicConfig(node);
  }
}

void TConf::TBuilder::ProcessSingleCompressionNamedConfig(
    const xml_node &config_node) {
  assert(this);
  ThrowIfNotLeaf(config_node);
  bool got_name = false;
  bool got_type = false;
  bool got_min_size = false;
  std::string name;
  TCompressionType type = TCompressionType::None;
  size_t min_size = 0;

  for (xml_attribute attr = config_node.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (!std::strcmp(attr.name(), "name")) {
      if (got_name) {
        ThrowOnDuplicateAttr(config_node, attr);
      }

      name = attr.value();
      TrimWhitespace(name);

      if (name.empty()) {
        ThrowOnBadAttrValue(config_node, attr);
      }

      got_name = true;
    } else if (!std::strcmp(attr.name(), "type")) {
      if (got_type) {
        ThrowOnDuplicateAttr(config_node, attr);
      }

      std::string type_str = attr.value();
      TrimWhitespace(type_str);

      if (!TCompressionConf::StringToType(type_str, type)) {
        ThrowOnBadAttrValue(config_node, attr);
      }

      got_type = true;
    } else if (!std::strcmp(attr.name(), "minSize")) {
      if (got_min_size) {
        ThrowOnDuplicateAttr(config_node, attr);
      }

      GetUnsigned(min_size, config_node, attr, true, nullptr);
      got_min_size = true;
    } else {
      ThrowOnUnexpectedAttr(config_node, attr);
    }
  }

  if (!got_name) {
    ThrowOnMissingAttr(config_node, "name");
  }

  if (!got_type) {
    ThrowOnMissingAttr(config_node, "type");
  }

  if (!got_min_size && (type != TCompressionType::None)) {
    ThrowOnMissingAttr(config_node, "minSize");
  }

  CompressionConfBuilder.AddNamedConfig(name, type, min_size);
}

void TConf::TBuilder::ProcessCompressionNamedConfigs(
    const xml_node &compression_elem) {
  assert(this);
  bool found = false;

  for (xml_node node = compression_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (!std::strcmp(node.name(), "namedConfigs")) {
      found = true;

      for (xml_node node2 = node.next_sibling();
           node2;
           node2 = node2.next_sibling()) {
        if (!std::strcmp(node2.name(), "namedConfigs")) {
          ThrowOnDuplicateElem(node2);
        }
      }

      for (xml_node config_node = node.first_child();
           config_node;
           config_node = config_node.next_sibling()) {
        if (std::strcmp(config_node.name(), "config")) {
          ThrowOnUnexpectedContent(config_node);
        }

        ProcessSingleCompressionNamedConfig(config_node);
      }

      break;
    }
  }

  if (!found) {
    ThrowOnMissingElem(compression_elem, "namedConfigs");
  }
}

void TConf::TBuilder::ProcessCompressionElem(
    const xml_node &compression_elem) {
  assert(this);
  ProcessCompressionNamedConfigs(compression_elem);
  bool got_size_threshold_percent = false;
  bool got_default_topic = false;
  bool got_topic_configs = false;

  for (xml_node node = compression_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (!std::strcmp(node.name(), "sizeThresholdPercent")) {
      if (got_size_threshold_percent) {
        ThrowOnDuplicateElem(node);
      }

      size_t size_threshold_percent = 0;
      ProcessSingleUnsignedValueElem(size_threshold_percent, node, "value",
          false, nullptr);
      CompressionConfBuilder.SetSizeThresholdPercent(size_threshold_percent);
      got_size_threshold_percent = true;
    } else if (!std::strcmp(node.name(), "defaultTopic")) {
      if (got_default_topic) {
        ThrowOnDuplicateElem(node);
      }

      std::string config_name = ProcessCompressionTopicConfig(node, true);
      CompressionConfBuilder.SetDefaultTopicConfig(config_name);
      got_default_topic = true;
    } else if (!std::strcmp(node.name(), "topicConfigs")) {
      if (got_topic_configs) {
        ThrowOnDuplicateElem(node);
      }

      ProcessCompressionTopicConfigsElem(node);
      got_topic_configs = true;
    } else if (std::strcmp(node.name(), "namedConfigs")) {
      ThrowOnUnexpectedContent(node);
    }
  }

  BuildResult.CompressionConf = CompressionConfBuilder.Build();
}

void TConf::TBuilder::ProcessSingleTopicRateNamedConfig(
    const xml_node &config_node) {
  assert(this);
  ThrowIfNotLeaf(config_node);
  bool got_name = false;
  bool got_interval = false;
  bool got_max_count = false;
  std::string name;
  size_t interval = 1;
  TOpt<size_t> opt_max_count;

  for (xml_attribute attr = config_node.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (!std::strcmp(attr.name(), "name")) {
      if (got_name) {
        ThrowOnDuplicateAttr(config_node, attr);
      }

      name = attr.value();
      TrimWhitespace(name);

      if (name.empty()) {
        ThrowOnBadAttrValue(config_node, attr);
      }

      got_name = true;
    } else if (!std::strcmp(attr.name(), "interval")) {
      if (got_interval) {
        ThrowOnDuplicateAttr(config_node, attr);
      }

      GetUnsigned(interval, config_node, attr, false, nullptr);
      got_interval = true;
    } else if (!std::strcmp(attr.name(), "maxCount")) {
      if (got_max_count) {
        ThrowOnDuplicateAttr(config_node, attr);
      }

      size_t max_count = 0;

      /* A false return value from GetUnsigned() indicates that "unlimited" was
         specified. */
      if (GetUnsigned(max_count, config_node, attr, true, "unlimited")) {
        opt_max_count.MakeKnown(max_count);
      }

      got_max_count = true;
    } else {
      ThrowOnUnexpectedAttr(config_node, attr);
    }
  }

  if (!got_name) {
    ThrowOnMissingAttr(config_node, "name");
  }

  if (!got_interval) {
    ThrowOnMissingAttr(config_node, "interval");
  }

  if (!got_max_count) {
    ThrowOnMissingAttr(config_node, "maxCount");
  }

  if (opt_max_count.IsKnown()) {
    TopicRateConfBuilder.AddBoundedNamedConfig(name, interval, *opt_max_count);
  } else {
    TopicRateConfBuilder.AddUnlimitedNamedConfig(name);
  }
}

void TConf::TBuilder::ProcessTopicRateNamedConfigs(
    const xml_node &topic_rate_elem) {
  assert(this);
  bool found = false;

  for (xml_node node = topic_rate_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (!std::strcmp(node.name(), "namedConfigs")) {
      found = true;

      for (xml_node node2 = node.next_sibling();
           node2;
           node2 = node2.next_sibling()) {
        if (!std::strcmp(node2.name(), "namedConfigs")) {
          ThrowOnDuplicateElem(node2);
        }
      }

      for (xml_node config_node = node.first_child();
           config_node;
           config_node = config_node.next_sibling()) {
        if (std::strcmp(config_node.name(), "config")) {
          ThrowOnUnexpectedContent(config_node);
        }

        ProcessSingleTopicRateNamedConfig(config_node);
      }

      break;
    }
  }

  if (!found) {
    ThrowOnMissingElem(topic_rate_elem, "namedConfigs");
  }
}

std::string TConf::TBuilder::ProcessTopicRateTopicConfig(
    const xml_node &topic_elem, bool is_default) {
  assert(this);
  ThrowIfNotLeaf(topic_elem);
  bool got_config_attr = false;
  std::string config;

  for (xml_attribute attr = topic_elem.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (!std::strcmp(attr.name(), "config")) {
      if (got_config_attr) {
        ThrowOnDuplicateAttr(topic_elem, attr);
      }

      config = attr.value();
      TrimWhitespace(config);
      got_config_attr = true;
    } else if (is_default || std::strcmp(attr.name(), "name")) {
      ThrowOnUnexpectedAttr(topic_elem, attr);
    }
  }

  if (!got_config_attr) {
    ThrowOnMissingAttr(topic_elem, "config");
  }

  return std::move(config);
}

void TConf::TBuilder::ProcessTopicRateSingleTopicConfig(const xml_node &node) {
  assert(this);
  std::string topic_name;
  bool got_name = false;

  for (xml_attribute attr = node.first_attribute();
       attr;
       attr = attr.next_attribute()) {
    if (!std::strcmp(attr.name(), "name")) {
      if (got_name) {
        ThrowOnDuplicateAttr(node, attr);
      }

      topic_name = attr.value();
      got_name = true;
    }
  }

  std::string config_name = ProcessTopicRateTopicConfig(node, false);
  TopicRateConfBuilder.SetTopicConfig(topic_name, config_name);
}

void TConf::TBuilder::ProcessTopicRateTopicConfigsElem(
    const xml_node &topic_configs_elem) {
  assert(this);

  for (xml_node node = topic_configs_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (std::strcmp(node.name(), "topic")) {
      ThrowOnUnexpectedContent(node);
    }

    ThrowIfNotLeaf(node);
    ProcessTopicRateSingleTopicConfig(node);
  }
}

void TConf::TBuilder::ProcessTopicRateElem(const xml_node &topic_rate_elem) {
  assert(this);
  ProcessTopicRateNamedConfigs(topic_rate_elem);
  bool got_default_topic = false;
  bool got_topic_configs = false;

  for (xml_node node = topic_rate_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (!std::strcmp(node.name(), "defaultTopic")) {
      if (got_default_topic) {
        ThrowOnDuplicateElem(node);
      }

      std::string config_name = ProcessTopicRateTopicConfig(node, true);
      TopicRateConfBuilder.SetDefaultTopicConfig(config_name);
      got_default_topic = true;
    } else if (!std::strcmp(node.name(), "topicConfigs")) {
      if (got_topic_configs) {
        ThrowOnDuplicateElem(node);
      }

      ProcessTopicRateTopicConfigsElem(node);
      got_topic_configs = true;
    } else if (std::strcmp(node.name(), "namedConfigs")) {
      ThrowOnUnexpectedContent(node);
    }
  }

  BuildResult.TopicRateConf = TopicRateConfBuilder.Build();
}

void TConf::TBuilder::ProcessInitialBrokersElem(
    const xml_node &initial_brokers_elem) {
  assert(this);
  std::vector<TBroker> broker_vec;
  std::string value;

  for (xml_node node = initial_brokers_elem.first_child();
       node;
       node = node.next_sibling()) {
    if ((node.type() != node_element) ||
        std::strcmp(node.name(), "broker")) {
      ThrowOnUnexpectedContent(node);
    }

    ThrowIfNotLeaf(node);
    bool got_host = false;
    std::string host;
    bool got_port = false;
    in_port_t port = DEFAULT_BROKER_PORT;

    for (xml_attribute attr = node.first_attribute();
         attr;
         attr = attr.next_attribute()) {
      if (!strcmp(attr.name(), "host")) {
        if (got_host) {
          ThrowOnDuplicateAttr(node, attr);
        }

        host = attr.value();
        TrimWhitespace(host);

        if (host.empty()) {
          throw TXmlDocumentError("[broker] element of [initialBrokers] has "
              "empty [host] attribute");
        }

        got_host = true;
      } else if (!strcmp(attr.name(), "port")) {
        if (got_port) {
          ThrowOnDuplicateAttr(node, attr);
        }

        value = attr.value();
        TrimWhitespace(value);

        try {
          port = boost::lexical_cast<in_port_t>(value);
        } catch (const boost::bad_lexical_cast &) {
          ThrowOnBadAttrValue(node, attr);
        }

        if (port == 0) {
          ThrowOnBadAttrValue(node, attr);
        }

        got_port = true;
      } else {
        ThrowOnUnexpectedAttr(node, attr);
      }
    }

    if (!got_host) {
      ThrowOnMissingAttr(node, "host");
    }

    broker_vec.push_back(TBroker(std::move(host), port));
  }

  if (broker_vec.empty()) {
    throw TXmlDocumentError(
        "[initialBrokers] element contains no [broker] elements");
  }

  BuildResult.InitialBrokers = std::move(broker_vec);
}

void TConf::TBuilder::ProcessRootElem(const xml_node &root_elem) {
  assert(this);

  for (xml_node node = root_elem.first_child();
       node;
       node = node.next_sibling()) {
    if (node.type() != node_element) {
      ThrowOnUnexpectedContent(node);
    }

    if (!std::strcmp(node.name(), "batching")) {
      if (GotBatchingElem) {
        ThrowOnDuplicateElem(node);
      }

      ProcessBatchingElem(node);
      GotBatchingElem = true;
    } else if (!std::strcmp(node.name(), "compression")) {
      if (GotCompressionElem) {
        ThrowOnDuplicateElem(node);
      }

      ProcessCompressionElem(node);
      GotCompressionElem = true;
    } else if (!std::strcmp(node.name(), "topicRateLimiting")) {
      if (GotTopicRateElem) {
        ThrowOnDuplicateElem(node);
      }

      ProcessTopicRateElem(node);
      GotTopicRateElem = true;
    } else if (!std::strcmp(node.name(), "initialBrokers")) {
      if (GotInitialBrokersElem) {
        ThrowOnDuplicateElem(node);
      }

      ProcessInitialBrokersElem(node);
      GotInitialBrokersElem = true;
    } else {
      ThrowOnUnexpectedContent(node);
    }
  }

  if (!GotBatchingElem) {
    ThrowOnMissingElem(root_elem, "batching");
  }

  if (!GotCompressionElem) {
    ThrowOnMissingElem(root_elem, "compression");
  }

  if (!GotTopicRateElem) {
    /* If the config file has no <topicRateLimiting> element, create a default
       config that imposes no rate limit on any topic. */
    TopicRateConfBuilder.AddUnlimitedNamedConfig("unlimited");
    TopicRateConfBuilder.SetDefaultTopicConfig("unlimited");
    BuildResult.TopicRateConf = TopicRateConfBuilder.Build();
  }

  if (!GotInitialBrokersElem) {
    ThrowOnMissingElem(root_elem, "initialBrokers");
  }
}

void TConf::TBuilder::ReadConfigFile(const char *config_filename,
    std::vector<char> &file_contents) {
  assert(this);
  std::ifstream infile(config_filename,
                       std::ios_base::in | std::ios_base::binary);

  if (!infile.is_open()) {
    throw TConfigFileOpenFailed(config_filename);
  }

  infile.exceptions(std::ifstream::badbit);

  try {
    infile.seekg(0, infile.end);
    size_t size = infile.tellg();
    infile.seekg(0);
    file_contents.resize(size);
    infile.read(reinterpret_cast<char *>(&file_contents[0]),
                file_contents.size());
  } catch (const std::ifstream::failure &) {
    throw TConfigFileReadFailed(config_filename);
  }
}

void TConf::TBuilder::ThrowOnParseError(
    const std::vector<char> &file_contents,
    const xml_parse_result &result) {
  assert(this);
  std::string s(&file_contents[0], file_contents.size());
  std::istringstream in(s);
  std::string line;
  size_t line_num = 0;
  size_t byte_count = 0;

  while (std::getline(in, line)) {
    ++line_num;
    byte_count += line.size();

    if (byte_count >= static_cast<size_t>(result.offset)) {
      break;
    }
  }

  throw TConfigFileParseError(line_num, result.description());
}

void TConf::TBuilder::ParseXml(const std::vector<char> &file_contents) {
  assert(this);

  if (file_contents.empty()) {
    throw TConfigFileIsEmpty();
  }

  delete XmlDoc;
  XmlDoc = nullptr;
  XmlDoc = new xml_document;
  xml_parse_result result = XmlDoc->load_buffer(&file_contents[0],
      file_contents.size(), parse_declaration | parse_escapes);

  if (result.status != status_ok) {
    ThrowOnParseError(file_contents, result);
  }
}
