/* <bruce/simple_client/simple_bruce_client.cc>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 Tagged

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

   Simple client program that sends messages to bruce daemon.
 */

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include <boost/lexical_cast.hpp>

#include <base/basename.h>
#include <base/no_default_case.h>
#include <base/time.h>
#include <bruce/input_dg/any_partition/v0/v0_input_dg_writer.h>
#include <bruce/input_dg/old_v0_input_dg_writer.h>
#include <bruce/input_dg/partition_key/v0/v0_input_dg_writer.h>
#include <bruce/test_util/unix_dg_socket_writer.h>
#include <bruce/util/arg_parse_error.h>
#include <bruce/util/field_access.h>
#include <bruce/util/time_util.h>
#include <bruce/version.h>
#include <tclap/CmdLine.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::InputDg;
using namespace Bruce::TestUtil;
using namespace Bruce::Util;
using namespace Socket;

struct TConfig {
  /* Throws TArgParseError on error parsing args. */
  TConfig(int argc, char *argv[]);

  std::string SocketPath;

  std::string Topic;

  std::string Body;

  uint32_t PartitionKey;

  bool UsePartitionKey;

  std::string Key;

  std::string Value;

  size_t Count;

  size_t Interval;

  bool Seq;

  size_t Pad;

  bool Bad;

  size_t Print;

  bool UseOldInputFormat;
};  // TConfig

static void ParseArgs(int argc, char *argv[], TConfig &config) {
  using namespace TCLAP;
  const std::string prog_name = Basename(argv[0]);
  std::vector<const char *> arg_vec(&argv[0], &argv[0] + argc);
  arg_vec[0] = prog_name.c_str();

  try {
    CmdLine cmd("Utility for sending messages to Bruce.", ' ', GetVersion());
    ValueArg<decltype(config.SocketPath)> arg_socket_path("", "socket_path",
        "Pathname of UNIX domain datagram socket for sending messages to "
        "Bruce.", true, config.SocketPath, "PATH");
    cmd.add(arg_socket_path);
    ValueArg<decltype(config.Topic)> arg_topic("", "topic", "Kafka topic.",
        true, config.Topic, "TOPIC");
    cmd.add(arg_topic);

    /* TODO: Eventually eliminate this, and only use key and value (see below).
     */
    ValueArg<decltype(config.Body)> arg_body("", "body", "Message body for "
        "old message format.", false, config.Body, "BODY");
    cmd.add(arg_body);

    ValueArg<decltype(config.PartitionKey)> arg_partition_key("",
        "partition_key", "Partition key.", false, config.PartitionKey,
        "PARTITION_KEY");
    cmd.add(arg_partition_key);
    ValueArg<decltype(config.Key)> arg_key("", "key", "Message key.", false,
        config.Key, "KEY");
    cmd.add(arg_key);
    ValueArg<decltype(config.Value)> arg_value("", "value", "Message value.",
        false, config.Value, "VALUE");
    cmd.add(arg_value);
    ValueArg<decltype(config.Count)> arg_count("", "count", "Number of "
        "messages to send.", false, config.Count, "COUNT");
    cmd.add(arg_count);
    ValueArg<decltype(config.Interval)> arg_interval("", "interval", "Message "
        "interval in microseconds.  A value of 0 means \"send messages as "
        "fast as possible\".", false, config.Interval, "INTERVAL");
    cmd.add(arg_interval);
    SwitchArg arg_seq("", "seq", "Prepend incrementing count to message "
        "value.", cmd, config.Seq);
    ValueArg<decltype(config.Pad)> arg_pad("", "pad", "Pad incrementing count "
        "with leading 0s to fill this many spaces.", false, config.Pad, "PAD");
    cmd.add(arg_pad);
    SwitchArg arg_bad("", "bad", "Send a malformed message.", cmd, config.Bad);
    ValueArg<decltype(config.Print)> arg_print("", "print", "If nonzero, "
        "print message number every nth message.", false, config.Print,
        "PRINT");
    cmd.add(arg_print);
    SwitchArg arg_use_old_input_format("", "use_old_input_format", "Send "
        "messages to bruce using legacy input format.", cmd,
        config.UseOldInputFormat);
    cmd.parse(argc, &arg_vec[0]);
    config.SocketPath = arg_socket_path.getValue();
    config.Topic = arg_topic.getValue();
    config.Body = arg_body.getValue();
    config.PartitionKey = arg_partition_key.getValue();
    config.UsePartitionKey = arg_partition_key.isSet();
    config.Key = arg_key.getValue();
    config.Value = arg_value.getValue();
    config.Count = arg_count.getValue();
    config.Interval = arg_interval.getValue();
    config.Seq = arg_seq.getValue();
    config.Pad = arg_pad.getValue();
    config.Bad = arg_bad.getValue();
    config.Print = arg_print.getValue();
    config.UseOldInputFormat = arg_use_old_input_format.getValue();
  } catch (const ArgException &x) {
    throw TArgParseError(x.error(), x.argId());
  }
}

TConfig::TConfig(int argc, char *argv[])
    : PartitionKey(0),
      UsePartitionKey(false),
      Count(1),
      Interval(0),
      Seq(false),
      Pad(0),
      Bad(false),
      Print(0),
      UseOldInputFormat(false) {
  ParseArgs(argc, argv, *this);
}

void CreateDgOldOldFormat(std::vector<uint8_t> &buf, const TConfig &cfg,
    size_t msg_count) {
  const uint8_t *topic_begin =
      reinterpret_cast<const uint8_t *>(cfg.Topic.data());
  const uint8_t *topic_end = topic_begin + cfg.Topic.size();
  buf.assign(topic_begin, topic_end);

  if (!cfg.Bad) {
    buf.push_back(' ');
  }

  if (cfg.Seq) {
    std::string seq = boost::lexical_cast<std::string>(msg_count);

    if (seq.size() < cfg.Pad) {
      std::string pad_str;
      pad_str.assign(cfg.Pad - seq.size(), '0');
      const uint8_t *pad_begin =
          reinterpret_cast<const uint8_t *>(pad_str.data());
      const uint8_t *pad_end =
          pad_begin + pad_str.size();
      buf.insert(buf.end(), pad_begin, pad_end);
    }

    const uint8_t *seq_begin = reinterpret_cast<const uint8_t *>(seq.data());
    const uint8_t *seq_end = seq_begin + seq.size();
    buf.insert(buf.end(), seq_begin, seq_end);

    if (!cfg.Bad) {
      buf.push_back(' ');
    }
  }

  const uint8_t *body_begin =
      reinterpret_cast<const uint8_t *>(cfg.Body.data());
  const uint8_t *body_end = body_begin + cfg.Body.size();
  buf.insert(buf.end(), body_begin, body_end);
}

void CreateDgOldFormat(std::vector<uint8_t> &buf, const TConfig &cfg,
    size_t msg_count) {
  const uint8_t *topic_begin =
      reinterpret_cast<const uint8_t *>(cfg.Topic.data());
  const uint8_t *topic_end = topic_begin + cfg.Topic.size();
  buf.clear();

  if (cfg.Seq) {
    std::vector<uint8_t> body;
    std::string seq = boost::lexical_cast<std::string>(msg_count);

    if (seq.size() < cfg.Pad) {
      std::string pad_str;
      pad_str.assign(cfg.Pad - seq.size(), '0');
      const uint8_t *pad_begin =
          reinterpret_cast<const uint8_t *>(pad_str.data());
      const uint8_t *pad_end =
          pad_begin + pad_str.size();
      body.insert(body.end(), pad_begin, pad_end);
    }

    const uint8_t *seq_begin = reinterpret_cast<const uint8_t *>(seq.data());
    const uint8_t *seq_end = seq_begin + seq.size();
    body.insert(body.end(), seq_begin, seq_end);
    body.push_back(' ');
    body.insert(body.end(), cfg.Body.begin(), cfg.Body.end());
    const uint8_t *body_begin = &body[0];
    const uint8_t *body_end = body_begin + body.size();
    TOldV0InputDgWriter().WriteDg(buf, GetEpochMilliseconds(), topic_begin,
        topic_end, body_begin, body_end);
  } else {
    const uint8_t *body_begin =
        reinterpret_cast<const uint8_t *>(cfg.Body.data());
    const uint8_t *body_end = body_begin + cfg.Body.size();
    TOldV0InputDgWriter().WriteDg(buf, GetEpochMilliseconds(), topic_begin,
        topic_end, body_begin, body_end);
  }

  if (cfg.Bad) {
    /* To make the message malformed, we change the size field to an incorrect
       value. */
    assert(buf.size() >= sizeof(int32_t));
    WriteInt32ToHeader(&buf[0], buf.size() - 1);
  }
}

void CreateDg(std::vector<uint8_t> &buf, const TConfig &cfg,
    size_t msg_count) {
  if (cfg.UseOldInputFormat) {
    CreateDgOldOldFormat(buf, cfg, msg_count);
    return;
  }

  if (!cfg.Body.empty()) {
    CreateDgOldFormat(buf, cfg, msg_count);
    return;
  }

  std::string value;

  if (cfg.Seq) {
    std::string seq = boost::lexical_cast<std::string>(msg_count);

    if (seq.size() < cfg.Pad) {
      value.assign(cfg.Pad - seq.size(), '0');
    }

    value += seq;
    value.push_back(' ');
  }

  value += cfg.Value;
  const char *topic_begin = cfg.Topic.data();
  const char *topic_end = topic_begin + cfg.Topic.size();
  const char *key_begin = cfg.Key.data();
  const char *key_end = key_begin + cfg.Key.size();
  const char *value_begin = value.data();
  const char *value_end = value_begin + value.size();
  uint64_t ts = GetEpochMilliseconds();

  if (cfg.UsePartitionKey) {
    PartitionKey::V0::TV0InputDgWriter().WriteDg(buf, ts, cfg.PartitionKey,
        topic_begin, topic_end, key_begin, key_end, value_begin, value_end);
  } else {
    AnyPartition::V0::TV0InputDgWriter().WriteDg(buf, ts, topic_begin,
        topic_end, key_begin, key_end, value_begin, value_end);
  }

  if (cfg.Bad) {
    /* To make the message malformed, we change the size field to an incorrect
       value. */
    assert(buf.size() >= sizeof(int32_t));
    WriteInt32ToHeader(&buf[0], buf.size() - 1);
  }
}

int simple_bruce_client_main(int argc, char **argv) {
  std::unique_ptr<TConfig> cfg;

  try {
    cfg.reset(new TConfig(argc, argv));
  } catch (const TArgParseError &x) {
    /* Error parsing command line arguments. */
    std::cerr << x.what() << std::endl;
    return EXIT_FAILURE;
  }

  if (cfg->UseOldInputFormat) {
    if (std::find(cfg->Topic.begin(), cfg->Topic.end(), ' ') !=
        cfg->Topic.end()) {
      std::cerr << "Topic must not contain space character." << std::endl;
      return EXIT_FAILURE;
    }
  } else {
    if (cfg->Topic.size() > TOldV0InputDgWriter::MAX_TOPIC_SIZE) {
      std::cerr << "Topic size can be at most "
          << TOldV0InputDgWriter::MAX_TOPIC_SIZE << " bytes" << std::endl;
      return EXIT_FAILURE;
    }

    if (cfg->Body.size() > TOldV0InputDgWriter::MAX_BODY_SIZE) {
      std::cerr << "Body size can be at most "
          << TOldV0InputDgWriter::MAX_BODY_SIZE << " bytes" << std::endl;
      return EXIT_FAILURE;
    }

    if (cfg->UsePartitionKey) {
      using namespace Bruce::InputDg::PartitionKey::V0;

      switch (TV0InputDgWriter::CheckDgSize(
          cfg->Topic.size(), cfg->Key.size(), cfg->Value.size())) {
        case TV0InputDgWriter::TDgSizeResult::Ok: {
          break;
        }
        case TV0InputDgWriter::TDgSizeResult::TopicTooLarge: {
          std::cerr << "Topic too large" << std::endl;
          return EXIT_FAILURE;
        }
        case TV0InputDgWriter::TDgSizeResult::MsgTooLarge: {
          std::cerr << "Message too large" << std::endl;
          return EXIT_FAILURE;
        }
        NO_DEFAULT_CASE;
      }
    } else {
      using namespace Bruce::InputDg::AnyPartition::V0;

      switch (TV0InputDgWriter::CheckDgSize(
          cfg->Topic.size(), cfg->Key.size(), cfg->Value.size())) {
        case TV0InputDgWriter::TDgSizeResult::Ok: {
          break;
        }
        case TV0InputDgWriter::TDgSizeResult::TopicTooLarge: {
          std::cerr << "Topic too large" << std::endl;
          return EXIT_FAILURE;
        }
        case TV0InputDgWriter::TDgSizeResult::MsgTooLarge: {
          std::cerr << "Message too large" << std::endl;
          return EXIT_FAILURE;
        }
        NO_DEFAULT_CASE;
      }
    }
  }

  TUnixDgSocketWriter writer(cfg->SocketPath.c_str());
  std::vector<uint8_t> dg_buf;
  const clockid_t CLOCK_TYPE = CLOCK_MONOTONIC_RAW;

  /* The constructor initializes this to the epoch.  On the first iteration the
     deadline will be in the past, so the sleep time will be 0. */
  TTime deadline;

  for (size_t i = 1; i <= cfg->Count; ++i) {
    CreateDg(dg_buf, *cfg, i);
    SleepMicroseconds(deadline.RemainingMicroseconds(CLOCK_TYPE));
    deadline.Now(CLOCK_TYPE);
    writer.WriteMsg(&dg_buf[0], dg_buf.size());
    deadline.AddMicroseconds(cfg->Interval);

    if (cfg->Print && ((i % cfg->Print) == 0)) {
        std::cout << i << " messages written" << std::endl;
    }
  }

  return EXIT_SUCCESS;
}

int main(int argc, char **argv) {
  int ret = EXIT_SUCCESS;

  try {
    ret = simple_bruce_client_main(argc, argv);
  } catch (const std::exception &ex) {
    std::cerr << "error: " << ex.what() << std::endl;
    ret = EXIT_FAILURE;
  } catch (...) {
    std::cerr << "error: unknown exception" << std::endl;
    ret = EXIT_FAILURE;
  }

  return ret;
}
