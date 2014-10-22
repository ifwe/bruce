/* <bruce/mock_kafka_server/setup.cc>

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

   Implements <bruce/mock_kafka_server/setup.h>.
 */

#include <bruce/mock_kafka_server/setup.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>

#include <boost/lexical_cast.hpp>

#include <bruce/util/exceptions.h>

using namespace Bruce;
using namespace Bruce::MockKafkaServer;
using namespace Bruce::Util;

static std::string MakeErrorMsg(size_t line_num, const char *msg) {
  std::string blurb("Error on line ");
  blurb += boost::lexical_cast<std::string>(line_num);
  blurb += " of setup file: ";
  blurb += msg;
  return blurb;
}

TSetup::TFileFormatError::TFileFormatError(size_t line_num, const char *msg)
    : std::runtime_error(MakeErrorMsg(line_num, msg)),
      LineNum(line_num) {
}

TSetup::TSetup()
    : LineNum(0) {
}

void TSetup::Get(const std::string &setup_file_path, TInfo &out) {
  assert(this);
  Result.Clear();
  std::ifstream infile(setup_file_path);

  if (!infile.is_open()) {
    throw TFileOpenError(setup_file_path);
  }

  infile.exceptions(std::ifstream::badbit);

  try {
    FillResult(infile);
  } catch (const std::ifstream::failure &) {
    throw TFileReadError(setup_file_path);
  }

  std::swap(Result.BasePort, out.BasePort);
  Result.Ports.swap(out.Ports);
  Result.Topics.swap(out.Topics);
}

void TSetup::NextInterestingLine(std::istream &in) {
  assert(this);
  CurrentLineTokens.clear();
  std::string line;
  std::vector<std::string> split_result;

  while (std::getline(in, line)) {
    ++LineNum;
    std::stringstream buf(line);
    split_result.clear();
    std::copy(std::istream_iterator<std::string>(buf),
              std::istream_iterator<std::string>(),
              std::back_inserter(split_result));

    if (split_result.empty()) {
      continue;  // blank line
    }

    const std::string& first_token = split_result[0];
    assert(!first_token.empty());

    if (first_token[0] == '#') {
      continue;  // comment line
    }

    /* We found an interesting line. */
    CurrentLineTokens.swap(split_result);
    break;
  }
}

std::string TSetup::ErrorBlurb() {
  assert(this);
  std::ostringstream oss;
  oss << "Error on line " << LineNum << " of setup file: ";
  return oss.str();
}

void TSetup::GetPortsLine(std::istream &in) {
  assert(this);

  if (CurrentLineTokens.empty()) {
    throw TFileFormatError(LineNum, "\"ports\" line not found");
  }

  if (CurrentLineTokens[0] != "ports") {
    throw TFileFormatError(LineNum, "\"ports\" line expected");
  }

  if (CurrentLineTokens.size() != 3) {
    throw TFileFormatError(LineNum,
                           "\"ports\" should be followed by exactly 2 values");
  }

  try {
    Result.BasePort = boost::lexical_cast<in_port_t>(CurrentLineTokens[1]);
  } catch (const boost::bad_lexical_cast &x) {
    throw TFileFormatError(LineNum,
                           "invalid port specified in \"ports\" line");
  }

  size_t num_ports = 0;

  try {
    num_ports = boost::lexical_cast<size_t>(CurrentLineTokens[2]);
  } catch (const boost::bad_lexical_cast &x) {
    throw TFileFormatError(LineNum,
                           "invalid port count specified in \"ports\" line");
  }

  size_t limit = std::numeric_limits<in_port_t>::max() - Result.BasePort + 1;

  if (num_ports > limit) {
    throw TFileFormatError(LineNum, "too many ports specified");
  }

  if (num_ports == 0) {
    throw TFileFormatError(LineNum, "number of ports must be nonzero");
  }

  Result.Ports.resize(num_ports);
  NextInterestingLine(in);
}

static bool ParseDelayAndInterval(const std::string &s, size_t &delay,
    size_t &interval) {
  std::string::const_iterator iter = std::find(s.begin(), s.end(), ':');

  if (iter == s.end()) {
    return false;
  }

  std::string first_chunk(s.begin(), iter);
  std::string second_chunk(iter + 1, s.end());

  try {
    delay = boost::lexical_cast<size_t>(first_chunk);
  } catch (const boost::bad_lexical_cast &x) {
    return false;
  }

  try {
    interval = boost::lexical_cast<size_t>(second_chunk);
  } catch (const boost::bad_lexical_cast &x) {
    return false;
  }

  return true;
}

void TSetup::GetPortLines(std::istream &in) {
  assert(this);
  assert(!Result.Ports.empty());
  in_port_t min_port = Result.BasePort;
  in_port_t max_port = min_port + (Result.Ports.size() - 1);

  for (; !CurrentLineTokens.empty() && (CurrentLineTokens[0] == "port");
       NextInterestingLine(in)) {
    if (CurrentLineTokens.size() != 6) {
      throw TFileFormatError(LineNum,
          "\"ports\" should be followed by exactly 5 values");
    }

    in_port_t port = 0;

    try {
      port = boost::lexical_cast<in_port_t>(CurrentLineTokens[1]);
    } catch (const boost::bad_lexical_cast &x) {
      throw TFileFormatError(LineNum,
                             "invalid port specified in \"port\" line");
    }

    if ((port < min_port) || (port > max_port)) {
      throw TFileFormatError(LineNum,
          "port specified in \"port\" line is out of range");
    }

    if (CurrentLineTokens[2] != "read_delay") {
      throw TFileFormatError(LineNum,
          "third token of \"port\" line should be \"read_delay\"");
    }

    size_t read_delay_time = 0;
    size_t read_delay_interval = 0;

    if (!ParseDelayAndInterval(CurrentLineTokens[3], read_delay_time,
                               read_delay_interval)) {
      throw TFileFormatError(LineNum,
                             "\"port\" line has invalid read_delay info");
    }

    if (CurrentLineTokens[4] != "ack_delay") {
      throw TFileFormatError(LineNum,
          "fifth token of \"port\" line should be \"ack_delay\"");
    }

    size_t ack_delay_time = 0;
    size_t ack_delay_interval = 0;

    if (!ParseDelayAndInterval(CurrentLineTokens[5], ack_delay_time,
                               ack_delay_interval)) {
      throw TFileFormatError(LineNum,
                             "\"port\" line has invalid ack_delay info");
    }

    size_t port_index = port - min_port;
    TPort &port_info = Result.Ports[port_index];
    port_info.ReadDelay = read_delay_time;
    port_info.ReadDelayInterval = read_delay_interval;
    port_info.AckDelay = ack_delay_time;
    port_info.AckDelayInterval = ack_delay_interval;
  }
}

void TSetup::GetTopicLines(std::istream &in) {
  assert(this);

  for (; !CurrentLineTokens.empty() && (CurrentLineTokens[0] == "topic");
       NextInterestingLine(in)) {
    if (CurrentLineTokens.size() != 4) {
      throw TFileFormatError(LineNum,
          "\"topic\" should be followed by exactly 3 values");
    }

    size_t num_partitions = 0;

    try {
      num_partitions = boost::lexical_cast<size_t>(CurrentLineTokens[2]);
    } catch (const boost::bad_lexical_cast &x) {
      throw TFileFormatError(LineNum,
          "invalid partition count specified in \"port\" line");
    }

    size_t first_port_offset = 0;

    try {
      first_port_offset = boost::lexical_cast<size_t>(CurrentLineTokens[3]);
    } catch (const boost::bad_lexical_cast &x) {
      throw TFileFormatError(LineNum,
          "invalid first port offset specified in \"port\" line");
    }

    auto ret = Result.Topics.insert(
        std::make_pair(CurrentLineTokens[1], TTopic()));

    if (!ret.second) {
      throw TFileFormatError(LineNum, "duplicate topic");
    }

    TTopic &topic = ret.first->second;
    topic.Partitions.resize(num_partitions);
    topic.FirstPortOffset = first_port_offset;
  }

  if (Result.Topics.empty()) {
      throw TFileFormatError(LineNum, "\"topic\" line expected");
  }
}

void TSetup::GetPartitionErrorLines(std::istream &in) {
  assert(this);

  for (; !CurrentLineTokens.empty() &&
           (CurrentLineTokens[0] == "partition_error");
       NextInterestingLine(in)) {
    if (CurrentLineTokens.size() != 5) {
      throw TFileFormatError(LineNum,
          "\"partition_error\" should be followed by exactly 4 values");
    }

    auto iter = Result.Topics.find(CurrentLineTokens[1]);

    if (iter == Result.Topics.end()) {
      throw TFileFormatError(LineNum,
          "\"partition_error\" line specifies unknown topic");
    }

    size_t partition = 0;

    try {
      partition = boost::lexical_cast<size_t>(CurrentLineTokens[2]);
    } catch (const boost::bad_lexical_cast &x) {
      throw TFileFormatError(LineNum,
          "invalid partition specified in \"partition_error\" line");
    }

    TTopic &topic = iter->second;

    if (partition >= topic.Partitions.size()) {
      throw TFileFormatError(LineNum,
          "nonexistent partition specified in \"partition_error\" line");
    }

    TPartition &partition_info = topic.Partitions[partition];
    int16_t ack_error = 0;

    try {
      ack_error = boost::lexical_cast<int16_t>(CurrentLineTokens[3]);
    } catch (const boost::bad_lexical_cast &x) {
      throw TFileFormatError(LineNum,
          "invalid error code specified in \"partition_error\" line");
    }

    size_t interval = 0;

    try {
      interval = boost::lexical_cast<size_t>(CurrentLineTokens[4]);
    } catch (const boost::bad_lexical_cast &x) {
      throw TFileFormatError(LineNum,
          "invalid interval specified in \"partition_error\" line");
    }

    partition_info.AckError = ack_error;
    partition_info.AckErrorInterval = interval;
  }
}

void TSetup::FillResult(std::istream &in) {
  assert(this);
  LineNum = 0;
  NextInterestingLine(in);
  GetPortsLine(in);
  GetPortLines(in);
  GetTopicLines(in);
  GetPartitionErrorLines(in);
}
