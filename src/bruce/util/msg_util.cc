/* <bruce/util/msg_util.cc>

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

   Implements <bruce/util/msg_util.h>.
 */

#include <bruce/util/msg_util.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

#include <boost/lexical_cast.hpp>

#include <bruce/util/time_util.h>
#include <capped/blob.h>

using namespace Bruce;
using namespace Bruce::Util;
using namespace Capped;

size_t Bruce::Util::GetDataSize(const std::list<TMsg::TPtr> &batch) {
  size_t total_size = 0;

  for (const TMsg::TPtr &msg_ptr : batch) {
    assert(msg_ptr);
    total_size += msg_ptr->GetKeyAndValue().Size();
  }

  return total_size;
}

void Bruce::Util::WriteKey(std::vector<uint8_t> &dst, size_t offset,
    const TMsg &msg) {
  size_t key_size = msg.GetKeySize();
  dst.resize(std::max(dst.size(), offset + key_size));

  /* Copy the key into the buffer. */
  if (key_size) {
    TReader reader(&msg.GetKeyAndValue());
    reader.Read(&dst[offset], key_size);
  }
}

/* This function becomes trivially simple when we get rid of the old output
   format. */
size_t Bruce::Util::WriteValue(std::vector<uint8_t> &dst, size_t offset,
    const TMsg &msg, bool add_timestamp, bool use_old_output_format) {
  const std::string &topic = msg.GetTopic();
  std::string timestamp;
  std::string topic_with_space;

  if (use_old_output_format) {
    topic_with_space = topic;
    topic_with_space += ' ';
  }

  if (use_old_output_format && add_timestamp) {
    timestamp = boost::lexical_cast<std::string>(msg.GetTimestamp());
    timestamp += ' ';
  }

  size_t value_size = msg.GetValueSize();
  size_t timestamp_size = timestamp.size();
  size_t topic_with_space_size = topic_with_space.size();
  size_t extended_value_size = timestamp_size + topic_with_space_size +
                               value_size;
  dst.resize(std::max(dst.size(), offset + extended_value_size));

  if (extended_value_size) {
    uint8_t *msg_timestamp = &dst[offset];
    uint8_t *msg_topic = msg_timestamp + timestamp_size;
    uint8_t *msg_value = msg_topic + topic_with_space_size;

    /* Copy the daemon's timestamp into the buffer. */
    std::memcpy(msg_timestamp, timestamp.data(), timestamp_size);

    /* Copy the topic into the buffer. */
    std::memcpy(msg_topic, topic_with_space.data(), topic_with_space_size);

    /* Copy the value into the buffer. */
    TReader reader(&msg.GetKeyAndValue());
    reader.Skip(msg.GetKeySize());
    reader.Read(msg_value, value_size);
  }

  return extended_value_size;
}

/* This function becomes trivially simple when we get rid of the old output
   format. */
void Bruce::Util::WriteValue(uint8_t *dst, const TMsg &msg, bool add_timestamp,
    bool use_old_output_format) {
  const std::string &topic = msg.GetTopic();
  std::string timestamp;
  std::string topic_with_space;

  if (use_old_output_format) {
    topic_with_space = topic;
    topic_with_space += ' ';
  }

  if (use_old_output_format && add_timestamp) {
    timestamp = boost::lexical_cast<std::string>(msg.GetTimestamp());
    timestamp += ' ';
  }

  size_t value_size = msg.GetValueSize();
  size_t timestamp_size = timestamp.size();
  size_t topic_with_space_size = topic_with_space.size();
  uint8_t *msg_timestamp = dst;
  uint8_t *msg_topic = msg_timestamp + timestamp_size;
  uint8_t *msg_value = msg_topic + topic_with_space_size;

  /* Copy the daemon's timestamp into the buffer. */
  std::memcpy(msg_timestamp, timestamp.data(), timestamp_size);

  /* Copy the topic into the buffer. */
  std::memcpy(msg_topic, topic_with_space.data(), topic_with_space_size);

  /* Copy the value into the buffer. */
  TReader reader(&msg.GetKeyAndValue());
  reader.Skip(msg.GetKeySize());
  reader.Read(msg_value, value_size);
}

/* This is a temporary hack.  It will go away when we get rid of the old
   output format. */
size_t Bruce::Util::ComputeValueSize(const TMsg &msg, bool add_timestamp,
    bool use_old_output_format) {
  size_t timestamp_size = 0;

  if (use_old_output_format && add_timestamp) {
    std::string timestamp;
    timestamp = boost::lexical_cast<std::string>(msg.GetTimestamp());
    timestamp += ' ';
    timestamp_size = timestamp.size();
  }

  size_t topic_with_space_size =
      use_old_output_format ? (msg.GetTopic().size() + 1) : 0;
  return timestamp_size + topic_with_space_size + msg.GetValueSize();
}
