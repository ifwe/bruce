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

   Bruce client library implementation.
 */

#include <bruce_client/bruce_client.h>

#include <cassert>
#include <cerrno>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <system_error>

#include <base/no_default_case.h>
#include <bruce/input_dg/any_partition/v0/v0_input_dg_writer.h>
#include <bruce/input_dg/partition_key/v0/v0_input_dg_writer.h>
#include <bruce/test_util/unix_dg_socket_writer.h>

using namespace Bruce;
using namespace Bruce::InputDg;
using namespace Bruce::TestUtil;

int FindAnyPartitionMsgSize(size_t topic_size, size_t key_size,
    size_t value_size, size_t &out_size) noexcept {
  using namespace Bruce::InputDg::AnyPartition::V0;
  out_size = 0;

  if (topic_size > std::numeric_limits<int8_t>::max()) {
    return BRUCE_TOPIC_TOO_LARGE;
  }

  const size_t max_int32_value =
      static_cast<size_t>(std::numeric_limits<int32_t>::max());

  if ((key_size > max_int32_value) || (value_size > max_int32_value)) {
    return BRUCE_MSG_TOO_LARGE;
  }

  switch (TV0InputDgWriter::ComputeDgSize(out_size, topic_size, key_size,
      value_size)) {
    case TV0InputDgWriter::TDgSizeResult::Ok: {
      break;
    }
    case TV0InputDgWriter::TDgSizeResult::TopicTooLarge: {
      return BRUCE_TOPIC_TOO_LARGE;
    }
    case TV0InputDgWriter::TDgSizeResult::MsgTooLarge: {
      return BRUCE_MSG_TOO_LARGE;
    }
    NO_DEFAULT_CASE;
  }

  return BRUCE_OK;
}

int FindPartitionKeyMsgSize(size_t topic_size, size_t key_size,
    size_t value_size, size_t &out_size) noexcept {
  using namespace Bruce::InputDg::PartitionKey::V0;
  out_size = 0;

  if (topic_size > std::numeric_limits<int8_t>::max()) {
    return BRUCE_TOPIC_TOO_LARGE;
  }

  const size_t max_int32_value =
      static_cast<size_t>(std::numeric_limits<int32_t>::max());

  if ((key_size > max_int32_value) || (value_size > max_int32_value)) {
    return BRUCE_MSG_TOO_LARGE;
  }

  switch (TV0InputDgWriter::ComputeDgSize(out_size, topic_size, key_size,
      value_size)) {
    case TV0InputDgWriter::TDgSizeResult::Ok: {
      break;
    }
    case TV0InputDgWriter::TDgSizeResult::TopicTooLarge: {
      return BRUCE_TOPIC_TOO_LARGE;
    }
    case TV0InputDgWriter::TDgSizeResult::MsgTooLarge: {
      return BRUCE_MSG_TOO_LARGE;
    }
    NO_DEFAULT_CASE;
  }

  return BRUCE_OK;
}

extern "C"
int bruce_find_any_partition_msg_size(size_t topic_size, size_t key_size,
    size_t value_size, size_t *out_size) {
  if (out_size == nullptr) {
    return BRUCE_INVALID_INPUT;
  }

  return FindAnyPartitionMsgSize(topic_size, key_size, value_size, *out_size);
}

extern "C"
int bruce_find_partition_key_msg_size(size_t topic_size, size_t key_size,
    size_t value_size, size_t *out_size) {
  if (out_size == nullptr) {
    return BRUCE_INVALID_INPUT;
  }

  return FindPartitionKeyMsgSize(topic_size, key_size, value_size, *out_size);
}

extern "C"
int bruce_write_any_partition_msg(void *out_buf, size_t out_buf_size,
    const char *topic, int64_t timestamp, const void *key, size_t key_size,
    const void *value, size_t value_size) {
  using namespace Bruce::InputDg::AnyPartition::V0;

  if ((out_buf == nullptr) || (topic == nullptr) || (key == nullptr) ||
      (value == nullptr)) {
    return BRUCE_INVALID_INPUT;
  }

  size_t topic_len = std::strlen(topic);
  size_t dg_size = 0;
  int ret = FindAnyPartitionMsgSize(topic_len, key_size, value_size, dg_size);

  if (ret != BRUCE_OK) {
    return ret;
  }

  if (out_buf_size < dg_size) {
    return BRUCE_BUF_TOO_SMALL;
  }

  TV0InputDgWriter().WriteDg(out_buf, timestamp, topic, topic + topic_len, key,
      reinterpret_cast<const uint8_t *>(key) + key_size, value,
      reinterpret_cast<const uint8_t *>(value) + value_size);
  return BRUCE_OK;
}

extern "C"
int bruce_write_partition_key_msg(void *out_buf, size_t out_buf_size,
    int32_t partition_key, const char *topic, int64_t timestamp,
    const void *key, size_t key_size, const void *value, size_t value_size) {
  using namespace Bruce::InputDg::PartitionKey::V0;

  if ((out_buf == nullptr) || (topic == nullptr) || (key == nullptr) ||
      (value == nullptr)) {
    return BRUCE_INVALID_INPUT;
  }

  size_t topic_len = std::strlen(topic);
  size_t dg_size = 0;
  int ret = FindPartitionKeyMsgSize(topic_len, key_size, value_size, dg_size);

  if (ret != BRUCE_OK) {
    return ret;
  }

  if (out_buf_size < dg_size) {
    return BRUCE_BUF_TOO_SMALL;
  }

  TV0InputDgWriter().WriteDg(out_buf, timestamp, partition_key, topic,
      topic + topic_len, key,
      reinterpret_cast<const uint8_t *>(key) + key_size, value,
      reinterpret_cast<const uint8_t *>(value) + value_size);
  return BRUCE_OK;
}

struct bruce_dg_socket_writer {
  TUnixDgSocketWriter *writer;
};  // bruce_dg_socket_writer

extern "C"
int bruce_create_dg_socket_writer(const char *socket_path,
    struct bruce_dg_socket_writer **out_sw) {
  if ((socket_path == nullptr) || (out_sw == nullptr)) {
    return BRUCE_INVALID_INPUT;
  }

  *out_sw = nullptr;

  try {
    std::unique_ptr<TUnixDgSocketWriter> w(
        new TUnixDgSocketWriter(socket_path));
    bruce_dg_socket_writer *sw = new bruce_dg_socket_writer;
    sw->writer = w.release();
    *out_sw = sw;
  } catch (const std::bad_alloc &) {
    return ENOMEM;
  } catch (const std::system_error &x) {
    return x.code().value();
  } catch (const std::exception &) {
    return BRUCE_INTERNAL_ERROR;
  } catch (...) {
    return BRUCE_INTERNAL_ERROR;
  }

  return BRUCE_OK;
}

extern "C"
void bruce_destroy_dg_socket_writer(struct bruce_dg_socket_writer *sw) {
  if (sw) {
    delete sw->writer;
    delete sw;
  }
}

extern "C"
int bruce_write_to_dg_socket(struct bruce_dg_socket_writer *sw,
    const void *msg, size_t msg_size) {
  if ((sw == nullptr) || (msg == nullptr)) {
    return BRUCE_INVALID_INPUT;
  }

  assert(sw->writer);

  try {
    sw->writer->WriteMsg(msg, msg_size);
  } catch (const std::system_error &x) {
    return x.code().value();
  } catch (const std::exception &) {
    return BRUCE_INTERNAL_ERROR;
  } catch (...) {
    return BRUCE_INTERNAL_ERROR;
  }

  return BRUCE_OK;
}
