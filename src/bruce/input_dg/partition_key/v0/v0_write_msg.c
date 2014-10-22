/* <bruce/input_dg/partition_key/v0/v0_write_msg.cc>

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

   Implements <bruce/input_dg/partition_key/v0/v0_write_msg.h>.
 */

#include <bruce/input_dg/partition_key/v0/v0_write_msg.h>

#include <assert.h>
#include <string.h>

#include <base/field_access.h>
#include <bruce/input_dg/partition_key/v0/v0_input_dg_constants.h>
#include <bruce/input_dg/input_dg_constants.h>

static inline size_t get_msg_overhead() {
  return INPUT_DG_SZ_FIELD_SIZE + INPUT_DG_API_KEY_FIELD_SIZE +
      INPUT_DG_API_VERSION_FIELD_SIZE + INPUT_DG_P_KEY_V0_FLAGS_FIELD_SIZE +
      INPUT_DG_P_KEY_V0_PARTITION_KEY_FIELD_SIZE +
      INPUT_DG_P_KEY_V0_TOPIC_SZ_FIELD_SIZE + INPUT_DG_P_KEY_V0_TS_FIELD_SIZE +
      INPUT_DG_P_KEY_V0_KEY_SZ_FIELD_SIZE +
      INPUT_DG_P_KEY_V0_VALUE_SZ_FIELD_SIZE;
}

int input_dg_p_key_v0_check_msg_size(size_t topic_size, size_t key_size,
    size_t value_size) {
  if (topic_size > INT16_MAX) {
    return BRUCE_TOPIC_TOO_LARGE;
  }

  size_t key_value_space = INT32_MAX - get_msg_overhead() - topic_size;

  if ((key_size > key_value_space) ||
      (value_size > (key_value_space - key_size))) {
    return BRUCE_MSG_TOO_LARGE;
  }

  return BRUCE_OK;
}

static inline size_t do_compute_msg_size(size_t topic_size, size_t key_size,
    size_t value_size) {
  return get_msg_overhead() + topic_size + key_size + value_size;
}

int input_dg_p_key_v0_compute_msg_size(size_t *result, size_t topic_size,
    size_t key_size, size_t value_size) {
  *result = 0;
  int ret = input_dg_p_key_v0_check_msg_size(topic_size, key_size, value_size);

  if (ret != BRUCE_OK) {
    return ret;
  }

  *result = do_compute_msg_size(topic_size, key_size, value_size);
  return BRUCE_OK;
}

void input_dg_p_key_v0_write_msg(void *result_buf, int64_t timestamp,
    int32_t partition_key, const void *topic_begin, const void *topic_end,
    const void *key_begin, const void *key_end, const void *value_begin,
    const void *value_end) {
  assert(result_buf);
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(key_begin || (key_end == key_begin));
  assert(key_end >= key_begin);
  assert(value_begin || (value_end == value_begin));
  assert(value_end >= value_begin);
  uint8_t *pos = (uint8_t *) result_buf;
  const uint8_t *topic_start = (const uint8_t *) topic_begin;
  const uint8_t *topic_finish = (const uint8_t *) topic_end;
  const uint8_t *key_start = (const uint8_t *) key_begin;
  const uint8_t *key_finish = (const uint8_t *) key_end;
  const uint8_t *value_start = (const uint8_t *) value_begin;
  const uint8_t *value_finish = (const uint8_t *) value_end;
  size_t topic_size = topic_finish - topic_start;
  size_t key_size = key_finish - key_start;
  size_t value_size = value_finish - value_start;
  size_t msg_size = 0;

  if (input_dg_p_key_v0_compute_msg_size(&msg_size, topic_size, key_size,
        value_size) != BRUCE_OK) {
    assert(0);
    return;
  }

  topic_finish = topic_start + topic_size;
  key_finish = key_start + key_size;
  value_finish = value_start + value_size;
  WriteInt32ToHeader(pos, msg_size);
  pos += INPUT_DG_SZ_FIELD_SIZE;

  WriteInt16ToHeader(pos, 257);

  pos += INPUT_DG_API_KEY_FIELD_SIZE;
  WriteInt16ToHeader(pos, 0);  // API version
  pos += INPUT_DG_API_VERSION_FIELD_SIZE;
  WriteInt16ToHeader(pos, 0);  // flags
  pos += INPUT_DG_P_KEY_V0_FLAGS_FIELD_SIZE;
  WriteInt32ToHeader(pos, partition_key);
  pos += INPUT_DG_P_KEY_V0_PARTITION_KEY_FIELD_SIZE;
  WriteInt16ToHeader(pos, topic_size);
  pos += INPUT_DG_P_KEY_V0_TOPIC_SZ_FIELD_SIZE;
  memcpy(pos, topic_start, topic_size);
  pos += topic_size;
  WriteInt64ToHeader(pos, timestamp);
  pos += INPUT_DG_P_KEY_V0_TS_FIELD_SIZE;
  WriteInt32ToHeader(pos, key_size);
  pos += INPUT_DG_P_KEY_V0_KEY_SZ_FIELD_SIZE;

  if (key_start) {
    memcpy(pos, key_start, key_size);
  }

  pos += key_size;
  WriteInt32ToHeader(pos, value_size);
  pos += INPUT_DG_P_KEY_V0_VALUE_SZ_FIELD_SIZE;

  if (value_start) {
    memcpy(pos, value_start, value_size);
  }
}
