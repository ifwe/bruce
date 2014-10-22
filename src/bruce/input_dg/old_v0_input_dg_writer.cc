/* <bruce/input_dg/old_v0_input_dg_writer.cc>

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

   Implements <bruce/input_dg/old_v0_input_dg_writer.h>.
 */

#include <bruce/input_dg/old_v0_input_dg_writer.h>

#include <cassert>
#include <cstring>

#include <base/field_access.h>
#include <bruce/input_dg/input_dg_constants.h>
#include <bruce/input_dg/old_v0_input_dg_common.h>

using namespace Bruce;
using namespace Bruce::InputDg;

size_t TOldV0InputDgWriter::ComputeDgSize(size_t topic_size,
    size_t body_size) {
  if (topic_size > MAX_TOPIC_SIZE) {
    assert(false);
    topic_size = MAX_TOPIC_SIZE;
  }

  if (body_size > MAX_BODY_SIZE) {
    assert(false);
    body_size = MAX_BODY_SIZE;
  }

  return INPUT_DG_SZ_FIELD_SIZE + INPUT_DG_OLD_VER_FIELD_SIZE +
      OLD_V0_TS_FIELD_SIZE + OLD_V0_TOPIC_SZ_FIELD_SIZE + topic_size +
      OLD_V0_BODY_SZ_FIELD_SIZE + body_size;
}

void TOldV0InputDgWriter::WriteDg(void *result_buf, int64_t timestamp, 
    const void *topic_begin, const void *topic_end, const void *body_begin,
    const void *body_end) {
  assert(this);
  assert(result_buf);
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(body_begin);
  assert(body_end >= body_begin);
  uint8_t *pos = reinterpret_cast<uint8_t *>(result_buf);
  const uint8_t *topic_start = reinterpret_cast<const uint8_t *>(topic_begin);
  const uint8_t *topic_finish = reinterpret_cast<const uint8_t *>(topic_end);
  const uint8_t *body_start = reinterpret_cast<const uint8_t *>(body_begin);
  const uint8_t *body_finish = reinterpret_cast<const uint8_t *>(body_end);
  size_t topic_size = topic_finish - topic_start;

  if (topic_size > MAX_TOPIC_SIZE) {
    assert(false);
    topic_size = MAX_TOPIC_SIZE;
    topic_finish = topic_start + topic_size;
  }

  size_t body_size = body_finish - body_start;

  if (body_size > MAX_BODY_SIZE) {
    assert(false);
    body_size = MAX_BODY_SIZE;
    body_finish = body_start + body_size;
  }

  WriteInt32ToHeader(pos,
      TOldV0InputDgWriter::ComputeDgSize(topic_size, body_size));
  pos += INPUT_DG_SZ_FIELD_SIZE;
  *pos = 0;
  ++pos;
  WriteInt64ToHeader(pos, timestamp);
  pos += OLD_V0_TS_FIELD_SIZE;
  *pos = topic_size;
  ++pos;
  std::memcpy(pos, topic_start, topic_size);
  pos += topic_size;
  WriteInt32ToHeader(pos, body_size);
  pos += OLD_V0_BODY_SZ_FIELD_SIZE;
  std::memcpy(pos, body_start, body_size);
}

size_t TOldV0InputDgWriter::WriteDg(std::vector<uint8_t> &result_buf,
    int64_t timestamp, const void *topic_begin, const void *topic_end,
    const void *body_begin, const void *body_end) {
  assert(this);
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(body_begin);
  assert(body_end >= body_begin);
  const uint8_t *topic_start = reinterpret_cast<const uint8_t *>(topic_begin);
  size_t topic_size = reinterpret_cast<const uint8_t *>(topic_end) -
      topic_start;

  if (topic_size > MAX_TOPIC_SIZE) {
    assert(false);
    topic_size = MAX_TOPIC_SIZE;
  }

  const uint8_t *body_start = reinterpret_cast<const uint8_t *>(body_begin);
  size_t body_size = reinterpret_cast<const uint8_t *>(body_end) - body_start;

  if (body_size > MAX_BODY_SIZE) {
    assert(false);
    body_size = MAX_BODY_SIZE;
  }

  size_t dg_size = ComputeDgSize(topic_size, body_size);

  if (result_buf.size() < dg_size) {
    result_buf.resize(dg_size);
  }

  WriteDg(&result_buf[0], timestamp, topic_start, topic_start + topic_size,
          body_start, body_start + body_size);
  return dg_size;
}
