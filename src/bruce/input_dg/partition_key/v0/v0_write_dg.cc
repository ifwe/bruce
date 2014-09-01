/* <bruce/input_dg/partition_key/v0/v0_write_dg.cc>

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

   Implements <bruce/input_dg/partition_key/v0/v0_write_dg.h>.
 */

#include <bruce/input_dg/partition_key/v0/v0_write_dg.h>

#include <cassert>
#include <cstring>

#include <base/field_access.h>
#include <bruce/input_dg/partition_key/v0/v0_input_dg_constants.h>
#include <bruce/input_dg/input_dg_constants.h>

using namespace Bruce;
using namespace Bruce::InputDg;
using namespace Bruce::InputDg::PartitionKey;
using namespace Bruce::InputDg::PartitionKey::V0;

TV0InputDgWriter::TDgSizeResult
Bruce::InputDg::PartitionKey::V0::WriteDg(
    std::vector<uint8_t> &result_buf, int64_t timestamp, int32_t partition_key,
    const void *topic_begin, const void *topic_end, const void *key_begin,
    const void *key_end, const void *value_begin, const void *value_end) {
  assert(topic_begin);
  assert(topic_end >= topic_begin);
  assert(key_begin || (key_end == key_begin));
  assert(key_end >= key_begin);
  assert(value_begin || (value_end == value_begin));
  assert(value_end >= value_begin);
  size_t topic_size = reinterpret_cast<const uint8_t *>(topic_end) -
                      reinterpret_cast<const uint8_t *>(topic_begin);
  size_t key_size = reinterpret_cast<const uint8_t *>(key_end) -
                    reinterpret_cast<const uint8_t *>(key_begin);
  size_t value_size = reinterpret_cast<const uint8_t *>(value_end) -
                    reinterpret_cast<const uint8_t *>(value_begin);
  size_t size = 0;
  TV0InputDgWriter::TDgSizeResult ret = TV0InputDgWriter::ComputeDgSize(size,
      topic_size, key_size, value_size);

  if (ret != TV0InputDgWriter::TDgSizeResult::Ok) {
    return ret;
  }

  result_buf.resize(size);
  TV0InputDgWriter().WriteDg(&result_buf[0], timestamp, partition_key,
      topic_begin, topic_end, key_begin, key_end, value_begin, value_end);
  return TV0InputDgWriter::TDgSizeResult::Ok;
}
