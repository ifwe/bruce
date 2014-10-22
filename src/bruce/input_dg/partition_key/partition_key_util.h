/* <bruce/input_dg/partition_key/partition_key_util.h>

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

   Utilities for dealing with input datagrams directed to a partition of a
   specific topic according to a supplied hash value.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include <bruce/anomaly_tracker.h>
#include <bruce/msg.h>
#include <bruce/msg_state_tracker.h>
#include <capped/pool.h>

namespace Bruce {

  namespace InputDg {

    namespace PartitionKey {

      TMsg::TPtr BuildPartitionKeyMsgFromDg(const uint8_t *dg_bytes,
          size_t dg_size, int16_t api_version,
          const uint8_t *versioned_part_begin,
          const uint8_t *versioned_part_end, Capped::TPool &pool,
          TAnomalyTracker &anomaly_tracker,
          TMsgStateTracker &msg_state_tracker, bool no_log_discard);

    }  // PartitionKey

  }  // InputDg

}  // Bruce
