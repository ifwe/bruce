/* <bruce/input_dg/partition_key/v0/v0_input_dg_reader.h>

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

   Class for reading datagram from bruce's input socket that conforms to
   version 0 of the input format for PartitionKey messages.  Builds a TMsg from
   the datagram contents.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

#include <base/no_copy_semantics.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/input_dg/partition_key/v0/v0_input_dg_constants.h>
#include <bruce/msg.h>
#include <bruce/msg_state_tracker.h>
#include <capped/pool.h>

namespace Bruce {

  namespace InputDg {

    namespace PartitionKey {

      namespace V0 {

        class TV0InputDgReader final {
          NO_COPY_SEMANTICS(TV0InputDgReader);

          public:
          TV0InputDgReader(const uint8_t *dg_begin,
              const uint8_t *data_begin, const uint8_t *data_end,
              Capped::TPool &pool, TAnomalyTracker &anomaly_tracker,
              TMsgStateTracker &msg_state_tracker, bool no_log_discard)
              : DgBegin(dg_begin),
                DataBegin(data_begin),
                DataEnd(data_end),
                DgSize(data_end - dg_begin),
                DataSize(data_end - data_begin),
                NoLogDiscard(no_log_discard),
                Pool(pool),
                AnomalyTracker(anomaly_tracker),
                MsgStateTracker(msg_state_tracker) {
            assert(DgBegin);
            assert(DataBegin > DgBegin);
            assert(DataEnd >= DataBegin);
          }

          TMsg::TPtr BuildMsg();

          private:
          /* Points to first byte of input datagram. */
          const uint8_t * const DgBegin;

          /* Points to first byte of version-specific part of input
             datagram. */
          const uint8_t * const DataBegin;

          /* Points one byte past last byte of input datagram. */
          const uint8_t * const DataEnd;

          /* Size in bytes of input datagram. */
          const size_t DgSize;

          /* Size in bytes of version-specific part of input datagram. */
          const size_t DataSize;

          const bool NoLogDiscard;

          /* Pool to allocate space for TMsg we are building from input
             datagram. */
          Capped::TPool &Pool;

          /* If some problem causes us to discard the input datagram while
             attempting to build a TMsg from it, we record the discard here. */
          TAnomalyTracker &AnomalyTracker;

          TMsgStateTracker &MsgStateTracker;
        };  // class TV0InputDgReader

      }  // V0

    }  // PartitionKey

  }  // InputDg

}  // Bruce
