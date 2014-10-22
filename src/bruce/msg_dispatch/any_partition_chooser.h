/* <bruce/msg_dispatch/any_partition_chooser.h>

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

   Class used for choosing a partition for AnyPartition messages.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

#include <base/opt.h>
#include <bruce/metadata.h>

namespace Bruce {

  namespace MsgDispatch {

    class TAnyPartitionChooser final {
      public:
      TAnyPartitionChooser()
          : Count(0), 
            ChoiceUsed(false) {
      }

      int32_t GetChoice(size_t broker_index, const TMetadata &md,
          const std::string &topic) {
        assert(this);

        if (Choice.IsUnknown()) {
          Choose(broker_index, md, topic);
        }

        return *Choice;
      }

      void SetChoiceUsed() {
        assert(this);
        ChoiceUsed = true;
      }

      void ClearChoice() {
        assert(this);
        Choice.Reset();

        if (ChoiceUsed) {
          ++Count;
          ChoiceUsed = false;
        }
      }

      private:
      void Choose(size_t broker_index, const TMetadata &md,
          const std::string &topic);

      size_t Count;

      Base::TOpt<int32_t> Choice;

      bool ChoiceUsed;
    };  // TAnyPartitionChooser

  }  // MsgDispatch

}  // Bruce
