/* <bruce/util/gate.h>

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

   Interthread message passing mechanism for bruce daemon.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <mutex>

#include <base/event_semaphore.h>
#include <base/no_copy_semantics.h>
#include <bruce/util/gate_get_api.h>
#include <bruce/util/gate_put_api.h>

namespace Bruce {

  namespace Util {

    template <typename TMsgType>
    class TGate final : public TGatePutApi<TMsgType>,
                        public TGateGetApi<TMsgType> {
      NO_COPY_SEMANTICS(TGate);

      public:
      TGate() = default;

      virtual ~TGate() noexcept { }

      virtual void Put(std::list<TMsgType> &&put_list) {
        assert(this);

        if (!put_list.empty()) {
          bool was_empty = false;

          {
            std::lock_guard<std::mutex> lock(Mutex);
            was_empty = MsgList.empty();
            MsgList.splice(MsgList.end(), std::move(put_list));
          }

          if (was_empty) {
            Sem.Push();
          }
        }
      }

      virtual void Put(TMsgType &&put_item) override {
        assert(this);

        {
          std::lock_guard<std::mutex> lock(Mutex);
          MsgList.push_back(std::move(put_item));
        }

        Sem.Push();
      }

      virtual std::list<TMsgType> Get() override {
        assert(this);
        Sem.Pop();
        return NonblockingGet();
      }

      virtual std::list<TMsgType> NonblockingGet() override {
        assert(this);
        std::list<TMsgType> result;

        {
          std::lock_guard<std::mutex> lock(Mutex);
          result.splice(result.end(), MsgList);
        }

        return std::move(result);
      }

      virtual const Base::TFd &GetMsgAvailableFd() const override {
        assert(this);
        return Sem.GetFd();
      }

      void Reset() {
        assert(this);
        Sem.Reset();
        MsgList.clear();
      }

      private:
      Base::TEventSemaphore Sem;

      std::mutex Mutex;

      std::list<TMsgType> MsgList;
    };  // TGate

  }  // Util

}  // Bruce
