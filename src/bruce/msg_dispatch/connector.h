/* <bruce/msg_dispatch/connector.h>

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

   Dispatcher class representing a connection to a Kafka broker.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>

#include <base/no_copy_semantics.h>
#include <bruce/metadata.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/connection_shared_state.h>
#include <bruce/msg_dispatch/dispatcher_shared_state.h>
#include <bruce/msg_dispatch/receiver.h>
#include <bruce/msg_dispatch/sender.h>

namespace Bruce {

  namespace MsgDispatch {

    class TConnector final {
      NO_COPY_SEMANTICS(TConnector);

      public:
      using TShutdownStatus = TDispatcherShutdownStatus;

      TConnector(size_t my_broker_index, TDispatcherSharedState &ds);

      void Start(const std::shared_ptr<TMetadata> &md);

      void Dispatch(TMsg::TPtr &&msg) {
        assert(this);
        Sender.Dispatch(std::move(msg));
        assert(!msg);
      }

      void DispatchNow(TMsg::TPtr &&msg) {
        assert(this);
        Sender.DispatchNow(std::move(msg));
        assert(!msg);
      }

      void DispatchNow(std::list<std::list<TMsg::TPtr>> &&batch) {
        assert(this);
        Sender.DispatchNow(std::move(batch));
        assert(batch.empty());
      }

      void StartSlowShutdown(uint64_t start_time);

      void StartFastShutdown();

      void WaitForShutdownAcks();

      void JoinAll();

      TShutdownStatus GetShutdownStatus() const {
        assert(this);
        return ShutdownStatus;
      }

      std::list<std::list<TMsg::TPtr>> GetAckWaitQueueAfterShutdown() {
        assert(this);
        return std::move(Cs.AckWaitAfterShutdown);
      }

      std::list<std::list<TMsg::TPtr>> GetSendWaitQueueAfterShutdown() {
        assert(this);
        return std::move(Cs.SendWaitAfterShutdown);
      }

      private:
      TConnectionSharedState Cs;

      /* Thread sends produce requests to broker. */
      TSender Sender;

      /* Thread receives produce responses from broker. */
      TReceiver Receiver;

      TShutdownStatus ShutdownStatus;
    };  // TConnector

  }  // MsgDispatch

}  // Bruce
