/* <bruce/msg_dispatch/connection_shared_state.h>

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

   State shared by send and receive thread pair for a connection to a broker.
 */

#pragma once

#include <cstdint>
#include <list>
#include <utility>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/common.h>
#include <bruce/util/gate.h>

namespace Bruce {

  namespace MsgDispatch {

    struct TConnectionSharedState {
      NO_COPY_SEMANTICS(TConnectionSharedState);

      TConnectionSharedState() = default;

      /* Connection to Kafka broker. */
      Base::TFd Sock;

      /* The send thread uses this to notify the receive thread that it has
         finished connecting to the Kafka broker (either successfully or
         unsuccessfully). */
      Base::TEventSemaphore ConnectFinished;

      /* Sent messages waiting to be claimed by receive thread. */
      Util::TGate<TProduceRequest> SendFinishedQueue;

      /* Messages waiting to be claimed by send thread for resend. */
      Util::TGate<std::list<TMsg::TPtr>> ResendQueue;

      /* After both the send thread and the receive thread are shutdown, all
         messages waiting to be sent are moved to this list.  */
      std::list<std::list<TMsg::TPtr>> SendWaitAfterShutdown;

      /* After both the send thread and the receive thread are shutdown, all
         messages waiting for an ACK (or that received an error ACK causing the
         message to be rerouted after metadata update) are moved to this list.
       */
      std::list<std::list<TMsg::TPtr>> AckWaitAfterShutdown;
    };  // TDispatcherSharedState

  }  // MsgDispatch

}  // Bruce
