/* <bruce/router_thread_api.h>

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

   Class defining Router thread API for bruce daemon.
 */

#pragma once

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/msg.h>
#include <bruce/util/gate_put_api.h>
#include <bruce/util/worker_thread_api.h>

namespace Bruce {

  /* This class is just an interface.  Subclasses are TRouterThread (the real
     router thread) and TMockRouterThread (for testing). */
  class TRouterThreadApi : virtual public Util::TWorkerThreadApi {
    NO_COPY_SEMANTICS(TRouterThreadApi);

    public:
    using TMsgChannel = Util::TGatePutApi<TMsg::TPtr>;

    enum class TShutdownStatus {
      Normal,
      Error
    };  // TShutdownStatus

    virtual ~TRouterThreadApi() noexcept { }

    /* This method is used only by unit test code.  The input thread does not
       wait for the router thread to finish its initialization, since the input
       thread must immediately be ready to read datagrams from its socket.  In
       the case where the Kafka cluster is temporarily unavailable, router
       thread initialization can take arbitrarily long. */
    virtual const Base::TFd &GetInitWaitFd() const = 0;

    virtual TShutdownStatus GetShutdownStatus() const = 0;

    virtual TMsgChannel &GetMsgChannel() = 0;

    virtual size_t GetAckCount() const = 0;

    virtual Base::TEventSemaphore &GetMetadataUpdateRequestSem() = 0;

    protected:
    TRouterThreadApi() = default;
  };  // TRouterThreadApi

}  // Bruce
