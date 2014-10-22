/* <bruce/msg_dispatch/api_defs.h>

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

   Definitions used by Kafka dispatcher API.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/msg.h>

namespace Bruce {

  namespace MsgDispatch {

    /* 'Started' indicates that Start() has been called, but a corresponding
       call to StartSlowShutdown(), StartFastShutdown(), or JoinAll() has not
       yet been made.  'ShuttingDown' indicates that StartSlowShutdown() or
       StartFastShutdown() has been called but a corresponding call to
       JoinAll() has not yet been made.  'Stopped' indicates that either
       Start() has never been called, or Start() has been called followed by a
       corresponding call to JoinAll().  After JoinAll() has been called, the
       dispatcher may be started again by calling Start().  In the case where
       the router thread detects a pause event (i.e. the FD returned by
       GetPauseFd() becomes readable), it will call JoinAll() without first
       calling one of the shutdown functions.  In this case, the dispatcher
       state changes from 'Started' directly to 'Stopped'. */
    enum class TDispatcherState {
      Started,
      ShuttingDown,
      Stopped
    };  // TDispatcherState

    enum class TDispatcherShutdownStatus {
      Normal,
      Error
    };  // TDispatcherShutdownStatus

  }  // MsgDispatch

}  // Bruce
