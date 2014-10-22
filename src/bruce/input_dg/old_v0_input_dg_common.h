/* <bruce/input_dg/old_v0_input_dg_common.h>

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

   Common implementation functions for dealing with input datagrams that get
   transmitted over bruce's UNIX domain datagram socket.  Everything here is
   specific to version 0 of the input datagram format.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include <bruce/anomaly_tracker.h>
#include <bruce/msg.h>

namespace Bruce {

  namespace InputDg {

    enum { OLD_V0_TS_FIELD_SIZE = 8 };

    enum { OLD_V0_TOPIC_SZ_FIELD_SIZE = 1 };

    enum { OLD_V0_BODY_SZ_FIELD_SIZE = 4 };

  }  // InputDg

}  // Bruce
