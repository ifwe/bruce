/* <bruce/client/status_codes.h>

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

   Status codes for Bruce client library.
 */

#pragma once

/* Status codes for library calls. */
enum {
  /* Success. */
  BRUCE_OK = 0,

  /* Internal error (should never occur). */
  BRUCE_INTERNAL_ERROR = -1,

  /* Supplied output buffer does not have enough space for result. */
  BRUCE_BUF_TOO_SMALL = -2,

  /* Kafka topic is too large. */
  BRUCE_TOPIC_TOO_LARGE = -3,

  /* Result message would exceed maximum possible size. */
  BRUCE_MSG_TOO_LARGE = -4
};
