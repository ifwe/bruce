/* <bruce/input_dg/partition_key/v0/v0_write_msg.h>

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

   Functions for creating PartitionKey datagrams to write to Bruce's input
   socket.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <bruce/client/status_codes.h>

/* This is a pure C implementation.  Avoiding C++ here allows C programs to use
   the client library without having to link to the standard C++ library. */

#ifdef __cplusplus
extern "C" {
#endif

/* See <bruce/client/status_codes.h> for definitions of returned status codes.
 */

/* Return BRUCE_OK if datagram can be created with the given field sizes in
   bytes.  Otherwise return BRUCE_TOPIC_TOO_LARGE or BRUCE_MSG_TOO_LARGE. */
int input_dg_p_key_v0_check_msg_size(size_t topic_size, size_t key_size,
    size_t value_size);

/* Compute size of datagram with field sizes given by 'topic_size', 'key_size',
   and 'value_size'.  On success, BRUCE_OK will be returned, and *result will
   contain the computed size.  All sizes are in bytes.  On error,
   BRUCE_TOPIC_TOO_LARGE or BRUCE_MSG_TOO_LARGE will be returned. */
int input_dg_p_key_v0_compute_msg_size(size_t *result, size_t topic_size,
    size_t key_size, size_t value_size);

/* Write datagram into 'result_buf'.  It is assumed that 'result_buf' has
enough space for entire datagram (see input_dg_p_key_v0_compute_msg_size()). */
void input_dg_p_key_v0_write_msg(void *result_buf, int64_t timestamp,
    int32_t partition_key, const void *topic_begin, const void *topic_end,
    const void *key_begin, const void *key_end, const void *value_begin,
    const void *value_end);

#ifdef __cplusplus
}  // extern "C"
#endif
