/* <bruce/client/bruce_client.h>

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

   Header file for Bruce client library.
   Here is example C code for sending an AnyPartition message to Bruce:

       // Create a UNIX domain datagram socket and bind() it to Bruce's input
       // socket.  You must destroy 'sw' by calling
       // bruce_destroy_dg_socket_writer() when you are done sending messages.
       struct bruce_dg_socket_writer *sw = NULL;
       int ret = bruce_create_dg_socket_writer("/path/to/bruce/socket", &sw);

       if (ret != BRUCE_OK) {
         // handle error
       }

       const char topic[] = "some_topic";  // Kafka topic
       const char key[] = "";  // message key
       const char value[] = "hello world";  // message value

       // should be current time in milliseconds since the epoch
       int64_t timestamp = 0;

       // Figure out how much memory is needed for datagram buffer.
       size_t topic_size = strlen(topic);
       size_t key_size = strlen(key);
       size_t value_size = strlen(value);
       size_t msg_size = 0;
       ret = bruce_find_any_partition_msg_size(topic_size, key_size,
           value_size, &msg_size);

       if (ret != BRUCE_OK) {
         // handle error
       }

       // Allocate datagram buffer.
       void *msg_buf = malloc(msg_size);

       if (msg_buf == NULL) {
         // handle out of memory condition
       }

       // Write datagram into buffer.
       ret = bruce_write_any_partition_msg(msg_buf, msg_size, topic, timestamp,
           key, key_size, value, value_size);

       if (ret != BRUCE_OK) {
         // handle error
       }

       // Send datagram to Bruce.
       ret = bruce_write_to_dg_socket(sw, msg_buf, msg_size);

       if (ret != BRUCE_OK) {
         // handle error
       }

       // Clean up.
       free(msg_buf);
       bruce_destroy_dg_socket_writer(sw);

   The code for sending a PartitionKey message is identical except that you
   call bruce_find_partition_key_msg_size() instead of
   bruce_find_any_partition_msg_size(), and bruce_write_partition_key_msg()
   instead of bruce_write_any_partition_msg().
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <bruce/client/status_codes.h>

struct bruce_dg_socket_writer;

#ifdef __cplusplus
extern "C" {
#endif

/* See <bruce/client/status_codes.h> for definitions of status codes returned
   by library functions. */

/* Compute the total size of an AnyPartition message with topic size (as
   reported by strlen()) 'topic_size', key size 'key_size', and value size
   'value_size'.  All sizes are in bytes.  On success, write total message size
   to *out_size and return BRUCE_OK.  Possible returned error codes are
   { BRUCE_INVALID_INPUT, BRUCE_TOPIC_TOO_LARGE, BRUCE_MSG_TOO_LARGE }. */
int bruce_find_any_partition_msg_size(size_t topic_size, size_t key_size,
    size_t value_size, size_t *out_size);

/* Compute the total size of a PartitionKey message with topic size (as
   reported by strlen()) 'topic_size', key size 'key_size', and value size
   'value_size'.  All sizes are in bytes.  On success, write total message size
   to *out_size and return BRUCE_OK.  Possible returned error codes are
   { BRUCE_INVALID_INPUT, BRUCE_TOPIC_TOO_LARGE, BRUCE_MSG_TOO_LARGE }. */
int bruce_find_partition_key_msg_size(size_t topic_size, size_t key_size,
    size_t value_size, size_t *out_size);

/* Write an AnyPartition message to output buffer 'out_buf' whose size is
   'out_buf_size'.  'topic' specifies the topic string.  'timestamp' gives
   timestamp to assign to message in milliseconds since the epoch.  'key' and
   'key_size' specify message key.  'value' and 'value_size' specify message
   value.  All sizes are in bytes.  'out_buf_size' must be at least as large as
   the size reported by bruce_find_any_partition_msg_size().  Return BRUCE_OK
   on success.  Possible returned error codes are { BRUCE_INVALID_INPUT,
   BRUCE_BUF_TOO_SMALL, BRUCE_TOPIC_TOO_LARGE, BRUCE_MSG_TOO_LARGE }. */
int bruce_write_any_partition_msg(void *out_buf, size_t out_buf_size,
    const char *topic, int64_t timestamp, const void *key, size_t key_size,
    const void *value, size_t value_size);

/* Write a PartitionKey message to output buffer 'out_buf' whose size is
   'out_buf_size'.  'topic' specifies the topic string.  'timestamp' gives
   timestamp to assign to message in milliseconds since the epoch.
   'partition_key' gives the partition key for message routing.  'key' and
   'key_size' specify message key.  'value' and 'value_size' specify message
   value.  All sizes are in bytes.  'out_buf_size' must be at least as large as
   the size reported by bruce_find_partition_key_msg_size().  Return BRUCE_OK
   on success.  Possible returned error codes are { BRUCE_INVALID_INPUT,
   BRUCE_BUF_TOO_SMALL, BRUCE_TOPIC_TOO_LARGE, BRUCE_MSG_TOO_LARGE }. */
int bruce_write_partition_key_msg(void *out_buf, size_t out_buf_size,
    int32_t partition_key, const char *topic, int64_t timestamp,
    const void *key, size_t key_size, const void *value, size_t value_size);

/* Create a bruce_dg_socket_writer struct for use with
   bruce_write_to_dg_socket().  'socket_path' specifies the pathname of Bruce's
   input UNIX domain datagram socket.  On success, set *out_sw to the newly
   created bruce_dg_socket_writer struct and return BRUCE_OK.  Caller takes
   ownership of newly created bruce_dg_socket_writer struct, and must destroy
   it by calling bruce_destroy_dg_socket_writer() when finished with it.
   There are two types of error codes this function may return:

       1.  If returned error code is negative, then its value will be one of
           { BRUCE_INTERNAL_ERROR, BRUCE_INVALID_INPUT }.

       2.  If returned error code is > 0, then its value will be an errno value
           giving the reason why the operation failed.
*/
int bruce_create_dg_socket_writer(const char *socket_path,
    struct bruce_dg_socket_writer **out_sw);

/* Destroy a bruce_dg_socket_writer struct previously created by calling
   bruce_create_dg_socket_writer(). */
void bruce_destroy_dg_socket_writer(struct bruce_dg_socket_writer *sw);

/* Write a message to Bruce's input socket, where 'sw' was created by calling
   bruce_create_dg_socket_writer(), and the message is specified by 'msg' and
   'msg_size'.  The message is expected to have been created by calling
   bruce_write_any_partition_msg() or bruce_write_partition_key_msg().  Return
   BRUCE_OK on success.  There are two types of error codes this function may
   return:

       1.  If returned error code is negative, then its value will be one of
           { BRUCE_INTERNAL_ERROR, BRUCE_INVALID_INPUT }.

       2.  If returned error code is > 0, then its value will be an errno value
           giving the reason why the operation failed.
*/
int bruce_write_to_dg_socket(struct bruce_dg_socket_writer *sw,
    const void *msg, size_t msg_size);

#ifdef __cplusplus
}  // extern "C"
#endif
