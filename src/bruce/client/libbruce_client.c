/* <bruce/client/libbruce_client.c>

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

   Bruce client C library implementation.
 */

#include <bruce/client/bruce_client.h>

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <base/export_sym.h>
#include <bruce/client/build_id.h>
#include <bruce/input_dg/any_partition/v0/v0_write_msg.h>
#include <bruce/input_dg/partition_key/v0/v0_write_msg.h>

const char EXPORT_SYM *bruce_get_build_id() {
  return bruce_client_lib_build_id;
}

int EXPORT_SYM bruce_find_any_partition_msg_size(size_t topic_size,
    size_t key_size, size_t value_size, size_t *out_size) {
  assert(out_size);
  return input_dg_any_p_v0_compute_msg_size(out_size, topic_size, key_size,
      value_size);
}

int EXPORT_SYM bruce_find_partition_key_msg_size(size_t topic_size,
    size_t key_size, size_t value_size, size_t *out_size) {
  assert(out_size);
  return input_dg_p_key_v0_compute_msg_size(out_size, topic_size, key_size,
      value_size);
}

int EXPORT_SYM bruce_write_any_partition_msg(void *out_buf,
    size_t out_buf_size, const char *topic, int64_t timestamp, const void *key,
    size_t key_size, const void *value, size_t value_size) {
  assert(out_buf);
  assert(topic);
  assert(key || (key_size == 0));
  assert(value || (value_size == 0));
  size_t topic_len = strlen(topic);
  size_t dg_size = 0;
  int ret = bruce_find_any_partition_msg_size(topic_len, key_size, value_size,
      &dg_size);

  if (ret != BRUCE_OK) {
    return ret;
  }

  if (out_buf_size < dg_size) {
    return BRUCE_BUF_TOO_SMALL;
  }

  input_dg_any_p_v0_write_msg(out_buf, timestamp, topic, topic + topic_len,
      key, ((const uint8_t *) key) + key_size, value,
      ((const uint8_t *) value) + value_size);
  return BRUCE_OK;
}

int EXPORT_SYM bruce_write_partition_key_msg(void *out_buf,
    size_t out_buf_size, int32_t partition_key, const char *topic,
    int64_t timestamp, const void *key, size_t key_size, const void *value,
    size_t value_size) {
  assert(out_buf);
  assert(topic);
  assert(key || (key_size == 0));
  assert(value || (value_size == 0));
  size_t topic_len = strlen(topic);
  size_t dg_size = 0;
  int ret = bruce_find_partition_key_msg_size(topic_len, key_size, value_size,
      &dg_size);

  if (ret != BRUCE_OK) {
    return ret;
  }

  if (out_buf_size < dg_size) {
    return BRUCE_BUF_TOO_SMALL;
  }

  input_dg_p_key_v0_write_msg(out_buf, timestamp, partition_key, topic,
      topic + topic_len, key, ((const uint8_t *) key) + key_size, value,
      ((const uint8_t *) value) + value_size);
  return BRUCE_OK;
}

void EXPORT_SYM bruce_client_socket_init(
    bruce_client_socket_t *client_socket) {
  assert(client_socket);
  client_socket->sock_fd = -1;
}

static int init_client_addr(struct sockaddr_un *client_addr) {
  assert(client_addr);
  memset(client_addr, 0, sizeof(*client_addr));
  static const char client_path_template[] = "/tmp/bruce_client.XXXXXX";
  char client_path[sizeof(client_path_template)];

  /* Unfortunately the preprocessor doesn't understand sizeof(), so this can't
     be converted to a purely compile-time test. */
  if (sizeof(client_addr->sun_path) < sizeof(client_path)) {
    return BRUCE_CLIENT_SOCK_PATH_TOO_LONG;
  }

  int tmp_fd = -1;

  /* Create temporary filename that client will bind() its socket to.  This is
     the equivalent of an ephemeral port for UNIX domain sockets. */
  for (; ; ) {
    strncpy(&client_path[0], client_path_template,
            sizeof(client_path_template));
    tmp_fd = mkstemp(&client_path[0]);

    if (tmp_fd >= 0) {
      break;  // success
    }

    if (errno != EEXIST) {
      return errno;
    }
  }

  int status = BRUCE_OK;
  client_addr->sun_family = AF_LOCAL;
  strncpy(client_addr->sun_path, client_path, sizeof(client_addr->sun_path));

  if ((unlink(client_path) < 0) && (errno != ENOENT)) {
    status = errno;
  }

  close(tmp_fd);
  return status;
}

int EXPORT_SYM bruce_client_socket_bind(bruce_client_socket_t *client_socket,
    const char *server_path) {
  assert(client_socket);
  assert(server_path);

  if (client_socket->sock_fd >= 0) {
    return BRUCE_CLIENT_SOCK_IS_OPENED;
  }

  memset(&client_socket->server_addr, 0, sizeof(client_socket->server_addr));
  int sock_fd = socket(AF_LOCAL, SOCK_DGRAM, 0);

  if (sock_fd < 0) {
    return errno;
  }

  struct sockaddr_un client_addr;
  int status = init_client_addr(&client_addr);

  if (status != BRUCE_OK) {
    goto fail;
  }

  int ret = bind(sock_fd, (struct sockaddr *) &client_addr,
                 sizeof(client_addr));

  if (ret < 0) {
    status = errno;
    goto fail;
  }

  if ((unlink(client_addr.sun_path) < 0) && (errno != ENOENT)) {
    status = errno;
    goto fail;
  }

  client_socket->server_addr.sun_family = AF_LOCAL;
  strncpy(client_socket->server_addr.sun_path, server_path,
          sizeof(client_socket->server_addr.sun_path));

  if (strcmp(server_path, client_socket->server_addr.sun_path)) {
    status = BRUCE_SERVER_SOCK_PATH_TOO_LONG;
    goto fail;
  }

  client_socket->sock_fd = sock_fd;
  return status;

fail:
  close(sock_fd);
  return status;
}

int EXPORT_SYM bruce_client_socket_send(
    const bruce_client_socket_t *client_socket, const void *msg,
    size_t msg_size) {
  assert(client_socket);
  assert(msg);
  assert(msg_size);
  ssize_t ret = sendto(client_socket->sock_fd, msg, msg_size, 0,
      (const struct sockaddr *) &client_socket->server_addr,
      sizeof(client_socket->server_addr));
  return (ret < 0) ? errno : BRUCE_OK;
}

void EXPORT_SYM bruce_client_socket_close(
    bruce_client_socket_t *client_socket) {
  assert(client_socket);

  if (client_socket->sock_fd >= 0) {
    close(client_socket->sock_fd);
    client_socket->sock_fd = -1;
  }
}
