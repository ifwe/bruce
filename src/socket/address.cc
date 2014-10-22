/* <socket/address.cc>

   ----------------------------------------------------------------------------
   Copyright 2010-2013 if(we)

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

   Implements <socket/address.h>.
 */

#include <socket/address.h>

#include <cctype>
#include <string>

#include <arpa/inet.h>
#include <bits/hash_bytes.h>
#include <netdb.h>
#include <unistd.h>

#include <base/no_default_case.h>
#include <base/os_error.h>
#include <base/zero.h>
#include <io/endian.h>
#include <socket/named_unix_socket.h>

using namespace std;
using namespace Base;
using namespace Io;
using namespace Socket;

TAddress::TAddress(TSpecial special, in_port_t port) {
  switch (special) {
    case IPv4Any: {
      Zero(IPv4);
      IPv4.sin_family = AF_INET;
      IPv4.sin_addr.s_addr = static_cast<in_addr_t>(0);
      break;
    }
    case IPv4Loopback: {
      Zero(IPv4);
      IPv4.sin_family = AF_INET;
      IPv4.sin_addr.s_addr = SwapEnds(static_cast<in_addr_t>(0x7f000001));
      break;
    }
    case IPv6Any: {
      Zero(IPv6);
      IPv6.sin6_family = AF_INET6;
      IPv6.sin6_addr = in6addr_any;
      break;
    }
    case IPv6Loopback: {
      Zero(IPv6);
      IPv6.sin6_family = AF_INET6;
      IPv6.sin6_addr = in6addr_loopback;
      break;
    }
    NO_DEFAULT_CASE;
  }
  SetPort(port);
}

TAddress::TAddress(istream &&strm) {
  assert(&strm);

  if (ws(strm).peek() == '!') {
    /* We skipped whitespace and found the mark indicating an unspecified
       address.  This is an easy, early-out for us. */
    strm.ignore();
    Storage.ss_family = AF_UNSPEC;
  } else if (strm.peek() == '@') {
    strm.ignore();
    Storage.ss_family = AF_LOCAL;
    std::string path;
    strm >> path;
    strncpy(Local.sun_path, path.c_str(), sizeof(Local.sun_path));
    Local.sun_path[sizeof(Local.sun_path) - 1] = '\0';
  } else {
    /* Read the IP portion of the address into a temp buffer.  The buffer must
       be big enough for a v6 address with square brackets around it, plus a
       null terminator. */
    static const size_t max_size = NI_MAXHOST + 3;
    char buf[max_size];
    char *csr = buf, *end = buf + max_size;
    bool is_ipv6 = (strm.peek() == '[');

    if (is_ipv6) {
      /* It started with an open-bracket, so it must be v6. */
      strm.ignore();

      for (;;) {
        int c = strm.peek();

        if (c < 0) {
          break;
        }

        if (c == ']') {
          strm.ignore();
          break;
        }

        if (isxdigit(c) || c == '.' || c == ':') {
          if (csr >= end) {
            throw 0; // TODO
          }

          *csr++ = c;
          strm.ignore();
        } else {
          throw 0; // TODO
        }
      }
    } else {

      /* It didn't start with an open-bracket, so it must be v4. */
      for (;;) {
        int c = strm.peek();

        if (isxdigit(c) || c == '.') {
          if (csr >= end) {
            throw 0; // TODO
          }

          *csr++ = c;
          strm.ignore();
        } else {
          break;
        }
      }
    }

    *csr = '\0';
    /* If we're positioned at a colon, read in the port number; otherwise,
       default to port 0. */
    in_port_t port = 0;
    int c = strm.peek();

    if (c == ':') {
      strm.ignore();
      strm >> port;
      port = SwapEnds(port);
    }

    /* Translate the IP. */
    if (is_ipv6) {
      Zero(IPv4);

      if (!inet_pton(AF_INET6, buf, &IPv6.sin6_addr)) {
        throw TOsError(HERE);
      }

      IPv6.sin6_family = AF_INET6;
      IPv6.sin6_port = port;
    } else {
      Zero(IPv6);

      if (!inet_pton(AF_INET, buf, &IPv4.sin_addr)) {
        throw TOsError(HERE);
      }

      IPv4.sin_family = AF_INET;
      IPv4.sin_port = port;
    }
  }
}

bool TAddress::operator==(const TAddress &that) const {
  assert(this);
  bool result = (Storage.ss_family == that.Storage.ss_family);

  if (result) {
    switch (Storage.ss_family) {
      case AF_UNSPEC: {
        break;
      }
      case AF_INET: {
        result = (memcmp(&IPv4.sin_addr.s_addr, &that.IPv4.sin_addr.s_addr,
                         sizeof(IPv4.sin_addr.s_addr)) == 0) &&
                 (IPv4.sin_port == that.IPv4.sin_port);
        break;
      }
      case AF_INET6: {
        result = (memcmp(&IPv6.sin6_addr, &that.IPv6.sin6_addr,
                         sizeof(IPv6.sin6_addr)) == 0) &&
                 (IPv6.sin6_port == that.IPv6.sin6_port);
        break;
      }
      case AF_LOCAL: {
        result = !strcmp(Local.sun_path, that.Local.sun_path);
      }
      NO_DEFAULT_CASE;
    }
  }

  return result;
}

size_t TAddress::GetHash() const {
  assert(this);
  return _Fnv_hash_bytes(&Storage, GetLen(), 0);
}

void TAddress::GetName(
    char *node_buf, size_t node_buf_size,
    char *serv_buf, size_t serv_buf_size, int flags) const {
  assert(this);
  assert(node_buf || !node_buf_size);
  assert(serv_buf || !serv_buf_size);
  Db::IfNe0(getnameinfo(&Generic, GetLen(), node_buf, node_buf_size, serv_buf,
                        serv_buf_size, flags));
}

in_port_t TAddress::GetPort() const {
  assert(this);
  in_port_t result;

  switch (Storage.ss_family) {
    case AF_UNSPEC: {
      result = 0;
      break;
    }
    case AF_INET: {
      result = IPv4.sin_port;
      break;
    }
    case AF_INET6: {
      result = IPv6.sin6_port;
      break;
    }
    NO_DEFAULT_CASE;
  }

  return SwapEnds(result);
}

TAddress &TAddress::SetPort(in_port_t port) {
  assert(this);

  switch (Storage.ss_family) {
    case AF_UNSPEC: {
      break;
    }
    case AF_INET: {
      IPv4.sin_port = SwapEnds(port);
      break;
    }
    case AF_INET6: {
      IPv6.sin6_port = SwapEnds(port);
      break;
    }
    NO_DEFAULT_CASE;
  }

  return *this;
}

const char *TAddress::GetPath() const {
  assert(this);
  const char *result = nullptr;

  switch (Storage.ss_family) {
    case AF_LOCAL: {
      result = Local.sun_path;
      break;
    }
    NO_DEFAULT_CASE;
  }

  return result;
}

TAddress &TAddress::SetPath(const char *path) {
  assert(this);

  switch (Storage.ss_family) {
    case AF_LOCAL: {
      strncpy(Local.sun_path, path, sizeof(Local.sun_path));
      Local.sun_path[sizeof(Local.sun_path) - 1] = '\0';
      if (strcmp(path, Local.sun_path)) {
        THROW_ERROR(TPathTooLong);
      }
      break;
    }
    NO_DEFAULT_CASE;
  }

  return *this;
}

void TAddress::Write(ostream &strm) const {
  assert(this);
  assert(&strm);
  in_port_t port = 0;

  switch (Storage.ss_family) {
    case AF_UNSPEC: {
      strm << '!';
      break;
    }
    case AF_INET: {
      char buf[INET_ADDRSTRLEN];
      if (!inet_ntop(AF_INET, &IPv4.sin_addr, buf, sizeof(buf))) {
        throw TOsError(HERE);
      }
      strm << buf;
      port = IPv4.sin_port;
      break;
    }
    case AF_INET6: {
      char buf[INET6_ADDRSTRLEN];
      if (!inet_ntop(AF_INET6, &IPv6.sin6_addr, buf, sizeof(buf))) {
        throw TOsError(HERE);
      }
      strm << '[' << buf << ']';
      port = IPv6.sin6_port;
      break;
    }
    case AF_LOCAL: {
      strm << '@' << Local.sun_path;
    }
    NO_DEFAULT_CASE;
  }

  if (port) {
    strm << ':' << SwapEnds(port);
  }
}

socklen_t TAddress::GetLen(sa_family_t family) {
  socklen_t result;

  switch (family) {
    case AF_UNSPEC: {
      result = sizeof(sa_family_t);
      break;
    }
    case AF_INET: {
      result = sizeof(IPv4);
      break;
    }
    case AF_INET6: {
      result = sizeof(IPv6);
      break;
    }
    case AF_LOCAL: {
      result = sizeof(Local);
      break;
    }
    NO_DEFAULT_CASE;
  }

  return result;
}

void Socket::Bind(TNamedUnixSocket &socket, const TAddress &address) {
  assert(&socket);
  assert(&address);
  assert(address.GetFamily() == AF_LOCAL);
  string path(address.GetPath());

  /* Make sure socket file doesn't already exist. */
  int ret = unlink(path.c_str());

  if ((ret < 0) && (errno != ENOENT)) {
    IfLt0(ret);
  }

  IfLt0(::bind(socket, address, address.GetLen()));
  socket.Path.swap(path);
}
