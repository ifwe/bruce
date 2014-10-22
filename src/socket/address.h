/* <socket/address.h>

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

   A socket address in either the IPv4 or IPv6 family.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <istream>
#include <ostream>
#include <stdexcept>

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <base/error_utils.h>
#include <base/thrower.h>
#include <socket/db/error.h>

namespace Socket {

  class TNamedUnixSocket;

  /* A socket address that may be UNIX domain (AF_LOCAL), or in either the IPv4
     or IPv6 family.  For UNIX domain sockets, Linux-specific abstract names
     are _not_ supported. */
  class TAddress {
    public:
    DEFINE_ERROR(TPathTooLong, std::runtime_error,
                 "UNIX domain socket path too long");

    /* The amount of storage space available in an address. */
    static const socklen_t MaxLen = sizeof(sockaddr_storage);

    /* Special values for address construction. */
    enum TSpecial { IPv4Any, IPv4Loopback, IPv6Any, IPv6Loopback };

    /* The default address is AF_UNSPEC. */
    TAddress() {
      Storage.ss_family = AF_UNSPEC;
    }

    /* Move constructor.  The donor is left as AF_UNSPEC. */
    TAddress(TAddress &&that) {
      that.CopyOut(Storage);
      that.Storage.ss_family = AF_UNSPEC;
    }

    /* Copy constructor. */
    TAddress(const TAddress &that) {
      that.CopyOut(Storage);
    }

    /* Copy construct from a naked sockaddr. */
    explicit TAddress(const sockaddr &sa) {
      memcpy(&Storage, &sa, GetLen(sa.sa_family));
    }

    /* Extract from the given stream.
       An AF_UNSPEC address is represented as a single bang.  ('!')
       An AF_LOCAL address is represented as an '@' symbol followed by a
       pathname (example: @/some/filesystem/path).
       An IPv4 address must be in a.b.c.d format.
       An IPv6 address must be in standard format with [brackets] around it.
       In either case, a port number may be appended, preceded by a colon.
       For example: '1.2.3.4:56' or '[1:2:3:4:5:6:7:8]:90' */
    explicit TAddress(std::istream &&that);

    /* Construct the given special address and port. */
    TAddress(TSpecial special, in_port_t port = 0);

    /* Swaperator. */
    TAddress &operator=(TAddress &&that) {
      assert(this);
      assert(&that);
      std::swap(Storage, that.Storage);
      return *this;
    }

    /* Assignment operator. */
    TAddress &operator=(const TAddress &that) {
      assert(this);
      return *this = TAddress(that);
    }

    /* Assign from a naked sockaddr. */
    TAddress &operator=(const sockaddr &sa) {
      assert(this);
      return *this = TAddress(sa);
    }

    /* Extract from the given stream. */
    TAddress &operator=(std::istream &&strm) {
      assert(this);
      return *this = TAddress(std::move(strm));
    }

    /* Assign the given special address, setting port to 0. */
    TAddress &operator=(TSpecial special) {
      assert(this);
      return *this = TAddress(special);
    }

    /* Cast to a naked sockaddr. */
    operator const sockaddr *() const {
      assert(this);
      return &Generic;
    }

    /* Cast to a modifiable naked sockaddr.  If you modify the structure,
       call Verify() afterward to make sure it's still in good shape. */
    operator sockaddr *() {
      assert(this);
      return &Generic;
    }

    /* True iff. the family, addr, and port fields match. */
    bool operator==(const TAddress &that) const;

    /* True iff. the family, addr, and port fields don't match. */
    bool operator!=(const TAddress &that) const {
      return !(*this == that);
    }

    /* Assign the given special address and port. */
    TAddress &Assign(TSpecial special, in_port_t port = 0) {
      assert(this);
      return *this = TAddress(special, port);
    }

    /* Copy the naked address to the given buffer. */
    void CopyOut(sockaddr_storage &storage) const {
      assert(this);
      assert(&storage);
      memcpy(&storage, &Storage, GetLen());
    }

    /* The family of the address. */
    sa_family_t GetFamily() const {
      assert(this);
      return Storage.ss_family;
    }

    TAddress &SetFamily(sa_family_t family) {
      assert(this);
      Storage.ss_family = family;
      Verify();
      return *this;
    }

    /* The hash of the address. */
    size_t GetHash() const;

    /* The number of bytes in use in the address.
       Use this, along with the naked cast, to make OS calls like bind(). */
    socklen_t GetLen() const {
      assert(this);
      return GetLen(Storage.ss_family);
    }

    /* Format the address for human presenatation as 'name of node' and 'name
       of service'.  The two buffers should be of sizes NI_MAXHOST and
       NI_MAXSERV, respecitvely.  See the OS function getnameinfo() for
       information about the flags. */
    void GetName(char *node_buf, size_t node_buf_size, char *serv_buf,
        size_t serv_buf_size, int flags = 0) const;

    /* The port number in host order.
       The port number of an AF_UNSPEC address is always 0. */
    in_port_t GetPort() const;

    /* Set the port number.  The argument must be in host order.
       An address in the AF_UNSPEC family will ignore this function. */
    TAddress &SetPort(in_port_t port);

    /* Get the path of a UNIX domain socket. */
    const char *GetPath() const;

    /* Set the path of a UNIX domain socket. */
    TAddress &SetPath(const char *path);

    /* If the address is of a supported family, this function does nothing;
       otherwise, it resets the address to the default-constructed state and
       throws an exception. */
    void Verify() {
      assert(this);

      switch (Storage.ss_family) {
        case AF_UNSPEC:
        case AF_INET:
        case AF_INET6:
        case AF_LOCAL: {
          break;
        }
        default: {
          Reset();
          throw Db::TError(EAI_ADDRFAMILY);
        }
      }
    }

    /* Insert the address onto the stream in a format which can be extracted
       later by the extraction constructor.  */
    void Write(std::ostream &strm) const;

    /* Return to the default-constructed state. */
    TAddress &Reset() {
      assert(this);
      return *this = TAddress();
    }

    /* The number of bytes used by an address of the given family. */
    static socklen_t GetLen(sa_family_t family);

    private:
    /* A union of all supported sockaddr families.
       This union can be discriminated by examining Storage.ss_family. */
    union {
      sockaddr_storage Storage;
      sockaddr_in IPv4;
      sockaddr_in6 IPv6;
      sockaddr_un Local;
      sockaddr Generic;
    };
  };  // TAddress

  /* A version of accept() using TAddress. */
  inline int Accept(int socket, TAddress &address) {
    assert(&address);
    int result;
    socklen_t len = TAddress::MaxLen;
    Base::IfLt0(result = accept(socket, address, &len));
    address.Verify();
    return result;
  }

  /* A version of bind() using TAddress. */
  inline void Bind(int socket, const TAddress &address) {
    assert(&address);
    Base::IfLt0(bind(socket, address, address.GetLen()));
  }

  /* A version of bind() using TAddress that is specifically intended for
     UNIX domain sickets. */
  void Bind(TNamedUnixSocket &socket, const TAddress &address);

  /* A version of connect() using TAddress. */
  inline void Connect(int socket, const TAddress &address) {
    assert(&address);
    Base::IfLt0(connect(socket, address, address.GetLen()));
  }

  /* A version of getpeername() using TAddress. */
  inline TAddress GetPeerName(int socket) {
    TAddress result;
    socklen_t len = TAddress::MaxLen;
    Base::IfLt0(getpeername(socket, result, &len));
    result.Verify();
    return result;
  }

  /* A version of getsockname() using TAddress. */
  inline TAddress GetSockName(int socket) {
    TAddress result;
    socklen_t len = TAddress::MaxLen;
    Base::IfLt0(getsockname(socket, result, &len));
    result.Verify();
    return result;
  }

  /* A version of recvfrom() using TAddress. */
  inline size_t RecvFrom(int socket, void *buffer, size_t max_size, int flags,
      TAddress &address) {
    assert(&address);
    ssize_t result;
    socklen_t len = TAddress::MaxLen;
    Base::IfLt0(result = recvfrom(socket, buffer, max_size, flags, address,
                                  &len));
    address.Verify();
    return result;
  }

  /* A version of sendto() using TAddress. */
  inline size_t SendTo(int socket, const void *buffer, size_t max_size,
      int flags, const TAddress &address) {
    assert(&address);
    ssize_t result;
    Base::IfLt0(result = sendto(socket, buffer, max_size, flags, address,
                                address.GetLen()));
    return result;
  }

  /* A async version of accept() using TAddress.
     Returns false iff. it would block. */
  inline bool TryAccept(int socket, int &new_socket, TAddress &address) {
    assert(&new_socket);
    assert(&address);
    socklen_t len = TAddress::MaxLen;
    int result = accept(socket, address, &len);

    if (result < 0) {
      if (errno == EWOULDBLOCK) {
        return false;
      }

      Base::ThrowSystemError(errno);
    }

    address.Verify();
    new_socket = result;
    return true;
  }

  /* An async version of connect() using TAddress.
     Returns false iff. it would block. */
  inline bool TryConnect(int socket, const TAddress &address) {
    assert(&address);

    if (connect(socket, address, address.GetLen()) < 0) {
      if (errno == EWOULDBLOCK) {
        return false;
      }

      Base::ThrowSystemError(errno);
    }

    return true;
  }

  /* An async version of recvfrom() using TAddress.
     Returns false iff. it would block. */
  inline size_t TryRecvFrom(int socket, void *buffer, size_t max_size,
      int flags, TAddress &address, size_t &size) {
    assert(&address);
    assert(&size);
    socklen_t len = TAddress::MaxLen;
    ssize_t result = recvfrom(socket, buffer, max_size, flags, address, &len);

    if (result < 0) {
      if (errno == EWOULDBLOCK) {
        return false;
      }

      Base::ThrowSystemError(errno);
    }

    address.Verify();
    size = result;
    return true;
  }

  /* An async version of sendto() using TAddress.  Returns false iff. it would
     block. */
  inline bool TrySendTo(int socket, const void *buffer, size_t max_size,
      int flags, const TAddress &address, size_t &size) {
    assert(&address);
    assert(&size);

    ssize_t result = sendto(socket, buffer, max_size, flags, address,
        address.GetLen());

    if (result < 0) {
      if (errno == EWOULDBLOCK) {
        return false;
      }

      Base::ThrowSystemError(errno);
    }

    size = result;
    return true;
  }

  /* A standard stream extractor for Socket::TAddress. */
  inline std::istream &operator>>(std::istream &strm, TAddress &that) {
    assert(&that);
    that = std::move(strm);
    return strm;
  }

  /* A standard stream inserter for Socket::TAddress. */
  inline std::ostream &operator<<(std::ostream &strm, const TAddress &that) {
    assert(&that);
    that.Write(strm);
    return strm;
  }

}  // Socket

namespace std {

  /* Standard hasher for Socket::TAddress. */
  template <>
  struct hash<Socket::TAddress> {
    inline size_t operator()(const Socket::TAddress &that) const {
      assert(&that);
      return that.GetHash();
    }
  };  // hash

}
