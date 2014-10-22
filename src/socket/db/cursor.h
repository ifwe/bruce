/* <socket/db/cursor.h>

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

   An RAII wrapper around getaddrinfo.
 */

#pragma once

#include <cassert>
#include <netdb.h>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <socket/address.h>

namespace Socket {

  namespace Db {

    /* An RAII wrapper around getaddrinfo. */
    class TCursor {
      NO_COPY_SEMANTICS(TCursor);

      public:
      /* Iterate over address for the given node and service.
         See getadddrinfo() for more information. */
      TCursor(const char *node, const char *serv, int family = AF_UNSPEC,
          int socktype = 0, int protocol = 0, int flags = 0);

      /* Calls freeaddinfo(). */
      ~TCursor();

      /* True iff. we have a current address. */
      operator bool() const {
        assert(this);
        TryFreshen();
        return Csr != 0;
      }

      /* The current address. */
      const TAddress &operator*() const {
        assert(this);
        Freshen();
        return Address;
      }

      /* The current address. */
      const TAddress *operator->() const {
        assert(this);
        Freshen();
        return &Address;
      }

      /* Move to the next address, if any. */
      TCursor &operator++() {
        assert(this);
        Freshen();
        Csr = 0;
        return *this;
      }

      /* Opens a socket compatible with the current host. */
      Base::TFd NewCompatSocket() const;

      /* Go back to the first address, if any. */
      TCursor &Rewind() {
        assert(this);
        Csr = 0;
        Next = First;
        return *this;
      }

      private:
      /* Make sure we have a current address. */
      void Freshen() const {
        assert(this);
        TryFreshen();
        assert(Csr);
      }

      /* Try to have a current address. */
      void TryFreshen() const;

      /* The result of getaddrinfo(). */
      addrinfo *First;

      /* Our current position in the linked list returned by getaddrinfo().
         Null if we've reached the end of the list or if we've invalidated our
         current position. */
      mutable addrinfo *Csr;

      /* The pointer after Csr, if any.
         Null if we've reached the end of the list. */
      mutable addrinfo *Next;

      /* The address pointed to by Csr, formatted for use. */
      mutable TAddress Address;

    };  // TCursor

  }  // Db

}  // Socket
