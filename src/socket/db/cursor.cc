/* <socket/db/cursor.cc>

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

   Implements <socket/db/cursor.h>.
 */

#include <socket/db/cursor.h>

#include <base/zero.h>
#include <socket/db/error.h>

using namespace Base;
using namespace Socket::Db;

TCursor::TCursor(const char *node, const char *serv, int family, int socktype,
    int protocol, int flags) {
  addrinfo hints;
  Zero(hints);
  hints.ai_family = family;
  hints.ai_socktype = socktype;
  hints.ai_protocol = protocol;
  hints.ai_flags = flags;
  Db::IfNe0(getaddrinfo(node, serv, &hints, &First));
  Rewind();
}

TCursor::~TCursor() {
  assert(this);
  freeaddrinfo(First);
}

TFd TCursor::NewCompatSocket() const {
  assert(this);
  Freshen();
  return TFd(socket(Csr->ai_family, Csr->ai_socktype, Csr->ai_protocol));
}

void TCursor::TryFreshen() const {
  assert(this);

  if (!Csr) {
    Csr = Next;

    if (Csr) {
      Next = Next->ai_next;
      Address = *(Csr->ai_addr);
    }
  }
}
