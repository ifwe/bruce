/* <server/url_decode.h>

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

   Decodes an encoded url.
 */

#include <cassert>
#include <stdexcept>
#include <string>

#include <base/piece.h>

namespace Server {

  // TODO: use Base::TError
  class TUrlDecodeError : public std::runtime_error {
    public:
    TUrlDecodeError(unsigned int offset, const char *msg)
        : runtime_error(msg), Offset(offset) {
    }

    unsigned int GetOffset() const {
      assert(this);
      return Offset;
    }

    private:
    unsigned int Offset;
  };  // TUrlDecodeError

  /* Decode a urlencoded block of text. */
  void UrlDecode(const Base::TPiece<const char> &in, std::string &out);

}  // Server
