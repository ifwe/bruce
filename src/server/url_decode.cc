/* <server/url_decode.cc>

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

   Implements <server/url_decode.h>.
 */

#include <server/url_decode.h>

#include <cstring>
#include <cctype>

#include <iostream>

using namespace std;

using namespace Base;
using namespace Server;

// TODO: This to hex conversion I feel could be heavily optimized.
static inline unsigned char HexToNum(const char *c_in, const char *start) {
  unsigned char c = tolower(*c_in);

  if(!isxdigit(c)) {
    throw TUrlDecodeError(c_in - start,
        "Expected a hex digit but found something else.");
  }

  unsigned char res = 0;

  if (c >= '0' && c <= '9') {
    res = c - '0';
  } else if (c >= 'a' && c <= 'f') {
    res = c - 'a' + 10;
  } else {
    // TODO: Change to a runtime assertion for executing web server.
    assert(false);
  }

  assert(res < 16);
  return res;
}

void Server::UrlDecode(const TPiece<const char> &in, std::string &out) {
  assert(&in);
  assert(&out);

  // If the in string is empty, clear out and no-op.
  if(!in) {
    out.clear();
    return;
  }

  // Copy the contents of the piece to a non-const buffer
  char *str = new char[in.GetSize()];
  memcpy(str, in.GetStart(), in.GetSize());

  // Convert the string character by character until the whole thing is decoded
  char *store_csr = str;
  const char *read_csr = in.GetStart();

  while(read_csr < in.GetLimit()) {
    if(*read_csr == '+') {
      *store_csr = ' ';
      ++read_csr;
      ++store_csr;
    } else if(*read_csr != '%') {
      *store_csr = *read_csr;
      ++read_csr;
      ++store_csr;
    } else {
      ++read_csr;

      if(read_csr + 2 > in.GetLimit()) {
        throw TUrlDecodeError(read_csr - in.GetStart(),
            "Expected two hex digits, but found end of stream");
      }

      unsigned char buf = HexToNum(read_csr, in.GetStart()) * 16;
      ++read_csr;
      buf += HexToNum(read_csr, in.GetStart());
      ++read_csr;
      *store_csr = buf;
      ++store_csr;
    }
  }

  // convert the temporary buffer into a std::string
  try {
    out.assign(str, store_csr - str);
  } catch (...) {
    delete [] str;
    throw;
  }

  // Clean up the temporary buffer
  delete [] str;
}
