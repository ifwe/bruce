/* <base/piece.cc>

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

   Implements <base/piece.h>
 */

#include <base/piece.h>

unsigned char *Base::BuildCStr(const TPiece<const unsigned char> &piece) {
  unsigned char *out_str = new unsigned char[piece.GetSize() + 1];
  memcpy(out_str, piece.GetStart(), piece.GetSize());
  out_str[piece.GetSize()] = 0;
  return out_str;
}

char *Base::BuildCStr(const TPiece<const char> &piece) {
  char *out_str = new char[piece.GetSize() + 1];
  memcpy(out_str, piece.GetStart(), piece.GetSize());
  out_str[piece.GetSize()] = 0;
  return out_str;
}

std::ostream &Base::operator<<(std::ostream &strm,
    const Base::TPiece<const char> &piece) {
  strm.write(piece.GetStart(), piece.GetSize());
  return strm;
}
