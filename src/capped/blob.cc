/* <capped/blob.cc>

   ----------------------------------------------------------------------------
   Copyright 2013 if(we)

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

   Implements <capped/blob.h>.
 */

#include <capped/blob.h>

using namespace Capped;

size_t TBlob::DoGetDataInFirstBlock(char *&data) const {
  assert(this);

  if (FirstBlock == nullptr) {
    data = nullptr;
    return 0;
  }

  data = &FirstBlock->Data[0];
  return (FirstBlock->NextBlock == nullptr) ? LastBlockSize : GetBlockSize();
}
