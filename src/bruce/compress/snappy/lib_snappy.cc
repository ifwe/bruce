/* <bruce/compress/snappy/lib_snappy.cc>

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

   Implements <bruce/compress/snappy/lib_snappy.h>.
 */

#include <bruce/compress/snappy/lib_snappy.h>

#include <dlfcn.h>

using namespace Bruce;
using namespace Bruce::Compress;
using namespace Bruce::Compress::Snappy;

const TLibSnappy *TLibSnappy::The() {
  if (!LoadAttempted) {
    LoadAttempted = true;
    Singleton.reset(new TLibSnappy);  // throw on failure
  }

  return Singleton.get();
}

TLibSnappy::TLibSnappy()
    : TDynamicLib(LibName, RTLD_LAZY),
      fn_snappy_compress(LoadSym<t_fn_snappy_compress>("snappy_compress")),
      fn_snappy_uncompress(
          LoadSym<t_fn_snappy_uncompress>("snappy_uncompress")),
      fn_snappy_max_compressed_length(
          LoadSym<t_fn_snappy_max_compressed_length>(
              "snappy_max_compressed_length")),
      fn_snappy_uncompressed_length(
          LoadSym<t_fn_snappy_uncompressed_length>(
              "snappy_uncompressed_length")),
      fn_snappy_validate_compressed_buffer(
          LoadSym<t_fn_snappy_validate_compressed_buffer>(
              "snappy_validate_compressed_buffer")) {
}

const char TLibSnappy::LibName[] = "libsnappy.so.1";

std::unique_ptr<const TLibSnappy> TLibSnappy::Singleton;

bool TLibSnappy::LoadAttempted = false;
