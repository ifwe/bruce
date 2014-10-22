/* <bruce/compress/get_compression_codec.h>

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

   Function to get compression codec.
 */

#pragma once

#include <bruce/compress/compression_codec_api.h>
#include <bruce/conf/compression_type.h>

namespace Bruce {

  namespace Compress {

    /* Return a pointer to the compression codec singleton for 'type', or
       nullptr if 'type' specifies "no compression". */
    const TCompressionCodecApi *GetCompressionCodec(
        Conf::TCompressionType type);

  }  // Compress

}  // Bruce
