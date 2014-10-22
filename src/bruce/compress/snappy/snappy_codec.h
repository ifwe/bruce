/* <bruce/compress/snappy/snappy_codec.h>

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

   Snappy compression codec.
 */

#pragma once

#include <cstddef>

#include <base/no_copy_semantics.h>
#include <bruce/compress/compression_codec_api.h>

namespace Bruce {

  namespace Compress {

    namespace Snappy {

      class TLibSnappy;

      class TSnappyCodec final : public TCompressionCodecApi {
        NO_COPY_SEMANTICS(TSnappyCodec);

        public:
        static const TSnappyCodec &The();  // singleton accessor

        virtual ~TSnappyCodec() noexcept { }

        virtual size_t ComputeCompressedResultBufSpace(
            const void *uncompressed_data,
            size_t uncompressed_size) const override;

        virtual size_t Compress(const void *input_buf, size_t input_buf_size,
            void *output_buf, size_t output_buf_size) const override;

        virtual size_t ComputeUncompressedResultBufSpace(
            const void *compressed_data,
            size_t compressed_size) const override;

        virtual size_t Uncompress(const void *input_buf, size_t input_buf_size,
            void *output_buf, size_t output_buf_size) const override;

        private:
        TSnappyCodec();

        const TLibSnappy &Lib;
      };  // TSnappyCodec

    }  // Snappy

  }  // Compress

}  // Bruce
