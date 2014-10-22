/* <bruce/compress/compression_codec_api.h>

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

   Compression codec interface class.
 */

#pragma once

#include <cstddef>
#include <stdexcept>

#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace Compress {

    /* Compression codec base class.  Subclasses should be completely
       stateless singletons, and their methods may be called concurrently by
       multiple threads. */
    class TCompressionCodecApi {
      NO_COPY_SEMANTICS(TCompressionCodecApi);

      public:
      /* Exception base class for errors reported by compression codec. */
      class TError : public std::runtime_error {
        public:
        virtual ~TError() noexcept { }

        explicit TError(const char *what_arg)
            : std::runtime_error(what_arg) {
        }
      };  // TError

      virtual ~TCompressionCodecApi() noexcept { }

      /* Return the maximum compressed size in bytes of 'uncompressed_size'
         bytes of data in buffer 'uncompressed_data'.  Throw TError on error.
       */
      virtual size_t ComputeCompressedResultBufSpace(
          const void *uncompressed_data,
          size_t uncompressed_size) const = 0;

      /* Compress data in 'input_buf' of size 'input_buf_size' bytes.  Place
         compressed result in 'output_buf' of size 'output_buf_size' bytes.
         Return actual size in bytes of compressed data, which will be at most
         'output_buf_size'.  Throw TError on error.  Call
         ComputeCompressedResultBufSpace() to determine how many bytes to
         allocate for 'output_buf'. */
      virtual size_t Compress(const void *input_buf, size_t input_buf_size,
          void *output_buf, size_t output_buf_size) const = 0;

      /* Return the maximum uncompressed size in bytes of 'compressed_size'
         bytes of data in buffer 'compressed_data'.  Throw TError on error. */
      virtual size_t ComputeUncompressedResultBufSpace(
          const void *compressed_data, size_t compressed_size) const = 0;

      /* Uncompress data in 'input_buf' of size 'input_buf_size' bytes.  Place
         uncompressed result in 'output_buf' of size 'output_buf_size' bytes.
         Return actual size in bytes of uncompressed data, which will be at
         most 'output_buf_size'.  Throw TError on error.  Call
         ComputeUncompressedResultBufSpace() to determine how many bytes to
         allocate for 'output_buf'. */
      virtual size_t Uncompress(const void *input_buf, size_t input_buf_size,
          void *output_buf, size_t output_buf_size) const = 0;

      protected:
      TCompressionCodecApi() = default;
    };  // TCompressionCodecApi

  }  // Compress

}  // Bruce
