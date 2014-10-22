/* <bruce/compress/snappy/lib_snappy.h>

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

   Wrapper class for Snappy compression library.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <memory>

#include <snappy-c.h>

#include <base/dynamic_lib.h>
#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace Compress {

    namespace Snappy {

      /* Wrapper class for Snappy compression library.  Constructor dynamically
         loads library and the symbols for its C language API. */
      class TLibSnappy final : public Base::TDynamicLib {
        NO_COPY_SEMANTICS(TLibSnappy);

        public:
        /* Singleton accessor.  On the first call, the behavior is as follows:

               Attempt to load library and its symbols.  On failure, throw
               TDynamicLib::TLibLoadError or TDynamicLib::TSymLoadError.  On
               success, return a pointer to the newly constructed TLibSnappy
               singleton.

           On subsequent calls, the behavior is as follows:

               If the first call failed, return nullptr.  Otherwise, return a
               pointer to the TLibSnappy singleton.  In either case, the method
               is guaranteed not to throw.
         */
        static const TLibSnappy *The();

        virtual ~TLibSnappy() noexcept { }

        snappy_status snappy_compress(const char *input, size_t input_length,
            char *compressed, size_t *compressed_length) const {
          return fn_snappy_compress(input, input_length, compressed,
                                    compressed_length);
        }

        snappy_status snappy_uncompress(const char *compressed,
            size_t compressed_length, char *uncompressed,
            size_t *uncompressed_length) const {
          return fn_snappy_uncompress(compressed, compressed_length,
              uncompressed, uncompressed_length);
        }

        size_t snappy_max_compressed_length(size_t source_length) const {
          return fn_snappy_max_compressed_length(source_length);
        }

        snappy_status snappy_uncompressed_length(const char *compressed,
            size_t compressed_length, size_t *result) const {
          return fn_snappy_uncompressed_length(compressed, compressed_length,
                                               result);
        }

        snappy_status snappy_validate_compressed_buffer(const char *compressed,
            size_t compressed_length) const {
          return fn_snappy_validate_compressed_buffer(compressed,
              compressed_length);
        }

        private:
        TLibSnappy();  // called by singleton accessor

        typedef snappy_status (*t_fn_snappy_compress)(const char *input,
            size_t input_length, char *compressed, size_t *compressed_length);

        typedef snappy_status (*t_fn_snappy_uncompress)(const char *compressed,
            size_t compressed_length, char *uncompressed,
            size_t *uncompressed_length);

        typedef size_t (*t_fn_snappy_max_compressed_length)(
            size_t source_length);

        typedef snappy_status (*t_fn_snappy_uncompressed_length)(
            const char *compressed, size_t compressed_length, size_t *result);

        typedef snappy_status (*t_fn_snappy_validate_compressed_buffer)(
            const char *compressed, size_t compressed_length);

        static const char LibName[];

        static std::unique_ptr<const TLibSnappy> Singleton;

        static bool LoadAttempted;

        t_fn_snappy_compress fn_snappy_compress;

        t_fn_snappy_uncompress fn_snappy_uncompress;

        t_fn_snappy_max_compressed_length fn_snappy_max_compressed_length;

        t_fn_snappy_uncompressed_length fn_snappy_uncompressed_length;

        t_fn_snappy_validate_compressed_buffer
            fn_snappy_validate_compressed_buffer;
      };  // TLibSnappy

    }  // Snappy

  }  // Compress

}  // Bruce
