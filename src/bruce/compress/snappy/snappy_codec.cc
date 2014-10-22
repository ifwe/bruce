/* <bruce/compress/snappy/snappy_codec.cc>

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

   Implements <bruce/compress/snappy/snappy_codec.h>.
 */

#include <bruce/compress/snappy/snappy_codec.h>

#include <cassert>
#include <memory>
#include <mutex>
#include <string>

#include <boost/lexical_cast.hpp>

#include <bruce/compress/snappy/lib_snappy.h>
#include <server/counter.h>

using namespace Bruce;
using namespace Bruce::Compress;
using namespace Bruce::Compress::Snappy;

SERVER_COUNTER(SnappyBufferTooSmallError);
SERVER_COUNTER(SnappyInvalidInputError);
SERVER_COUNTER(SnappyUnknownError);

static void CheckSnappyStatus(snappy_status status,
    const char *snappy_function_name) {
  assert(snappy_function_name);

  if (status == SNAPPY_OK) {
    return;
  }

  std::string msg("Function ");
  msg += snappy_function_name;
  msg += " reported ";

  switch (status) {
    case SNAPPY_INVALID_INPUT: {
      SnappyInvalidInputError.Increment();
      msg += "invalid input";
      break;
    }
    case SNAPPY_BUFFER_TOO_SMALL: {
      SnappyBufferTooSmallError.Increment();
      msg += "buffer too small";
      break;
    }
    default: {
      SnappyUnknownError.Increment();
      msg += "unknown error ";
      msg += boost::lexical_cast<std::string>(status);
      break;
    }
  }

  throw TCompressionCodecApi::TError(msg.c_str());
}

static std::mutex SingletonInitMutex;

static std::unique_ptr<const TSnappyCodec> Singleton;

const TSnappyCodec &TSnappyCodec::The() {
  if (!Singleton) {
    std::lock_guard<std::mutex> lock(SingletonInitMutex);

    if (!Singleton) {
      Singleton.reset(new TSnappyCodec);
    }
  }

  return *Singleton;
}

size_t TSnappyCodec::ComputeCompressedResultBufSpace(
    const void * /*uncompressed_data*/, size_t uncompressed_size) const {
  assert(this);
  return Lib.snappy_max_compressed_length(uncompressed_size);
}

size_t TSnappyCodec::Compress(const void *input_buf, size_t input_buf_size,
    void *output_buf, size_t output_buf_size) const {
  assert(this);
  CheckSnappyStatus(Lib.snappy_compress(
      reinterpret_cast<const char *>(input_buf), input_buf_size,
      reinterpret_cast<char *>(output_buf), &output_buf_size),
                    "snappy_compress()");

  /* Return true size of compressed output (set by above library call). */
  return output_buf_size;
}

size_t TSnappyCodec::ComputeUncompressedResultBufSpace(
    const void *compressed_data, size_t compressed_size) const {
  assert(this);
  size_t result = 0;
  CheckSnappyStatus(Lib.snappy_uncompressed_length(
      reinterpret_cast<const char *>(compressed_data), compressed_size,
      &result),
                    "snappy_uncompressed_length()");
  return result;
}

size_t TSnappyCodec::Uncompress(const void *input_buf, size_t input_buf_size,
    void *output_buf, size_t output_buf_size) const {
  assert(this);
  CheckSnappyStatus(Lib.snappy_uncompress(
      reinterpret_cast<const char *>(input_buf), input_buf_size,
      reinterpret_cast<char *>(output_buf), &output_buf_size),
                    "snappy_uncompress()");

  /* Return true size of uncompressed output (set by above library call). */
  return output_buf_size;
}

TSnappyCodec::TSnappyCodec()
    : Lib(*TLibSnappy::The()) {
}
