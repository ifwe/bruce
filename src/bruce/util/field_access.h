/* <bruce/util/field_access.h>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 Tagged

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

   Utility functions for reading/writing multibyte integer values to/from Kafka
   headers.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#include <arpa/inet.h>
#include <endian.h>

namespace Bruce {

  namespace Util {

    inline uint16_t ReadUint16FromHeader(const void *src) {
      uint16_t header_data;
      std::memcpy(&header_data, src, sizeof(header_data));
      return ntohs(header_data);
    }

    inline uint32_t ReadUint32FromHeader(const void *src) {
      uint32_t header_data;
      std::memcpy(&header_data, src, sizeof(header_data));
      return ntohl(header_data);
    }

    inline uint64_t ReadUint64FromHeader(const void *src) {
      uint64_t header_data;
      std::memcpy(&header_data, src, sizeof(header_data));
      return be64toh(header_data);
    }

    inline void WriteUint16ToHeader(void *dst, uint16_t value) {
      uint16_t header_data = htons(value);
      std::memcpy(dst, &header_data, sizeof(header_data));
    }

    inline void WriteUint32ToHeader(void *dst, uint32_t value) {
      uint32_t header_data = htonl(value);
      std::memcpy(dst, &header_data, sizeof(header_data));
    }

    inline void WriteUint64ToHeader(void *dst, uint64_t value) {
      uint64_t header_data = htobe64(value);
      std::memcpy(dst, &header_data, sizeof(header_data));
    }

    inline int16_t ReadInt16FromHeader(const void *src) {
      int16_t header_data;
      std::memcpy(&header_data, src, sizeof(header_data));
      return ntohs(header_data);
    }

    inline int32_t ReadInt32FromHeader(const void *src) {
      int32_t header_data;
      std::memcpy(&header_data, src, sizeof(header_data));
      return ntohl(header_data);
    }

    inline int64_t ReadInt64FromHeader(const void *src) {
      int64_t header_data;
      std::memcpy(&header_data, src, sizeof(header_data));
      return be64toh(header_data);
    }

    inline void WriteInt16ToHeader(void *dst, int16_t value) {
      int16_t header_data = htons(value);
      std::memcpy(dst, &header_data, sizeof(header_data));
    }

    inline void WriteInt32ToHeader(void *dst, int32_t value) {
      int32_t header_data = htonl(value);
      std::memcpy(dst, &header_data, sizeof(header_data));
    }

    inline void WriteInt64ToHeader(void *dst, int64_t value) {
      int64_t header_data = htobe64(value);
      std::memcpy(dst, &header_data, sizeof(header_data));
    }

  }  // Util

}  // Bruce
