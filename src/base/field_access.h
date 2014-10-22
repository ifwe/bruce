/* <base/field_access.h>

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

   Utility functions for reading/writing multibyte integer values to/from
   headers that store fields in network byte order.
 */

#pragma once

#include <stdint.h>

#include <arpa/inet.h>
#include <endian.h>

/* It should be possible to compile everything in here with a C compiler.
   That's why there are C-style casts and no namespaces below. */

#ifdef __cplusplus
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif

static inline uint16_t ReadUint16FromHeader(const void *src) {
  return ntohs(*((const uint16_t *) src));
}

static inline int16_t ReadInt16FromHeader(const void *src) {
  return ntohs(*((const int16_t *) src));
}

static inline uint32_t ReadUint32FromHeader(const void *src) {
  return ntohl(*((const uint32_t *) src));
}

static inline int32_t ReadInt32FromHeader(const void *src) {
  return ntohl(*((const int32_t *) src));
}

static inline uint64_t ReadUint64FromHeader(const void *src) {
  return be64toh(*((const uint64_t *) src));
}

static inline int64_t ReadInt64FromHeader(const void *src) {
  return be64toh(*((const int64_t *) src));
}

static inline void WriteUint16ToHeader(void *dst, uint16_t value) {
  *((uint16_t *) dst) = htons(value);
}

static inline void WriteInt16ToHeader(void *dst, int16_t value) {
  *((int16_t *) dst) = htons(value);
}

static inline void WriteUint32ToHeader(void *dst, uint32_t value) {
  *((uint32_t *) dst) = htonl(value);
}

static inline void WriteInt32ToHeader(void *dst, int32_t value) {
  *((int32_t *) dst) = htonl(value);
}

static inline void WriteUint64ToHeader(void *dst, uint64_t value) {
  *((uint64_t *) dst) = htobe64(value);
}

static inline void WriteInt64ToHeader(void *dst, int64_t value) {
  *((int64_t *) dst) = htobe64(value);
}

#ifdef __cplusplus
#pragma GCC diagnostic pop
#endif
