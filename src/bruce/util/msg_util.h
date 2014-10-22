/* <bruce/util/msg_util.h>

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

   Message-related utilities.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <vector>

#include <bruce/msg.h>
#include <capped/reader.h>

namespace Bruce {

  namespace Util {

    /* Return the total combined size in bytes of the keys and values of all
       messages in 'batch'. */
    size_t GetDataSize(const std::list<TMsg::TPtr> &batch);

    /* Write key of 'msg' into 'dst' starting at offset 'offset'.  Increase
       size of 'dst' if necessary to make space for key.  This function makes
       _no_ assumptions about the size of 'dst' on entry, and will never shrink
       'dst'.  It will never grow 'dst' any more than the minimum amount
       required to hold the entire output. */
    void WriteKey(std::vector<uint8_t> &dst, size_t offset, const TMsg &msg);

    /* This version of WriteKey() is similar to the above except that the key
       is written into the memory pointed to by 'dst'.  Here, it is assumed
       that buffer 'dst' contains enough space for the entire key.  To find
       out how much space must be allocated for 'dst', one can call
       msg.GetKeySize(). */
    inline void WriteKey(uint8_t *dst, const TMsg &msg) {
      assert(dst);
      Capped::TReader(&msg.GetKeyAndValue()).Read(dst, msg.GetKeySize());
    }

    /* Write value of 'msg' into 'dst' starting at offset 'offset'.  Increase
       size of 'dst' if necessary to make space for value.  This function makes
       _no_ assumptions about the size of 'dst' on entry, and will never shrink
       'dst'.  It will never grow 'dst' any more than the minimum amount
       required to hold the entire output.  Return size in bytes of written
       value, not including 'offset' value passed in.

       This function becomes trivially simple when we get rid of the old output
       format. */
    size_t WriteValue(std::vector<uint8_t> &dst, size_t offset,
        const TMsg &msg, bool add_timestamp, bool use_old_output_format);

    /* This version of WriteValue() is similar to the above except that the
       value is written into the memory pointed to by 'dst'.  Here, it is
       assumed that buffer 'dst' contains enough space for the entire value.
       To find out how much space must be allocated for 'dst', one can call
       ComputeValueSize() below. */
    void WriteValue(uint8_t *dst, const TMsg &msg, bool add_timestamp,
        bool use_old_output_format);

    /* This is a temporary hack.  It will go away when we get rid of the old
       output format. */
    size_t ComputeValueSize(const TMsg &msg, bool add_timestamp,
        bool use_old_output_format);

  }  // Util

}  // Bruce
