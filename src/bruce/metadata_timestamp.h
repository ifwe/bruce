/* <bruce/metadata_timestamp.h>

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

   Class for tracking when bruce last fetched metadata.
 */

#pragma once

#include <cstdint>
#include <mutex>

#include <base/no_copy_semantics.h>

namespace Bruce {

  /* Keeps track of when bruce last updated its metadata.  This info is
     reported by Mongoose, so thread synchronization is necessary. */
  class TMetadataTimestamp {
    NO_COPY_SEMANTICS(TMetadataTimestamp);

    public:
    /* Trivial constructor. */
    TMetadataTimestamp()
        : LastUpdateTime(0),
          LastModifiedTime(0) {
    }

    /* Called by router thread when it updates its metadata. */
    void RecordUpdate(bool modified);

    /* Called by Mongoose thread when it needs to report how recent the
       metadata is.  Results are provided in milliseconds since the epoch. */
    void GetTimes(uint64_t &last_update_time,
                  uint64_t &last_modified_time) const;

    private:
    /* Protects 'LastUpdateTime' and 'LastModifiedTime' from concurrent access
       by Mongoose and the router thread. */
    mutable std::mutex Mutex;

    /* Updated with current time in UTC whenever router thread gets new
       metadata (regardless of whether metadata is unchanged because new and
       old metadata are identical). */
    uint64_t LastUpdateTime;

    /* Updated with current time in UTC whenever router thread gets new
       metadata and replaces its current metadata with the new metadata. */
    uint64_t LastModifiedTime;
  };  // TMetadataTimestamp

}  // Bruce
