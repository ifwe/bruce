/* <server/exclusive_lock.h>

   ----------------------------------------------------------------------------
   Copyright 2010-2013 if(we)

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

   An RAII object for holding a exclusive lock on an asset.
 */

#pragma once

#include <cassert>

#include <base/no_copy_semantics.h>

namespace Server {

  /* An RAII object for holding a exclusive lock on an asset. */
  template <typename TAsset>
  class TExclusiveLock {
    NO_COPY_SEMANTICS(TExclusiveLock);

    public:
    /* Will not return until the lock is granted. */
    TExclusiveLock(const TAsset &asset)
        : Asset(asset) {
      assert(&asset);
      asset.AcquireExclusive();
    }

    /* Releases the lock. */
    ~TExclusiveLock() {
      assert(this);
      Asset.ReleaseExclusive();
    }

    private:
    /* The asset we're locking. */
    const TAsset &Asset;
  };  // TExclusiveLock

}  // Server
