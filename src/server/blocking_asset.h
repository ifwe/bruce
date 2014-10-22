/* <server/blocking_asset.h>

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

   A target for shared and exclusive locking, built on pthread read/write
   locking.
 */

#pragma once

#include <cassert>
#include <pthread.h>

#include <base/no_copy_semantics.h>
#include <base/os_error.h>

namespace Server {

  /* A non-reentrant target for read-write synchronization. */
  class TBlockingAsset {
    NO_COPY_SEMANTICS(TBlockingAsset);

    public:
    /* Constructs a new, unlocked asset. */
    TBlockingAsset() {
      Base::TOsError::IfNe0(HERE, pthread_rwlock_init(&RwLock, 0));
    }

    /* Destroys the target.
       The target must not currently be locked by any thread.
       Destroying a locked target has undefined results. */
    ~TBlockingAsset() {
      assert(this);
      pthread_rwlock_destroy(&RwLock);
    }

    void AcquireExclusive() const {
      assert(this);
      Base::TOsError::IfNe0(HERE, pthread_rwlock_wrlock(&RwLock));
    }

    void AcquireShared() const {
      assert(this);
      Base::TOsError::IfNe0(HERE, pthread_rwlock_rdlock(&RwLock));
    }

    void ReleaseExclusive() const {
      assert(this);
      pthread_rwlock_unlock(&RwLock);
    }

    void ReleaseShared() const {
      assert(this);
      pthread_rwlock_unlock(&RwLock);
    }

    private:
    /* The rw-lock for which threads will contend.
       It's mutable so we can lock and unlock a constant target. */
    mutable pthread_rwlock_t RwLock;
  };  // TBlockingAsset

}  // Server
