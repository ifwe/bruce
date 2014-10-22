/* <capped/pool.test.cc>
  
   ----------------------------------------------------------------------------
   Copyright 2013 if(we)

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

   Unit test for <capped/pool.h>.
 */

#include <capped/pool.h>
  
#include <cstdint>
#include <memory>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Base;
using namespace Capped;

namespace {

  /* A point in 3D space. */
  class TPoint {
    public:
    /* Default to the origin. */
    TPoint()
        : X(0), Y(0), Z(0) {}
  
    /* Our coordinates. */
    int64_t X, Y, Z;
  
    /* Allocate memory from our pool. */
    static void *operator new(size_t) {
      return Pool.Alloc();
    }
  
    /* Return memory to our pool. */
    static void operator delete(void *ptr, size_t) {
      Pool.Free(ptr);
    }
  
    /* The storage pool for points. */
    static TPool Pool;
  };  // TPoint

  /* A very constrained pool from which to allocate. */
  TPool TPoint::Pool(sizeof(TPoint), 3, TPool::TSync::Unguarded);
  
  /* Try to construct a new point on the heap and return success/failure.
     Either way, don't bother to keep the point around. */
  static bool TryNewPoint() {
    bool success;

    try {
      unique_ptr<TPoint> p(new TPoint);
      success = true;
    } catch (const TMemoryCapReached &) {
      success = false;
    }

    return success;
  }

  /* The fixture for testing class TPool. */
  class TPoolTest : public ::testing::Test {
    protected:
    TPoolTest() {
    }

    virtual ~TPoolTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TPoolTest

  TEST_F(TPoolTest, Typical) {
    unique_ptr<TPoint>
        a(new TPoint),
        b(new TPoint),
        c(new TPoint);
    ASSERT_FALSE(TryNewPoint());
    a.reset();
    ASSERT_TRUE(TryNewPoint());
    a.reset(new TPoint);
    ASSERT_FALSE(TryNewPoint());
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
