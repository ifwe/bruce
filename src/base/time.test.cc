/* <base/time.test.cc>

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

   Unit test for <base/time.h>.
 */
  
#include <base/time.h>
  
#include <unistd.h>
  
#include <gtest/gtest.h>
  
using namespace Base;

namespace {

  /* The fixture for testing class TTime. */
  class TTimeTest : public ::testing::Test {
    protected:
    TTimeTest() {
    }

    virtual ~TTimeTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TTimeTest

  TEST_F(TTimeTest, Now) {
    TTime t1, t2;
    t1.Now();
    sleep(1);
    t2.Now();
    ASSERT_TRUE(t2 > t1);
    ASSERT_TRUE(t2 >= t1);
    ASSERT_TRUE(t1 < t2);
    ASSERT_TRUE(t1 <= t2);
    ASSERT_TRUE(t1 != t2);
    ASSERT_FALSE(t1 == t2);
    ASSERT_FALSE(t2 < t1);
    ASSERT_FALSE(t2 <= t1);
    ASSERT_FALSE(t1 > t2);
    ASSERT_FALSE(t1 >= t2);
  }
  
  TEST_F(TTimeTest, Plus) {
    TTime t1(1, 500000000);
    TTime t2(1, 500000000);
    TTime t3(3, 0);
    TTime t4 = t1 + t2;
    ASSERT_TRUE(t4 == t3);
  }
  
  TEST_F(TTimeTest, Minus) {
    TTime t1(1, 500000000);
    TTime t2(1, 500000000);
    TTime t3(3, 0);
    TTime t4 = t3 - t2;
    ASSERT_TRUE(t4 == t1);
    ASSERT_TRUE(t4 == t2);
  }
  
  TEST_F(TTimeTest, PlusEqMsec) {
    TTime t1(1, 500000000);
    TTime t2(3, 0);
    t1 += 1500;
    ASSERT_TRUE(t1 == t2);
  }
  
  TEST_F(TTimeTest, MinusEqMsec) {
    TTime t1(1, 500000000);
    TTime t2(3, 0);
    t2 -= 1500;
    ASSERT_TRUE(t1 == t2);
  }
  
  TEST_F(TTimeTest, Remaining) {
    TTime t1(1, 500000000);
    TTime t2;
    t2.Now();
    t2 += t1;
    size_t remaining = t2.Remaining();
    ASSERT_TRUE(remaining < 1500);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
