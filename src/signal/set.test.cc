/* <signal/set.test.cc>
 
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
 
   Unit test for <signal/set.h>.
 */

#include <signal/set.h>
  
#include <gtest/gtest.h>
  
using namespace Signal;

namespace {

  /* The fixture for testing signal sets. */
  class TSetTest : public ::testing::Test {
    protected:
    TSetTest() {
    }

    virtual ~TSetTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TSetTest

  TEST_F(TSetTest, Empty) {
    TSet a;
    ASSERT_FALSE(a[SIGINT]);
    a += SIGINT;
    ASSERT_TRUE(a[SIGINT]);
    a -= SIGINT;
    ASSERT_FALSE(a[SIGINT]);
  }
  
  TEST_F(TSetTest, Full) {
    TSet a(TSet::Full);
    ASSERT_TRUE(a[SIGINT]);
    a -= SIGINT;
    ASSERT_FALSE(a[SIGINT]);
    a += SIGINT;
    ASSERT_TRUE(a[SIGINT]);
  }
  
  TEST_F(TSetTest, Copy) {
    TSet a(TSet::Include, { SIGINT });
    ASSERT_TRUE(a[SIGINT]);
    TSet b(a);
    ASSERT_TRUE(a[SIGINT]);
    ASSERT_TRUE(b[SIGINT]);
  }
  
  TEST_F(TSetTest, Assign) {
    TSet a(TSet::Include, { SIGINT });
    ASSERT_TRUE(a[SIGINT]);
    TSet b;
    ASSERT_FALSE(b[SIGINT]);
    b = a;
    ASSERT_TRUE(a[SIGINT]);
    ASSERT_TRUE(b[SIGINT]);
  }
  
  TEST_F(TSetTest, Exclude) {
    TSet a(TSet::Exclude, { SIGINT });
    ASSERT_TRUE(a[SIGPIPE]);
    ASSERT_FALSE(a[SIGINT]);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
