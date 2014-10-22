/* <base/slice.test.cc>
 
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
 
   Unit test for <base/slice.h>.
 */

#include <base/slice.h>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Base;

namespace {

  size_t AsSize(int i) {
    return static_cast<size_t>(i);
  }

  /* The fixture for testing class TSlice. */
  class TSliceTest : public ::testing::Test {
    protected:
    // You can remove any or all of the following functions if its body
    // is empty.

    TSliceTest() {
      // You can do set-up work for each test here.
    }

    virtual ~TSliceTest() {
      // You can do clean-up work that doesn't throw exceptions here.
    }

    // If the constructor and destructor are not enough for setting up
    // and cleaning up each test, you can define the following methods:

    virtual void SetUp() {
      // Code here will be called immediately after the constructor (right
      // before each test).
    }

    virtual void TearDown() {
      // Code here will be called immediately after each test (right
      // before the destructor).
    }

    // Objects declared here can be used by all tests in the test case for Foo.
  };  // TSliceTest

  TEST_F(TSliceTest, Success) {
    size_t start = 99, limit = 99;
    TSlice().GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(0));
    ASSERT_EQ(limit, AsSize(10));
    start = 99; limit = 99;
    TSlice::All->GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(0));
    ASSERT_EQ(limit, AsSize(10));
    start = 99; limit = 99;
    TSlice::AtStart->GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(0));
    ASSERT_EQ(limit, AsSize(0));
    start = 99; limit = 99;
    TSlice::AtLimit->GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(10));
    ASSERT_EQ(limit, AsSize(10));
    start = 99; limit = 99;
    TSlice(0, 1).GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(0));
    ASSERT_EQ(limit, AsSize(1));
    start = 99; limit = 99;
    TSlice(1, 2).GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(1));
    ASSERT_EQ(limit, AsSize(2));
    start = 99; limit = 99;
    TSlice(8, 9).GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(8));
    ASSERT_EQ(limit, AsSize(9));
    start = 99; limit = 99;
    TSlice(9, 10).GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(9));
    ASSERT_EQ(limit, AsSize(10));
    start = 99; limit = 99;
    TSlice(5).GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(5));
    ASSERT_EQ(limit, AsSize(10));
    start = 99; limit = 99;
    TSlice(TPos(7, TPos::Reverse)).GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(2));
    ASSERT_EQ(limit, AsSize(10));
    start = 99; limit = 99;
    TSlice(TPos(7, TPos::Reverse), TPos(4, TPos::Reverse))
        .GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(2));
    ASSERT_EQ(limit, AsSize(5));
    start = 99; limit = 99;
    TSlice(1, TPos(0, TPos::Reverse)).GetAbsPair(10, start, limit);
    ASSERT_EQ(start, AsSize(1));
    ASSERT_EQ(limit, AsSize(9));
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
