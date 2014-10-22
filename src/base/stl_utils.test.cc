/* <base/stl_utils.test.cc>
 
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
 
   Unit test for <base/stl_utils.h>.
 */

#include <base/stl_utils.h>
  
#include <unordered_map>
#include <unordered_set>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Base;

namespace {

  /* The fixture for testing STL utilities. */
  class TStlUtilsTest : public ::testing::Test {
    protected:
    TStlUtilsTest() {
    }

    virtual ~TStlUtilsTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TStlUtilsTest

  TEST_F(TStlUtilsTest, Contains) {
    unordered_set<int> container;
    container.insert(101);
    ASSERT_TRUE(Contains(container, 101));
    ASSERT_FALSE(Contains(container, 202));
  }
  
  TEST_F(TStlUtilsTest, FindOrDefault) {
    unordered_map<int, double> container;
    const int key = 101;
    const double expected = 98.6;
    container[key] = expected;
    ASSERT_EQ(FindOrDefault(container, key, expected + 1), expected);
    ASSERT_EQ(FindOrDefault(container, key + 1, expected + 1), expected + 1);
  }
  
  TEST_F(TStlUtilsTest, FindOrInsert) {
    unordered_map<int, double> container;
    const int key = 101;
    const double expected = 98.6;
    ASSERT_EQ(FindOrInsert(container, key, expected), expected);
    ASSERT_EQ(FindOrInsert(container, key, expected + 1), expected);
  }
  
  TEST_F(TStlUtilsTest, RotatedLeft) {
    ASSERT_EQ(RotatedLeft<unsigned short>(0x1234, 4), 0x2341);
  }
  
  TEST_F(TStlUtilsTest, RotatedRight) {
    ASSERT_EQ(RotatedRight<unsigned short>(0x1234, 4), 0x4123);
  }
  
  TEST_F(TStlUtilsTest, TryFind) {
    unordered_map<int, double> container;
    const int key = 101;
    const double expected = 98.6;
    container[key] = expected;
    const double *result = TryFind(container, key);
    ASSERT_TRUE(result);
    ASSERT_EQ(*result, expected);
    ASSERT_FALSE(TryFind(container, key + 1));
  }
  
  TEST_F(TStlUtilsTest, EqEq) {
    unordered_set<int> lhs, rhs;
    ASSERT_TRUE(eqeq(lhs,rhs));
    lhs.insert(101);
    ASSERT_FALSE(eqeq(lhs,rhs));
    ASSERT_FALSE(eqeq(rhs,lhs));
    rhs.insert(101);
    ASSERT_TRUE(eqeq(lhs,rhs));
    lhs.insert(202);
    ASSERT_FALSE(eqeq(lhs,rhs));
    rhs.insert(303);
    ASSERT_FALSE(eqeq(lhs,rhs));
    lhs.insert(303);
    ASSERT_FALSE(eqeq(lhs,rhs));
    rhs.insert(202);
    ASSERT_TRUE(eqeq(lhs,rhs));
  }
  
  TEST_F(TStlUtilsTest, EqEqMap) {
    unordered_map<int,double> lhs, rhs;
    ASSERT_TRUE(eqeq_map(lhs,rhs));
    lhs[101]=98.7;
    ASSERT_FALSE(eqeq_map(lhs,rhs));
    ASSERT_FALSE(eqeq_map(rhs,lhs));
    rhs[101]=98.7;
    ASSERT_TRUE(eqeq_map(lhs,rhs));
    lhs[202]=1.23;
    ASSERT_FALSE(eqeq_map(lhs,rhs));
    rhs[303]=3.14;
    ASSERT_FALSE(eqeq_map(lhs,rhs));
    lhs[303]=3.14;
    ASSERT_FALSE(eqeq_map(lhs,rhs));
    rhs[202]=1.23;
    ASSERT_TRUE(eqeq_map(lhs,rhs));
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
