/* <base/convert.test.cc>
 
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
 
   Unit test for <base/convert.h>.
 */

#include <base/convert.h>
  
#include <gtest/gtest.h>
  
#include <climits>
#include <iostream>
  
using namespace Base;
 
namespace {

  class TConvertTest : public ::testing::Test {
    protected:
    TConvertTest() {
    }

    virtual ~TConvertTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TConvertTest

  // TODO: Get to full coverage. Currently just a compile test.
  TEST_F(TConvertTest, Int) {
    int i = 0;
    TConverter(AsPiece("42")).ReadInt(i);
    ASSERT_EQ(i, 42);
  }
  
  TEST_F(TConvertTest, TypeLimits) {
    long i = 0;
    TConverter(AsPiece("9223372036854775807")).ReadInt(i);
    ASSERT_EQ(i, 9223372036854775807l);
    TConverter(AsPiece("-9223372036854775808")).ReadInt(i);
    ASSERT_EQ(i, LONG_MIN);
  }
  
  TEST_F(TConvertTest, ProxyInt) {
    int i = TConvertProxy(AsPiece("123"));
    ASSERT_EQ(i, 123);
    size_t size = TConvertProxy(AsPiece("456"));
    ASSERT_EQ(size, 456ul);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
