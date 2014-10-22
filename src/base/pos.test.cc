/* <base/pos.test.cc>
 
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
 
   Unit test for <base/pos.h>.
 */

#include <base/pos.h>
  
#include <gtest/gtest.h>
  
using namespace Base;

namespace {

  /* The fixture for testing class TPos. */
  class TPosTest : public ::testing::Test {
    protected:
    TPosTest() {
    }

    virtual ~TPosTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TPosTest

  TEST_F(TPosTest, Constants) {
    ASSERT_EQ(TPos::Start->GetAbsOffset(10),  0);
    ASSERT_EQ(TPos::Limit->GetAbsOffset(10), 10);
    ASSERT_EQ(TPos::ReverseStart->GetAbsOffset(10),  9);
    ASSERT_EQ(TPos::ReverseLimit->GetAbsOffset(10), -1);
    ASSERT_EQ(TPos::GetStart(TPos::Forward).GetAbsOffset(10),  0);
    ASSERT_EQ(TPos::GetLimit(TPos::Forward).GetAbsOffset(10), 10);
    ASSERT_EQ(TPos::GetStart(TPos::Reverse).GetAbsOffset(10),  9);
    ASSERT_EQ(TPos::GetLimit(TPos::Reverse).GetAbsOffset(10), -1);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
