/* <base/random_exp_backoff.test.cc>
 
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
 
   Unit test for <base/random_exp_backoff.h>.
 */
  
#include <base/random_exp_backoff.h>
  
#include <gtest/gtest.h>
  
using namespace Base;

namespace {

  unsigned MockRandomNumberGenerator() {
    return 0;
  }

  /* The fixture for testing class TRandomExpBackoff. */
  class TRandomExpBackoffTest : public ::testing::Test {
    protected:
    TRandomExpBackoffTest() {
    }

    virtual ~TRandomExpBackoffTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TRandomExpBackoffTest

  TEST_F(TRandomExpBackoffTest, Test1) {
    TRandomExpBackoff backoff(8, 5, MockRandomNumberGenerator);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 8U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 5U);
    ASSERT_EQ(backoff.NextValue(), 4U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 16U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 4U);
    ASSERT_EQ(backoff.NextValue(), 8U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 32U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 3U);
    ASSERT_EQ(backoff.NextValue(), 16U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 64U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 2U);
    ASSERT_EQ(backoff.NextValue(), 32U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 128U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 1U);
    ASSERT_EQ(backoff.NextValue(), 64U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 256U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 0U);
    ASSERT_EQ(backoff.NextValue(), 128U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 256U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 0U);
    ASSERT_EQ(backoff.NextValue(), 128U);
    ASSERT_EQ(backoff.GetInitialCount(), 8U);
    ASSERT_EQ(backoff.GetMaxDouble(), 5U);
  
    backoff.Reset();
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 8U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 5U);
    ASSERT_EQ(backoff.NextValue(), 4U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 16U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 4U);
    ASSERT_EQ(backoff.NextValue(), 8U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 32U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 3U);
    ASSERT_EQ(backoff.NextValue(), 16U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 64U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 2U);
    ASSERT_EQ(backoff.NextValue(), 32U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 128U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 1U);
    ASSERT_EQ(backoff.NextValue(), 64U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 256U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 0U);
    ASSERT_EQ(backoff.NextValue(), 128U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 256U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 0U);
    ASSERT_EQ(backoff.NextValue(), 128U);
    ASSERT_EQ(backoff.GetInitialCount(), 8U);
    ASSERT_EQ(backoff.GetMaxDouble(), 5U);
  
    backoff.Reset(16, 4);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 16U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 4U);
    ASSERT_EQ(backoff.NextValue(), 8U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 32U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 3U);
    ASSERT_EQ(backoff.NextValue(), 16U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 64U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 2U);
    ASSERT_EQ(backoff.NextValue(), 32U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 128U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 1U);
    ASSERT_EQ(backoff.NextValue(), 64U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 256U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 0U);
    ASSERT_EQ(backoff.NextValue(), 128U);
    ASSERT_EQ(backoff.GetCurrentBaseCount(), 256U);
    ASSERT_EQ(backoff.GetCurrentDoubleTimesLeft(), 0U);
    ASSERT_EQ(backoff.NextValue(), 128U);
    ASSERT_EQ(backoff.GetInitialCount(), 16U);
    ASSERT_EQ(backoff.GetMaxDouble(), 4U);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
