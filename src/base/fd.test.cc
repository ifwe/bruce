/* <base/fd.test.cc>
 
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
 
   Unit test for <base/fd.h>.
 */

#include <base/fd.h>
  
#include <cstring>
  
#include <base/io_utils.h>
#include <base/zero.h>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Base;
 
namespace {

  /* The fixture for testing class TFd. */
  class TFdTest : public ::testing::Test {
    protected:
    TFdTest() {
    }

    virtual ~TFdTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TFdTest

  static const char *ExpectedData = "hello";
  static const size_t ExpectedSize = strlen(ExpectedData);
  
  static const size_t MaxActualSize = 1024;
  static char ActualData[MaxActualSize];
  
  static void Transact(const TFd &readable, const TFd &writeable) {
    WriteExactly(writeable, ExpectedData, ExpectedSize);
    Zero(ActualData);
    size_t actual_size = ReadAtMost(readable, ActualData, MaxActualSize);
    ASSERT_EQ(actual_size, ExpectedSize);
    ASSERT_FALSE(strcmp(ActualData, ExpectedData));
  }
  
  static void CheckClose(const TFd &readable, TFd &writeable) {
    writeable.Reset();
    size_t actual_size = ReadAtMost(readable, ActualData, MaxActualSize);
    ASSERT_FALSE(actual_size);
  }
  
  TEST_F(TFdTest, Pipe) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    Transact(readable, writeable);
    CheckClose(readable, writeable);
  }
  
  TEST_F(TFdTest, SocketPair) {
    TFd lhs, rhs;
    TFd::SocketPair(lhs, rhs, AF_UNIX, SOCK_STREAM, 0);
    Transact(lhs, rhs);
    Transact(rhs, lhs);
    CheckClose(lhs, rhs);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
