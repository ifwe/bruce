/* <base/io_utils.test.cc>
 
   ----------------------------------------------------------------------------
   Copyright 2010-2013 Tagged

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
 
   Unit test for <base/io_utils.h>.
 */

#include <base/io_utils.h>
  
#include <cerrno>
#include <cstring>
#include <string>
#include <system_error>
  
#include <signal.h>
  
#include <base/fd.h>
#include <base/zero.h>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Base;

namespace {

  static const char *ExpectedData = "Mofo the Psychic Gorilla";
  static const size_t ExpectedSize = strlen(ExpectedData);
  
  static const size_t MaxActualSize = 1024;
  static char ActualData[MaxActualSize];

  /* The fixture for testing I/O utils. */
  class TIoUtilsTest : public ::testing::Test {
    protected:
    // You can remove any or all of the following functions if its body
    // is empty.

    TIoUtilsTest() {
      // You can do set-up work for each test here.
    }

    virtual ~TIoUtilsTest() {
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
  };  // TIoUtilsTest

  TEST_F(TIoUtilsTest, ReadAtMost) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    WriteExactly(writeable, ExpectedData, ExpectedSize);
    Zero(ActualData);
    size_t actual_size = ReadAtMost(readable, ActualData, MaxActualSize);
    ASSERT_EQ(actual_size, ExpectedSize);
    ASSERT_FALSE(strcmp(ActualData, ExpectedData));
    bool timed_out = false;
    try {
      actual_size = ReadAtMost(readable, ActualData, MaxActualSize, 1000);
    } catch (const system_error &x) {
      if (x.code().value() == ETIMEDOUT) {
        timed_out = true;
      } else {
        ASSERT_TRUE(false);
      }
    }
    ASSERT_TRUE(timed_out);
  }
  
  TEST_F(TIoUtilsTest, WriteAtMost) {
    struct sigaction action;
    Zero(action);
    action.sa_handler = [](int) {};
    sigaction(SIGPIPE, &action, 0);
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    readable.Reset();
    size_t actual_size = WriteAtMost(writeable, 0, 0);
    ASSERT_FALSE(actual_size);
    bool caught_broken_pipe = false;
    try {
      WriteAtMost(writeable, ExpectedData, ExpectedSize);
    } catch (const system_error &error) {
      /* TODO: change this to error.code() == errc::broken_pipe */
      caught_broken_pipe = (error.code().value() == EPIPE);
    }
    ASSERT_TRUE(caught_broken_pipe);
    writeable.Reset();
    TFd::Pipe(readable, writeable);
    bool timed_out = false;
    try {
      for (; ; ) {
        WriteAtMost(writeable, ExpectedData, ExpectedSize, 1000);
      }
    } catch (const system_error &x) {
      if (x.code().value() == ETIMEDOUT) {
        timed_out = true;
      } else {
        ASSERT_TRUE(false);
      }
    }
    ASSERT_TRUE(timed_out);
  }
  
  TEST_F(TIoUtilsTest, TryReadExactlyNothing) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    writeable.Reset();
    ASSERT_FALSE(TryReadExactly(readable, ActualData, ExpectedSize));
  }
  
  TEST_F(TIoUtilsTest, TryReadExactlyEverything) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    WriteExactly(writeable, ExpectedData, ExpectedSize);
    ASSERT_TRUE(TryReadExactly(readable, ActualData, ExpectedSize));
  }
  
  TEST_F(TIoUtilsTest, TryReadExactlyTimeout) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    bool timed_out = false;
    try {
      TryReadExactly(readable, ActualData, ExpectedSize, 1000);
    } catch (const system_error &x) {
      if (x.code().value() == ETIMEDOUT) {
        timed_out = true;
      } else {
        ASSERT_TRUE(false);
      }
    }
    ASSERT_TRUE(timed_out);
  }
  
  TEST_F(TIoUtilsTest, TryWriteExactlyTimeout) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    bool timed_out = false;
    try {
      for (; ; ) {
        TryWriteExactly(writeable, ExpectedData, ExpectedSize, 1000);
      }
    } catch (const system_error &x) {
      if (x.code().value() == ETIMEDOUT) {
        timed_out = true;
      } else {
        ASSERT_TRUE(false);
      }
    }
    ASSERT_TRUE(timed_out);
  }
  
  TEST_F(TIoUtilsTest, TryReadExactlySomething) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    WriteExactly(writeable, ExpectedData, ExpectedSize / 2);
    writeable.Reset();
    bool caught_unexpected_end = false;
    try {
      TryReadExactly(readable, ActualData, ExpectedSize);
    } catch (const TUnexpectedEnd &) {
      caught_unexpected_end = true;
    }
    ASSERT_TRUE(caught_unexpected_end);
  }
  
  TEST_F(TIoUtilsTest, ReadExactlyNothing) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    writeable.Reset();
    bool caught_could_not_start = false;
    try {
      ReadExactly(readable, ActualData, ExpectedSize);
    } catch (const TCouldNotStart &) {
      caught_could_not_start = true;
    }
    ASSERT_TRUE(caught_could_not_start);
  }
  
  TEST_F(TIoUtilsTest, ReadExactlyEverything) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    WriteExactly(writeable, ExpectedData, ExpectedSize);
    Zero(ActualData);
    ReadExactly(readable, ActualData, ExpectedSize);
    ASSERT_FALSE(strcmp(ActualData, ExpectedData));
  }
  
  TEST_F(TIoUtilsTest, ReadExactlySomething) {
    TFd readable, writeable;
    TFd::Pipe(readable, writeable);
    WriteExactly(writeable, ExpectedData, ExpectedSize / 2);
    writeable.Reset();
    bool caught_unexpected_end = false;
    try {
      ReadExactly(readable, ActualData, ExpectedSize);
    } catch (const TUnexpectedEnd &) {
      caught_unexpected_end = true;
    }
    ASSERT_TRUE(caught_unexpected_end);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
