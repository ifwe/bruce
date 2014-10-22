/* <base/error_utils.test.cc>
 
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
 
   Unit test for <base/error_utils.h>.
 */

#include <base/error_utils.h>
  
#include <condition_variable>
#include <mutex>
#include <thread>
  
#include <signal.h>
#include <unistd.h>
  
#include <base/zero.h>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Base;

namespace {

  /* The fixture for testing error utils. */
  class TErrorUtilsTest : public ::testing::Test {
    protected:
    // You can remove any or all of the following functions if its body
    // is empty.

    TErrorUtilsTest() {
      // You can do set-up work for each test here.
    }

    virtual ~TErrorUtilsTest() {
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
  };  // TErrorUtilsTest

  TEST_F(TErrorUtilsTest, LibraryGenerated) {
    bool caught = false;
    try {
      thread().detach();
    } catch (const system_error &error) {
      caught = true;
    } catch (...) {}
    ASSERT_TRUE(caught);
  }
  
  TEST_F(TErrorUtilsTest, UtilsGenerated) {
    bool caught = false;

    try {
      IfLt0(read(-1, 0, 0));
    } catch (const system_error &error) {
      caught = true;
    } catch (...) {}
    ASSERT_TRUE(caught);
  }
 
#if 0 
  TEST_F(TErrorUtilsTest, Interruption) {
    struct sigaction action;
    Zero(action);
    action.sa_handler = [](int) {};
    sigaction(SIGUSR1, &action, 0);
    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGUSR1);
    sigprocmask(SIG_UNBLOCK, &sigset, nullptr);
    mutex mx;
    condition_variable cv;
    bool running = false, was_interrupted = false;
    thread t([&mx, &cv, &running, &was_interrupted] {
      /* lock */ {
        unique_lock<mutex> lock(mx);
        running = true;
        cv.notify_one();
      }
      try {
        IfLt0(pause());
      } catch (system_error &error) {
        was_interrupted = WasInterrupted(error);
      } catch (...) {}
    });
    /* lock */ {
      unique_lock<mutex> lock(mx);
      while (!running) {
        cv.wait(lock);
      }
    }
    IfNe0(pthread_kill(t.native_handle(), SIGUSR1));
    t.join();
    ASSERT_TRUE(was_interrupted);
  }
#endif

}  // namespace
