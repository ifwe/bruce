/* <thread/fd_managed_thread.test.cc>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 if(we)

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

   Unit test for <thread/fd_managed_thread.h>.
 */

#include <thread/fd_managed_thread.h>

#include <stdexcept>

#include <unistd.h>

#include <gtest/gtest.h>

using namespace Base;
using namespace Thread;

namespace {

  /* The fixture for testing class TFdManagedThread. */
  class TFdManagedThreadTest : public ::testing::Test {
    protected:
    TFdManagedThreadTest() {
    }

    virtual ~TFdManagedThreadTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TFdManagedThreadTest

  class TTestWorker1 final : public TFdManagedThread {
    public:
    explicit TTestWorker1(bool &flag)
        : Flag(flag) {
    }

    virtual ~TTestWorker1() noexcept {
      ShutdownOnDestroy();
    }

    virtual void Run() override;

    private:
    bool &Flag;
  };  // TTestWorker1

  void TTestWorker1::Run() {
    Flag = true;
  }

  class TTestWorker2 final : public TFdManagedThread {
    public:
    explicit TTestWorker2(bool &flag)
        : Flag(flag) {
    }

    virtual ~TTestWorker2() noexcept {
      ShutdownOnDestroy();
    }

    virtual void Run() override;

    private:
    bool &Flag;
  };  // TTestWorker2

  void TTestWorker2::Run() {
    const TFd &fd = GetShutdownRequestFd();

    while (!fd.IsReadable()) {
      sleep(1);
    }

    Flag = true;
  }

  class TTestWorker3 final : public TFdManagedThread {
    public:
    TTestWorker3(bool &flag, bool throw_std_exception)
        : Flag(flag),
          ThrowStdException(throw_std_exception) {
    }

    virtual ~TTestWorker3() noexcept {
      ShutdownOnDestroy();
    }

    virtual void Run() override;

    private:
    bool &Flag;

    bool ThrowStdException;
  };  // TTestWorker3

  void TTestWorker3::Run() {
    const TFd &fd = GetShutdownRequestFd();

    while (!fd.IsReadable()) {
      sleep(1);
    }

    Flag = true;

    if (ThrowStdException) {
      throw std::length_error("blah");
    }

    throw "random junk";
  }

  TEST_F(TFdManagedThreadTest, Test1) {
    bool thread_executed = false;
    TTestWorker1 worker(thread_executed);
    ASSERT_FALSE(thread_executed);
    worker.Start();

    if (!worker.GetShutdownWaitFd().IsReadable(30000)) {
      ASSERT_TRUE(false);
      return;
    }

    worker.Join();
    ASSERT_TRUE(thread_executed);
    thread_executed = false;
    worker.Start();

    if (!worker.GetShutdownWaitFd().IsReadable(30000)) {
      ASSERT_TRUE(false);
      return;
    }

    worker.Join();
    ASSERT_TRUE(thread_executed);
  }

  TEST_F(TFdManagedThreadTest, Test2) {
    bool flag = false;
    TTestWorker2 worker(flag);
    worker.Start();
    sleep(2);
    ASSERT_FALSE(flag);
    worker.RequestShutdown();

    if (!worker.GetShutdownWaitFd().IsReadable(30000)) {
      ASSERT_TRUE(false);
      return;
    }

    worker.Join();
    ASSERT_TRUE(flag);

    flag = false;
    worker.Start();
    sleep(2);
    ASSERT_FALSE(flag);
    worker.RequestShutdown();

    if (!worker.GetShutdownWaitFd().IsReadable(30000)) {
      ASSERT_TRUE(false);
      return;
    }

    worker.Join();
    ASSERT_TRUE(flag);
  }

  TEST_F(TFdManagedThreadTest, Test3) {
    bool flag = false;
    TTestWorker3 worker(flag, true);
    worker.Start();
    ASSERT_FALSE(flag);
    worker.RequestShutdown();
    bool threw = false;

    if (!worker.GetShutdownWaitFd().IsReadable(30000)) {
      ASSERT_TRUE(false);
      return;
    }

    try {
      worker.Join();
    } catch (const TFdManagedThread::TThreadThrewStdException &x) {
      threw = true;
    }

    ASSERT_TRUE(flag);
    ASSERT_TRUE(threw);

    flag = false;
    worker.Start();
    ASSERT_FALSE(flag);
    worker.RequestShutdown();
    threw = false;

    if (!worker.GetShutdownWaitFd().IsReadable(30000)) {
      ASSERT_TRUE(false);
      return;
    }

    try {
      worker.Join();
    } catch (const TFdManagedThread::TThreadThrewStdException &x) {
      threw = true;
    }

    ASSERT_TRUE(flag);
    ASSERT_TRUE(threw);
  }

  TEST_F(TFdManagedThreadTest, Test4) {
    bool flag = false;
    TTestWorker3 worker(flag, false);
    worker.Start();
    ASSERT_FALSE(flag);
    worker.RequestShutdown();
    bool threw = false;

    if (!worker.GetShutdownWaitFd().IsReadable(30000)) {
      ASSERT_TRUE(false);
      return;
    }

    try {
      worker.Join();
    } catch (const TFdManagedThread::TThreadThrewUnknownException &x) {
      threw = true;
    }

    ASSERT_TRUE(flag);
    ASSERT_TRUE(threw);

    flag = false;
    worker.Start();
    ASSERT_FALSE(flag);
    worker.RequestShutdown();
    threw = false;

    if (!worker.GetShutdownWaitFd().IsReadable(30000)) {
      ASSERT_TRUE(false);
      return;
    }

    try {
      worker.Join();
    } catch (const TFdManagedThread::TThreadThrewUnknownException &x) {
      threw = true;
    }

    ASSERT_TRUE(flag);
    ASSERT_TRUE(threw);
  }

  TEST_F(TFdManagedThreadTest, Test5) {
    bool flag = false;
    TTestWorker1 worker(flag);
    ASSERT_FALSE(flag);
    worker.Start();
    bool threw = false;

    try {
      worker.Start();
    } catch (const TFdManagedThread::TThreadAlreadyStarted &x) {
      threw = true;
    }

    ASSERT_TRUE(threw);
    threw = false;

    if (!worker.GetShutdownWaitFd().IsReadable(30000)) {
      ASSERT_TRUE(false);
      return;
    }

    worker.Join();

    try {
      worker.Join();
    } catch (const TFdManagedThread::TCannotJoinNonexistentThread &x) {
      threw = true;
    }

    ASSERT_TRUE(threw);
    threw = false;

    try {
      worker.RequestShutdown();
    } catch (const TFdManagedThread::TThreadAlreadyShutDown &x) {
      threw = true;
    }

    ASSERT_TRUE(threw);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
