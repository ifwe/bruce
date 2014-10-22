/* <socket/option.test.cc>
 
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
 
   Unit test for <socket/option.h>.
 */

#include <socket/option.h>
  
#include <sstream>
  
#include <base/fd.h>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace chrono;
using namespace Base;
using namespace Socket;

namespace {

  /* The fixture for testing class TSocketOption. */
  class TSocketOptionTest : public ::testing::Test {
    protected:

    TSocketOptionTest() {
    }

    virtual ~TSocketOptionTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TSocketOptionTest

  TEST_F(TSocketOptionTest, LowLevelGetAndSet) {
    TFd sock(socket(AF_INET, SOCK_STREAM, 0));
    int arg;
    GetSockOpt(sock, SO_REUSEADDR, arg);
    ASSERT_EQ(arg, 0);
    SetSockOpt(sock, SO_REUSEADDR, 1);
    GetSockOpt(sock, SO_REUSEADDR, arg);
    ASSERT_EQ(arg, 1);
    SetSockOpt(sock, SO_REUSEADDR, 0);
    GetSockOpt(sock, SO_REUSEADDR, arg);
    ASSERT_EQ(arg, 0);
  }
  
  TEST_F(TSocketOptionTest, ConvBool) {
    TFd sock(socket(AF_INET, SOCK_STREAM, 0));
    bool val;
    Conv<bool>::GetSockOpt(sock, SO_REUSEADDR, val);
    ASSERT_FALSE(val);
    Conv<bool>::SetSockOpt(sock, SO_REUSEADDR, true);
    Conv<bool>::GetSockOpt(sock, SO_REUSEADDR, val);
    ASSERT_TRUE(val);
    Conv<bool>::SetSockOpt(sock, SO_REUSEADDR, false);
    Conv<bool>::GetSockOpt(sock, SO_REUSEADDR, val);
    ASSERT_FALSE(val);
  }
  
  TEST_F(TSocketOptionTest, ConvInt) {
    TFd sock(socket(AF_INET, SOCK_STREAM, 0));
    int val;
    Conv<int>::GetSockOpt(sock, SO_REUSEADDR, val);
    ASSERT_EQ(val, 0);
    Conv<int>::SetSockOpt(sock, SO_REUSEADDR, 1);
    Conv<int>::GetSockOpt(sock, SO_REUSEADDR, val);
    ASSERT_EQ(val, 1);
    Conv<int>::SetSockOpt(sock, SO_REUSEADDR, 0);
    Conv<int>::GetSockOpt(sock, SO_REUSEADDR, val);
    ASSERT_EQ(val, 0);
  }
  
  TEST_F(TSocketOptionTest, ConvLinger) {
    TFd sock(socket(AF_INET, SOCK_STREAM, 0));
    Conv<TLinger>::SetSockOpt(sock, SO_LINGER, TLinger());
    TLinger val;
    Conv<TLinger>::GetSockOpt(sock, SO_LINGER, val);
    ASSERT_FALSE(val.IsKnown());
    Conv<TLinger>::SetSockOpt(sock, SO_LINGER, TLinger(seconds(30)));
    Conv<TLinger>::GetSockOpt(sock, SO_LINGER, val);
    ASSERT_TRUE(val.IsKnown());
    ASSERT_EQ(val->count(), 30);
  }
  
  TEST_F(TSocketOptionTest, ConvTimeout) {
    TFd sock(socket(AF_INET, SOCK_STREAM, 0));
    Conv<TTimeout>::SetSockOpt(sock, SO_RCVTIMEO, TTimeout(2000000));
    TTimeout val;
    Conv<TTimeout>::GetSockOpt(sock, SO_RCVTIMEO, val);
    ASSERT_EQ(val.count(), 2000000);
    Conv<TTimeout>::SetSockOpt(sock, SO_RCVTIMEO, TTimeout(0));
    Conv<TTimeout>::GetSockOpt(sock, SO_RCVTIMEO, val);
    ASSERT_EQ(val.count(), 0);
  }
  
  template <typename TVal>
  string ToString(const TVal &val) {
    ostringstream strm;
    Format<TVal>::Dump(strm, val);
    return strm.str();
  }
  
  TEST_F(TSocketOptionTest, FormatBool) {
    ASSERT_EQ(ToString(true), "true");
    ASSERT_EQ(ToString(false), "false");
  }
  
  TEST_F(TSocketOptionTest, FormatInt) {
    ASSERT_EQ(ToString(0), "0");
    ASSERT_EQ(ToString(101), "101");
    ASSERT_EQ(ToString(-101), "-101");
  }
  
  TEST_F(TSocketOptionTest, FormatLinger) {
    ASSERT_EQ(ToString(TLinger()), "off");
    ASSERT_EQ(ToString(TLinger(seconds(0))), "0 sec(s)");
    ASSERT_EQ(ToString(TLinger(seconds(30))), "30 sec(s)");
  }
  
  TEST_F(TSocketOptionTest, FormatUcred) {
    ucred val;
    val.pid = 101;
    val.uid = 202;
    val.gid = 303;
    ASSERT_EQ(ToString(val), "{ pid: 101, uid: 202, gid: 303 }");
  }
  
  TEST_F(TSocketOptionTest, FormatTimeout) {
    ASSERT_EQ(ToString(TTimeout()), "0 usec(s)");
    ASSERT_EQ(ToString(TTimeout(microseconds(200))), "200 usec(s)");
  }
  
  TEST_F(TSocketOptionTest, FormatString) {
    ASSERT_EQ(ToString(string()), "\"\"");
    ASSERT_EQ(ToString(string("eth0")), "\"eth0\"");
  }
  
  TEST_F(TSocketOptionTest, StdObject) {
    TFd sock(socket(AF_INET, SOCK_STREAM, 0));
    bool val = ReuseAddr.Get(sock);
    ASSERT_FALSE(val);
    ReuseAddr.Set(sock, true);
    ReuseAddr.Get(sock, val);
    ASSERT_TRUE(val);
    ReuseAddr.Set(sock, false);
    ReuseAddr.Get(sock, val);
    ASSERT_FALSE(val);
    ostringstream strm;
    ReuseAddr.Dump(strm, sock);
    ASSERT_EQ(strm.str(), "reuse_addr: false");
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
