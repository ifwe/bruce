/* <socket/named_unix_socket.test.cc>
 
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
 
   Unit test for <socket/named_unix_socket.h>.
 */ 

#include <socket/named_unix_socket.h>
  
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
  
#include <socket/address.h>
  
#include <gtest/gtest.h>
  
using namespace Socket;

namespace {

  /* The fixture for testing class TNamedUnixSocket. */
  class TNamedUnixSocketTest : public ::testing::Test {
    protected:
    TNamedUnixSocketTest() {
    }

    virtual ~TNamedUnixSocketTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TNamedUnixSocketTest

  TEST_F(TNamedUnixSocketTest, NamedUnixSocketTest) {
    TNamedUnixSocket sock(SOCK_DGRAM, 0);
    ASSERT_TRUE(sock.IsOpen());
    ASSERT_FALSE(sock.IsBound());
    ASSERT_TRUE(sock.GetPath().empty());
    int fd = sock;
    int fd2 = sock.GetFd();
    ASSERT_EQ(fd, fd2);
    TAddress address;
    address.SetFamily(AF_LOCAL);
    const char path[] = "/tmp/named_unix_socket_test";
    address.SetPath(path);
    Bind(sock, address);
    ASSERT_TRUE(sock.IsOpen());
    ASSERT_TRUE(sock.IsBound());
    struct stat buf;
    int ret = stat(path, &buf);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(S_ISSOCK(buf.st_mode));
    sock.Reset();
    ASSERT_FALSE(sock.IsOpen());
    ASSERT_FALSE(sock.IsBound());
    ret = stat(path, &buf);
    ASSERT_EQ(ret, -1);
  
    {
      TNamedUnixSocket sock2(SOCK_DGRAM, 0);
      TAddress address2;
      address2.SetFamily(AF_LOCAL);
      address2.SetPath(path);
      Bind(sock2, address2);
      ASSERT_TRUE(sock2.IsOpen());
      ASSERT_TRUE(sock2.IsBound());
      ret = stat(path, &buf);
      ASSERT_EQ(ret, 0);
      ASSERT_TRUE(S_ISSOCK(buf.st_mode));
    }
  
    ret = stat(path, &buf);
    ASSERT_EQ(ret, -1);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
