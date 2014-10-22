/* <server/url_decode.test.cc>
 
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
 
   Unit test for <server/url_decode.h>.
 */ 

#include <server/url_decode.h>
  
#include <gtest/gtest.h>
  
using namespace Base;
using namespace std;
using namespace Server;

namespace {

  /* The fixture for testing URL decoding. */
  class TUrlDecodeTest : public ::testing::Test {
    protected:
    TUrlDecodeTest() {
    }

    virtual ~TUrlDecodeTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TUrlDecodeTest

  TEST_F(TUrlDecodeTest, Typical) {
    string result_buf;
    UrlDecode(AsPiece("Hello%2C%20World%26%25Ab%fF"), result_buf);
    ASSERT_EQ(result_buf, "Hello, World&%Ab\xff");
  }
  
  TEST_F(TUrlDecodeTest, Empty) {
    string result_buf;
  
    UrlDecode(AsPiece(""), result_buf);
    ASSERT_EQ(result_buf, "");
  
    UrlDecode(AsPiece("foo"), result_buf);
    ASSERT_EQ(result_buf, "foo");
  
    UrlDecode(Base::TPiece<const char>(), result_buf);
    ASSERT_EQ(result_buf, "");
  
    UrlDecode(AsPiece("%3C%7B+.x%3A448.0%2C+.y%3A147.0+%7D%3E"), result_buf);
    ASSERT_EQ(result_buf, "<{ .x:448.0, .y:147.0 }>");
    UrlDecode(AsPiece(
        "%3C%7B.text%3A%27%5Cn%3Cp%3EWhat%27s+up%3C%2Fp%3E%27%7D%3E"),
        result_buf);
    ASSERT_EQ(result_buf, "<{.text:'\\n<p>What's up</p>'}>");
    UrlDecode(AsPiece(
        "%7B%22likes%22%3A%5B%5D%2C%22userref%22%3A%22test2%22%2C%22content"
        "%22%3A%22aaa%22%2C%22cid%22%3A1%7D"), result_buf);
    ASSERT_EQ(result_buf,
        "{\"likes\":[],\"userref\":\"test2\",\"content\":\"aaa\",\"cid\":1}");
  }
  
  // TODO: Test for error cases

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
