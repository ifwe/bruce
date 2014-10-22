/* <io/stream.test.cc>
 
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
 
   Unit test for <io/stream.h>.
 */

#include <io/stream.h>
  
#include <string>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Io;

namespace {

  class TMyFormat {
    public:
    TMyFormat()
        : X(DefaultX), Y(DefaultY) {
    }
  
    int X, Y;
  
    string Foo;
  
    static const int DefaultX, DefaultY;
  };  // TMyFormat
  
  const int
      TMyFormat::DefaultX = 101,
      TMyFormat::DefaultY = 202;
  
  class TMyStream : public TStream<TMyFormat> {
    NO_COPY_SEMANTICS(TMyStream);

    public:
    TMyStream() {
    }
  };  // TMyStream
  
  typedef TFormatter<TMyFormat> TMyFormatter;

  /* The fixture for testing class TStream. */
  class TStreamTest : public ::testing::Test {
    protected:
    TStreamTest() {
    }

    virtual ~TStreamTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TStreamTest
  
  TEST_F(TStreamTest, Typical) {
    TMyStream strm;
    ASSERT_EQ(strm.GetFormat().X, TMyFormat::DefaultX);
    ASSERT_EQ(strm.GetFormat().Y, TMyFormat::DefaultY);
    static const int expected = 303;
    strm << SetFormat(&TMyFormat::X, expected);
    ASSERT_EQ(strm.GetFormat().X, expected);

    /* extra */ {
      TFormatter<TMyFormat> formatter(&strm);
      formatter->Y = expected;
      ASSERT_EQ(strm.GetFormat().Y, expected);
    }

    ASSERT_EQ(strm.GetFormat().Y, TMyFormat::DefaultY);
  }
  
  TEST_F(TStreamTest, Strings) {
    TMyStream strm;
    ASSERT_TRUE(strm.GetFormat().Foo.empty());
    const string expected = "mofo";
    strm << SetFormat(&TMyFormat::Foo, expected);
    ASSERT_EQ(strm.GetFormat().Foo, expected);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
