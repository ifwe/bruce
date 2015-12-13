/* <bruce/util/send_buf.test.cc>

   ----------------------------------------------------------------------------
   Copyright 2015 Dave Peterson <dave@dspeterson.com>

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

   Unit test for <bruce/util/send_buf.h>.
 */

#include <bruce/util/send_buf.h>

#include <vector>

#include <gtest/gtest.h>

using namespace Bruce;
using namespace Bruce::Util;

namespace {

  /* The fixture for testing class TSendBuf. */
  class TSendBufTest : public ::testing::Test {
    protected:
    TSendBufTest() {
    }

    virtual ~TSendBufTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TSendBufTest

  TEST_F(TSendBufTest, Test1) {
    TSendBuf<char> buf;
    ASSERT_EQ(buf.DataSize(), 0U);
    ASSERT_TRUE(buf.IsEmpty());
    std::vector<char> storage(buf.GetBuf());
    storage.clear();
    storage.reserve(3);
    storage.push_back('a');
    char *addr = &storage[0];
    buf.PutBuf(std::move(storage));
    ASSERT_TRUE(storage.empty());
    ASSERT_EQ(buf.DataSize(), 1U);
    ASSERT_FALSE(buf.IsEmpty());
    ASSERT_TRUE(buf.Data() == addr);
    ASSERT_EQ(buf.Data()[0], 'a');
    buf.MarkConsumed(1);
    ASSERT_EQ(buf.DataSize(), 0U);
    ASSERT_TRUE(buf.IsEmpty());
    storage = buf.GetBuf();
    ASSERT_TRUE(&storage[0] == addr);
    storage.clear();
    storage.push_back('x');
    storage.push_back('y');
    storage.push_back('z');
    buf.PutBuf(std::move(storage));
    ASSERT_TRUE(storage.empty());
    ASSERT_EQ(buf.DataSize(), 3U);
    ASSERT_FALSE(buf.IsEmpty());
    ASSERT_TRUE(buf.Data() == addr);
    ASSERT_EQ(buf.Data()[0], 'x');
    ASSERT_EQ(buf.Data()[1], 'y');
    ASSERT_EQ(buf.Data()[2], 'z');
    buf.MarkConsumed(2);
    ASSERT_EQ(buf.DataSize(), 1U);
    ASSERT_FALSE(buf.IsEmpty());
    ASSERT_TRUE(buf.Data() == (addr + 2));
    ASSERT_EQ(buf.Data()[0], 'z');
    buf.MarkConsumed(1);
    ASSERT_EQ(buf.DataSize(), 0U);
    ASSERT_TRUE(buf.IsEmpty());
    storage = buf.GetBuf();
    ASSERT_TRUE(&storage[0] == addr);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
