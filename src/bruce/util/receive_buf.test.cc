/* <bruce/util/receive_buf.test.cc>

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

   Unit test for <bruce/util/receive_buf.h>.
 */

#include <bruce/util/receive_buf.h>

#include <cstring>
#include <vector>

#include <gtest/gtest.h>

using namespace Bruce;
using namespace Bruce::Util;

namespace {

  /* The fixture for testing class TReceiveBuf. */
  class TReceiveBufTest : public ::testing::Test {
    protected:
    TReceiveBufTest() {
    }

    virtual ~TReceiveBufTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TReceiveBufTest

  TEST_F(TReceiveBufTest, Test1) {
    TReceiveBuf<char> buf;
    ASSERT_EQ(buf.SpaceSize(), 0U);
    ASSERT_TRUE(buf.SpaceIsEmpty());
    ASSERT_EQ(buf.DataSize(), 0U);
    ASSERT_TRUE(buf.DataIsEmpty());

    buf.AddSpace(10);
    ASSERT_EQ(buf.SpaceSize(), 10U);
    ASSERT_FALSE(buf.SpaceIsEmpty());
    ASSERT_EQ(buf.DataSize(), 0U);
    ASSERT_TRUE(buf.DataIsEmpty());
    buf.EnsureSpace(9);
    ASSERT_EQ(buf.SpaceSize(), 10U);
    ASSERT_FALSE(buf.SpaceIsEmpty());
    buf.EnsureSpace(11);
    ASSERT_EQ(buf.SpaceSize(), 11U);
    ASSERT_FALSE(buf.SpaceIsEmpty());
    buf.EnsureDataPlusSpace(12);
    ASSERT_EQ(buf.SpaceSize(), 12U);
    ASSERT_FALSE(buf.SpaceIsEmpty());

    char *storage = buf.Space();
    std::string s("abcde");
    std::memcpy(storage, s.data(), s.size());
    buf.MarkSpaceConsumed(s.size());
    ASSERT_EQ(buf.SpaceSize(), 7U);
    ASSERT_FALSE(buf.SpaceIsEmpty());
    ASSERT_EQ(buf.DataSize(), 5U);
    ASSERT_FALSE(buf.DataIsEmpty());
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 5U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.MarkDataConsumed(3);
    s = "de";
    ASSERT_EQ(buf.SpaceSize(), 7U);
    ASSERT_FALSE(buf.SpaceIsEmpty());
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_FALSE(buf.DataIsEmpty());
    ASSERT_TRUE(buf.Data() == (storage + 3U));
    ASSERT_TRUE(buf.Space() == (storage + 5U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);
    buf.EnsureSpace(7);
    ASSERT_EQ(buf.SpaceSize(), 7U);
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_TRUE(buf.Data() == (storage + 3U));
    ASSERT_TRUE(buf.Space() == (storage + 5U));
    buf.EnsureDataPlusSpace(9);
    ASSERT_EQ(buf.SpaceSize(), 7U);
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_TRUE(buf.Data() == (storage + 3U));
    ASSERT_TRUE(buf.Space() == (storage + 5U));
    buf.AddSpace(0);
    ASSERT_EQ(buf.SpaceSize(), 7U);
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_TRUE(buf.Data() == (storage + 3U));
    ASSERT_TRUE(buf.Space() == (storage + 5U));

    buf.EnsureSpace(8);
    ASSERT_EQ(buf.SpaceSize(), 10U);
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 2U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.Space()[0] = 'f';
    buf.MarkSpaceConsumed(1);
    s.push_back('f');
    ASSERT_EQ(buf.SpaceSize(), 9U);
    ASSERT_EQ(buf.DataSize(), 3U);
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 3U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.MarkDataConsumed(1);
    s = "ef";
    ASSERT_EQ(buf.SpaceSize(), 9U);
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_TRUE(buf.Data() == (storage + 1U));
    ASSERT_TRUE(buf.Space() == (storage + 3U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.EnsureDataPlusSpace(12);
    ASSERT_EQ(buf.SpaceSize(), 10U);
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 2U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.MarkDataConsumed(1);
    s = "f";
    ASSERT_EQ(buf.SpaceSize(), 10U);
    ASSERT_EQ(buf.DataSize(), 1U);
    ASSERT_TRUE(buf.Data() == (storage + 1U));
    ASSERT_TRUE(buf.Space() == (storage + 2U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.AddSpace(1);
    ASSERT_EQ(buf.SpaceSize(), 11U);
    ASSERT_EQ(buf.DataSize(), 1U);
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 1U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    std::memcpy(buf.Space(), "gh", 2);
    buf.MarkSpaceConsumed(2);
    s = "fgh";
    ASSERT_EQ(buf.SpaceSize(), 9U);
    ASSERT_EQ(buf.DataSize(), 3U);
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 3U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.AddSpace(1024 - buf.SpaceSize() - buf.DataSize());
    storage = buf.Data();
    ASSERT_EQ(buf.SpaceSize(), 1024 - buf.DataSize());
    ASSERT_EQ(buf.DataSize(), 3U);
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 3U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.MarkDataConsumed(1);
    s = "gh";
    ASSERT_EQ(buf.SpaceSize(), 1021U);
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_TRUE(buf.Data() == (storage + 1));
    ASSERT_TRUE(buf.Space() == (storage + 3U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.EnsureSpace(8192 - buf.DataSize());
    storage = buf.Data();
    ASSERT_EQ(buf.SpaceSize(), 8192 - buf.DataSize());
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 2U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.MarkDataConsumed(buf.DataSize());
    ASSERT_EQ(buf.SpaceSize(), 8192U);
    ASSERT_EQ(buf.DataSize(), 0U);
    ASSERT_TRUE(buf.Space() == storage);

    s = "ijk";
    std::memcpy(buf.Space(), s.data(), s.size());
    buf.MarkSpaceConsumed(s.size());
    ASSERT_EQ(buf.SpaceSize(), 8192U - s.size());
    ASSERT_EQ(buf.DataSize(), 3U);
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 3U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.MarkDataConsumed(1);
    s = "jk";
    ASSERT_EQ(buf.SpaceSize(), 8192U - s.size() - 1U);
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_TRUE(buf.Data() == (storage + 1U));
    ASSERT_TRUE(buf.Space() == (storage + 3U));
    ASSERT_EQ(std::memcmp(buf.Data(), s.data(), s.size()), 0);

    buf.MarkDataConsumed(buf.DataSize());
    ASSERT_EQ(buf.SpaceSize(), 8192U);
    ASSERT_EQ(buf.DataSize(), 0U);
    ASSERT_TRUE(buf.Space() == storage);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
