/* <base/buf.test.cc>

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

   Unit test for <base/buf.h>.
 */

#include <base/buf.h>

#include <cstring>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

using namespace Base;

namespace {

  /* The fixture for testing class TBuf. */
  class TBufTest : public ::testing::Test {
    protected:
    TBufTest() {
    }

    virtual ~TBufTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TBufTest

  TEST_F(TBufTest, Test1) {
    std::vector<char> items;
    items.push_back('a');
    items.push_back('b');
    items.push_back('c');
    items.push_back('d');
    char *storage = &items[0];
    TBuf<char> buf_1(items);
    ASSERT_EQ(items.size(), 4U);
    ASSERT_EQ(buf_1.DataSize(), items.size());
    ASSERT_EQ(buf_1.SpaceSize(), 0U);
    ASSERT_EQ(std::memcmp(buf_1.Data(), &items[0], items.size()), 0);

    TBuf<char> buf_2(std::move(items));
    ASSERT_TRUE(items.empty());
    ASSERT_EQ(buf_2.DataSize(), 4U);
    ASSERT_EQ(buf_2.SpaceSize(), 0U);
    ASSERT_TRUE(buf_2.Data() == storage);
    buf_2.AddSpace(2);
    storage = buf_2.Data();
    ASSERT_EQ(buf_2.DataSize(), 4U);
    ASSERT_EQ(buf_2.SpaceSize(), 2U);
    ASSERT_EQ(std::memcmp(buf_2.Data(), "abcd", 4), 0);
    buf_2.MarkDataConsumed(1);
    ASSERT_EQ(buf_2.DataSize(), 3U);
    ASSERT_EQ(buf_2.SpaceSize(), 2U);

    TBuf<char> buf_3(std::move(buf_2));
    ASSERT_TRUE(buf_3.Data() == (storage + 1));
    ASSERT_EQ(buf_3.DataSize(), 3U);
    ASSERT_EQ(buf_3.SpaceSize(), 2U);
    ASSERT_EQ(std::memcmp(buf_3.Data(), "bcd", 3), 0);
    ASSERT_EQ(buf_2.DataSize(), 0U);
    ASSERT_EQ(buf_2.SpaceSize(), 0U);

    buf_2 = std::move(buf_3);
    ASSERT_TRUE(buf_2.Data() == (storage + 1));
    ASSERT_EQ(buf_2.DataSize(), 3U);
    ASSERT_EQ(buf_2.SpaceSize(), 2U);
    ASSERT_EQ(std::memcmp(buf_2.Data(), "bcd", 3), 0);
    ASSERT_EQ(buf_3.DataSize(), 0U);
    ASSERT_EQ(buf_3.SpaceSize(), 0U);

    buf_3.Swap(buf_2);
    ASSERT_TRUE(buf_3.Data() == (storage + 1));
    ASSERT_EQ(buf_3.DataSize(), 3U);
    ASSERT_EQ(buf_3.SpaceSize(), 2U);
    ASSERT_EQ(std::memcmp(buf_3.Data(), "bcd", 3), 0);
    ASSERT_EQ(buf_2.DataSize(), 0U);
    ASSERT_EQ(buf_2.SpaceSize(), 0U);

    TBuf<char> buf_4(buf_3);
    ASSERT_EQ(buf_4.DataSize(), 3U);
    ASSERT_EQ(buf_4.SpaceSize(), 2U);
    ASSERT_EQ(std::memcmp(buf_4.Data(), "bcd", 3), 0);
    ASSERT_EQ(buf_3.DataSize(), 3U);
    ASSERT_EQ(buf_3.SpaceSize(), 2U);
    ASSERT_EQ(std::memcmp(buf_3.Data(), "bcd", 3), 0);

    buf_3.Clear();
    ASSERT_TRUE(buf_3.Space() == storage);
    ASSERT_EQ(buf_3.DataSize(), 0U);
    ASSERT_EQ(buf_3.SpaceSize(), 6U);

    buf_3 = buf_4;
    ASSERT_EQ(buf_3.DataSize(), 3U);
    ASSERT_EQ(buf_3.SpaceSize(), 2U);
    ASSERT_EQ(std::memcmp(buf_3.Data(), "bcd", 3), 0);
    ASSERT_EQ(buf_4.DataSize(), 3U);
    ASSERT_EQ(buf_4.SpaceSize(), 2U);
    ASSERT_EQ(std::memcmp(buf_4.Data(), "bcd", 3), 0);

    buf_3.Clear();
    ASSERT_EQ(buf_3.SpaceSize(), 6U);
    storage = buf_3.Space();
    items = buf_3.TakeStorage();
    ASSERT_EQ(buf_3.DataSize(), 0U);
    ASSERT_EQ(buf_3.SpaceSize(), 0U);
    ASSERT_EQ(items.size(), 6U);
    ASSERT_TRUE(&items[0] == storage);

    const char *data = "efghij";
    std::memcpy(&items[0], data, std::strlen(data));
    buf_3 = std::move(items);
    ASSERT_TRUE(items.empty());
    ASSERT_EQ(buf_3.DataSize(), 6U);
    ASSERT_EQ(buf_3.SpaceSize(), 0U);
    ASSERT_TRUE(buf_3.Data() == storage);
    ASSERT_EQ(std::memcmp(buf_3.Data(), data, std::strlen(data)), 0);
  }

  TEST_F(TBufTest, Test2) {
    TBuf<int> buf;
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

    int *storage = buf.Space();
    std::vector<int> v(5);
    v[0] = 5;
    v[1] = 10;
    v[2] = 15;
    v[3] = 20;
    v[4] = 25;
    std::memcpy(storage, &v[0], v.size() * sizeof(decltype(v)::value_type));
    buf.MarkSpaceConsumed(v.size());
    ASSERT_EQ(buf.SpaceSize(), 7U);
    ASSERT_FALSE(buf.SpaceIsEmpty());
    ASSERT_EQ(buf.DataSize(), 5U);
    ASSERT_FALSE(buf.DataIsEmpty());
    ASSERT_TRUE(buf.Data() == storage);
    ASSERT_TRUE(buf.Space() == (storage + 5U));
    ASSERT_EQ(std::memcmp(buf.Data(), &v[0],
        v.size() * sizeof(decltype(v)::value_type)), 0);

    buf.MarkDataConsumed(3);
    v.resize(2);
    v[0] = 20;
    v[1] = 25;
    ASSERT_EQ(buf.SpaceSize(), 7U);
    ASSERT_FALSE(buf.SpaceIsEmpty());
    ASSERT_EQ(buf.DataSize(), 2U);
    ASSERT_FALSE(buf.DataIsEmpty());
    ASSERT_TRUE(buf.Data() == (storage + 3U));
    ASSERT_TRUE(buf.Space() == (storage + 5U));
    ASSERT_EQ(std::memcmp(buf.Data(), &v[0], v.size()), 0);
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
    ASSERT_EQ(std::memcmp(buf.Data(), &v[0],
        v.size() * sizeof(decltype(v)::value_type)), 0);
  }

  TEST_F(TBufTest, Test3) {
    TBuf<char> buf;
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
