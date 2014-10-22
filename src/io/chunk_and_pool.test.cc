/* <io/chunk_and_pool->test.cc>
 
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
 
   Unit test for <io/chunk_and_pool.h>.
 */

#include <io/chunk_and_pool.h>
  
#include <cstring>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Io;

namespace {

  /* The fixture for testing chunk and pool stuff. */
  class TChunkAndPoolTest : public ::testing::Test {
    protected:
    TChunkAndPoolTest() {
    }

    virtual ~TChunkAndPoolTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TChunkAndPoolTest

  TEST_F(TChunkAndPoolTest, Growth) {
    auto pool = make_shared<TPool>();
    ASSERT_EQ(pool->GetFreeChunkCount(), 0U);
    auto chunk = pool->AcquireChunk();
    ASSERT_EQ(pool->GetFreeChunkCount(), 0U);
    chunk.reset();
    ASSERT_EQ(pool->GetFreeChunkCount(), 1U);
  }
  
  TEST_F(TChunkAndPoolTest, NoGrowth) {
    auto pool = make_shared<TPool>(
        TPool::TArgs(TPool::DefaultChunkSize, 1, 0));
    ASSERT_EQ(pool->GetFreeChunkCount(), 1U);
    auto chunk = pool->AcquireChunk();
    ASSERT_EQ(pool->GetFreeChunkCount(), 0U);
    bool caught;

    try {
      auto bad = pool->AcquireChunk();
      caught = false;
    } catch (const TPool::TOutOfChunksError &) {
      caught = true;
    }

    ASSERT_TRUE(caught);
    chunk.reset();
    ASSERT_EQ(pool->GetFreeChunkCount(), 1U);
  }
  
  TEST_F(TChunkAndPoolTest, ExtraGrowth) {
    auto pool = make_shared<TPool>(
        TPool::TArgs(TPool::DefaultChunkSize, 0, 10));
    ASSERT_EQ(pool->GetFreeChunkCount(), 0U);
    auto chunk = pool->AcquireChunk();
    ASSERT_EQ(pool->GetFreeChunkCount(), 9U);
    chunk.reset();
    ASSERT_EQ(pool->GetFreeChunkCount(), 10U);
  }
  
  TEST_F(TChunkAndPoolTest, NonRecycled) {
    static const char *expected_str = "mofo";
    const size_t expected_size = strlen(expected_str);
    static const size_t buffer_size = 100;
    char buffer[buffer_size];
    auto pool = make_shared<TPool>(
        TPool::TArgs(TPool::DefaultChunkSize, 0, 0));
    pool->EnqueueChunk(new TChunk(
        TChunk::Empty, buffer, buffer + buffer_size));
    auto chunk = pool->AcquireChunk();
    const char *csr = expected_str;
    size_t size = expected_size;
    ASSERT_TRUE(chunk->Store(csr, size));
    ASSERT_EQ(csr, expected_str + expected_size);
    ASSERT_FALSE(size);
    buffer[expected_size] = '\0';
    const char *start, *limit;
    chunk->GetData(start, limit);
    ASSERT_LE(start, limit);
    ASSERT_EQ(start, buffer);
    ASSERT_EQ(limit, buffer + expected_size);
    ASSERT_TRUE(strcmp(buffer, expected_str) == 0);
    ASSERT_EQ(pool->GetFreeChunkCount(), 0U);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
