/* <capped/blob.test.cc>
 
   ----------------------------------------------------------------------------
   Copyright 2013 if(we)

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
 
   Unit test for <capped/blob.h>, <capped/writer.h>, and <capped/reader.h>.
 */ 

#include <capped/blob.h>
#include <capped/writer.h>
#include <capped/reader.h>
  
#include <cstring>
#include <sstream>
#include <string>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Capped;

namespace {

  /* Sample data. */
  static const char *Str = "Mofo the Psychic Gorilla";
  static const size_t StrSize = strlen(Str);
  
  /* Convert a blob to a std string. */
  static string ToString(const TBlob &blob) {
    ostringstream strm;
    blob.ForEachBlock<ostream &>(
        [](const void *data, size_t size, ostream &os) -> bool {
          os.write(static_cast<const char *>(data), size);
          return true;
        },
        strm
    );
    return strm.str();
  }

  /* The fixture for testing class TBlob. */
  class TBlobTest : public ::testing::Test {
    protected:
    TBlobTest() {
    }

    virtual ~TBlobTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TBlobTest

  TEST_F(TBlobTest, Dumps) {
    /* Dump an empty blob, get an empty string. */
    TPool pool(256, 1, TPool::TSync::Unguarded);
    TBlob a;
    ASSERT_EQ(ToString(a), "");
    /* Dump a blob with a string in it, get the string back. */
    TWriter writer(&pool);
    writer.Write(Str, StrSize);
    a = writer.DraftBlob();
    ASSERT_EQ(ToString(a), Str);
  }
  
  TEST_F(TBlobTest, WriteAndRead) {
    /* Write a blob. */
    TPool pool(256, 1, TPool::TSync::Unguarded);
    TWriter writer(&pool);
    writer.Write(Str, StrSize);
    TBlob blob = writer.DraftBlob();
    ASSERT_EQ(blob.Size(), StrSize);
    /* Read the blob. */
    TReader reader(&blob);
    ASSERT_TRUE(reader);
    char str[StrSize + 1];
    reader.Read(str, StrSize);
    ASSERT_FALSE(reader);
    ASSERT_EQ(reader.GetBytesRemaining(), 0U);
    ASSERT_EQ(reader.GetBytesConsumed(), StrSize);
    str[StrSize] = '\0';
    ASSERT_EQ(strcmp(str, Str), 0);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
