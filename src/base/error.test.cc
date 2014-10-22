/* <base/error.test.cc>

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

   Unit test for <base/error.h>.
 */

#include <base/error.h>
  
#include <gtest/gtest.h>
  
using namespace Base;

namespace {

  /* The fixture for testing errors. */
  class TErrorTest : public ::testing::Test {
    protected:
    TErrorTest() {
    }

    virtual ~TErrorTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TErrorTest

  class TMyError : public TFinalError<TMyError> {
    public:
    TMyError(const TCodeLocation &code_location, const char *details = 0) {
      PostCtor(code_location, details);
    }

    TMyError(const TCodeLocation &code_location, const char *details_start,
        const char *details_end) {
      PostCtor(code_location, details_start, details_end);
    }
  };  // TMyError
  
  TEST_F(TErrorTest, Typical) {
    TMyError my_error(HERE);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
