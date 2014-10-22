/* <base/indent.test.cc>
 
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
 
   Unit test for <base/indent.h>.
 */

#include <base/indent.h>
  
#include <sstream>
  
#include <gtest/gtest.h>
  
using namespace Base;
 
namespace {

  /* The fixture for testing class TIndent. */
  class TIndentTest : public ::testing::Test {
    protected:
    TIndentTest() {
    }

    virtual ~TIndentTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TIndentTest

  TEST_F(TIndentTest, Test1) {
    std::string x_rated;
    TIndent ind1(x_rated, TIndent::StartAt::Indented, 3, 'x');
    ASSERT_EQ(ind1.Get(), "xxx");
    std::ostringstream oss1;
    oss1 << ind1;
    std::string s = oss1.str();
    ASSERT_EQ(s, "xxx");
  
    {
      TIndent ind2(ind1);
      ASSERT_EQ(ind2.Get(), "xxxxxx");
  
      {
        TIndent ind3(ind2, 4, 'y');
        ASSERT_EQ(ind3.Get(), "xxxxxxyyyy");
  
        {
          TIndent ind4(ind3);
          ASSERT_EQ(ind4.Get(), "xxxxxxyyyyyyyy");
          ind4.AddOnce("zz");
          ASSERT_EQ(ind4.Get(), "xxxxxxyyyyyyyyzz");
  
          {
            TIndent ind5(ind4);
            ASSERT_EQ(ind5.Get(), "xxxxxxyyyyyyyyzzyyyy");
  
            {
              TIndent ind6(ind5);
              ASSERT_EQ(ind6.Get(), "xxxxxxyyyyyyyyzzyyyyyyyy");
              ind6.AddOnce("splat");
              ASSERT_EQ(ind6.Get(), "xxxxxxyyyyyyyyzzyyyyyyyysplat");
  
              {
                TIndent ind7(ind6);
                ASSERT_EQ(ind6.Get(), "xxxxxxyyyyyyyyzzyyyyyyyysplatyyyy");
              }
  
              ASSERT_EQ(ind6.Get(), "xxxxxxyyyyyyyyzzyyyyyyyysplat");
            }
  
            ASSERT_EQ(ind5.Get(), "xxxxxxyyyyyyyyzzyyyy");
          }
  
          ASSERT_EQ(ind4.Get(), "xxxxxxyyyyyyyyzz");
        }
  
        ASSERT_EQ(ind3.Get(), "xxxxxxyyyy");
      }
  
      ASSERT_EQ(ind2.Get(), "xxxxxx");
    }
  
    ASSERT_EQ(ind1.Get(), "xxx");
  }
  
  TEST_F(TIndentTest, StartAtZeroTest) {
    std::string x_rated;
    TIndent ind1(x_rated, TIndent::StartAt::Zero, 3, 'x');
    ASSERT_EQ(ind1.Get(), "");
    std::ostringstream oss1;
    oss1 << ind1;
    std::string s = oss1.str();
    ASSERT_EQ(s, "");
  
    {
      TIndent ind2(ind1);
      ASSERT_EQ(ind2.Get(), "xxx");
  
      {
        TIndent ind3(ind2, 4, 'y');
        ASSERT_EQ(ind3.Get(), "xxxyyyy");
      }
  
      ASSERT_EQ(ind2.Get(), "xxx");
    }
  
    ASSERT_EQ(ind1.Get(), "");
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
