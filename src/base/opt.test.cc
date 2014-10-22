/* <base/opt.test.cc>
 
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
 
   Unit test for <base/opt.h>.
 */
  
#include <base/opt.h>
  
#include <string>
  
#include <base/no_copy_semantics.h>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace Base;
 
namespace {
 
  class TToken {
    public:
  
    TToken()
        : Dummy(0) {
      Push('D');
    }
  
    TToken(TToken &&that)
        : Dummy(that.Dummy) {
      Push('M');
    }
  
    TToken(const TToken &that)
        : Dummy(that.Dummy) {
      Push('C');
    }
  
    ~TToken() {
      Push('X');
    }
  
    TToken &operator=(TToken &&that) {
      Push('S');
      swap(Dummy, that.Dummy);
      return *this;
    }
  
    TToken &operator=(const TToken &that) {
      Push('A');
      return *this = TToken(that);
    }
  
    static string LatchOps() {
      string temp = Ops;
      Ops.clear();
      return temp;
    }
  
    int Dummy;
  
    private:
  
    static void Push(char op) {
      Ops.push_back(op);
    }
  
    static string Ops;
  
  };
  
  string TToken::Ops;
  
  ostream &operator<<(ostream &strm, const TToken &that) {
    return strm << "TToken(" << that.Dummy << ')';
  }

  /* The fixture for testing class TOpt. */
  class TOptTest : public ::testing::Test {
    protected:
    TOptTest() {
    }

    virtual ~TOptTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TOptTest
  
  TEST_F(TOptTest, KnownVsUnknown) {
    TOpt<int> opt;
    ASSERT_FALSE(opt);
    ASSERT_FALSE(opt.IsKnown());
    ASSERT_TRUE(opt.IsUnknown());
    opt = 0;
    ASSERT_TRUE(opt);
    ASSERT_TRUE(opt.IsKnown());
    ASSERT_FALSE(opt.IsUnknown());
    opt.Reset();
    ASSERT_FALSE(opt);
    ASSERT_FALSE(opt.IsKnown());
    ASSERT_TRUE(opt.IsUnknown());
    ASSERT_FALSE(*opt.Unknown);
  }
  
  TEST_F(TOptTest, TokenBasics) {
    /* extra */ {
      TToken a;
      ASSERT_EQ(TToken::LatchOps(), string("D"));
      TToken b = a;
      ASSERT_EQ(TToken::LatchOps(), string("C"));
      TToken c = move(b);
      ASSERT_EQ(TToken::LatchOps(), string("M"));
      a = move(b);
      ASSERT_EQ(TToken::LatchOps(), string("S"));
      a = c;
      ASSERT_EQ(TToken::LatchOps(), string("ACSX"));
    }
    ASSERT_EQ(TToken::LatchOps(), string("XXX"));
  }
  
  TEST_F(TOptTest, MoveAndCopy) {
    TToken token;
    TOpt<TToken> a, b = token, c = move(b);
    ASSERT_EQ(TToken::LatchOps(), string("DCMX"));
    ASSERT_FALSE(a);
    ASSERT_FALSE(b);
    ASSERT_TRUE(c);
    a = move(c);
    ASSERT_EQ(TToken::LatchOps(), string("MX"));
    ASSERT_TRUE(a);
    ASSERT_FALSE(c);
    b = a;
    ASSERT_EQ(TToken::LatchOps(), string("CMX"));
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    TOpt<TToken> d = b;
    ASSERT_EQ(TToken::LatchOps(), string("C"));
    ASSERT_TRUE(d);
    d.MakeKnown();
    ASSERT_EQ(TToken::LatchOps(), string(""));
    ASSERT_TRUE(d);
    c.MakeKnown();
    ASSERT_EQ(TToken::LatchOps(), string("D"));
    ASSERT_TRUE(c);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
