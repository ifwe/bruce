/* <bruce/msg_rate_limiter.test.cc>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 if(we)

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

   Unit test for <bruce/msg_rate_limiter.h>
 */

#include <bruce/msg_rate_limiter.h>

#include <bruce/conf/topic_rate_conf.h>

#include <gtest/gtest.h>

using namespace Bruce;
using namespace Bruce::Conf;

namespace {

  /* The fixture for testing class TMsgRateLimiter. */
  class TMsgRateLimiterTest : public ::testing::Test {
    protected:
    TMsgRateLimiterTest() {
    }

    virtual ~TMsgRateLimiterTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TMsgRateLimiterTest

  TEST_F(TMsgRateLimiterTest, BasicTest) {
    TTopicRateConf::TBuilder b;
    b.AddBoundedNamedConfig("default_conf", 2, 4);
    b.AddBoundedNamedConfig("conf1", 1, 0);
    b.AddBoundedNamedConfig("conf2", 1, 1);
    b.AddBoundedNamedConfig("conf3", 1, 2);
    b.AddBoundedNamedConfig("conf4", 10, 0);
    b.AddBoundedNamedConfig("conf5", 20, 3);
    b.AddUnlimitedNamedConfig("conf6");
    b.SetDefaultTopicConfig("default_conf");
    b.SetTopicConfig("topic1", "conf1");
    b.SetTopicConfig("topic2", "conf2");
    b.SetTopicConfig("topic3", "conf3");
    b.SetTopicConfig("topic4", "conf4");
    b.SetTopicConfig("topic5", "conf5");
    b.SetTopicConfig("topic6", "default_conf");
    b.SetTopicConfig("topic7", "conf6");
    TTopicRateConf conf = b.Build();
    TMsgRateLimiter lim(conf);

    ASSERT_FALSE(lim.WouldExceedLimit("blah", 0));
    ASSERT_FALSE(lim.WouldExceedLimit("blah", 1));
    ASSERT_FALSE(lim.WouldExceedLimit("blah", 1));
    ASSERT_FALSE(lim.WouldExceedLimit("blah", 1));
    ASSERT_TRUE(lim.WouldExceedLimit("blah", 1));

    ASSERT_FALSE(lim.WouldExceedLimit("blah", 2));
    ASSERT_FALSE(lim.WouldExceedLimit("duh", 2));
    ASSERT_FALSE(lim.WouldExceedLimit("duh", 2));
    ASSERT_FALSE(lim.WouldExceedLimit("duh", 2));
    ASSERT_FALSE(lim.WouldExceedLimit("duh", 2));
    ASSERT_TRUE(lim.WouldExceedLimit("duh", 2));

    ASSERT_FALSE(lim.WouldExceedLimit("blah", 2));
    ASSERT_FALSE(lim.WouldExceedLimit("blah", 2));
    ASSERT_FALSE(lim.WouldExceedLimit("blah", 2));
    ASSERT_TRUE(lim.WouldExceedLimit("blah", 3));

    ASSERT_TRUE(lim.WouldExceedLimit("topic1", 4));

    ASSERT_FALSE(lim.WouldExceedLimit("topic2", 4));
    ASSERT_TRUE(lim.WouldExceedLimit("topic2", 4));

    ASSERT_FALSE(lim.WouldExceedLimit("topic3", 5));
    ASSERT_TRUE(lim.WouldExceedLimit("topic4", 5));
    ASSERT_FALSE(lim.WouldExceedLimit("topic3", 5));
    ASSERT_TRUE(lim.WouldExceedLimit("topic3", 5));

    ASSERT_FALSE(lim.WouldExceedLimit("topic5", 10));
    ASSERT_FALSE(lim.WouldExceedLimit("topic5", 20));
    ASSERT_FALSE(lim.WouldExceedLimit("topic5", 29));
    ASSERT_TRUE(lim.WouldExceedLimit("topic5", 29));

    ASSERT_FALSE(lim.WouldExceedLimit("topic5", 30));
    ASSERT_FALSE(lim.WouldExceedLimit("topic5", 40));
    ASSERT_FALSE(lim.WouldExceedLimit("topic5", 49));
    ASSERT_FALSE(lim.WouldExceedLimit("topic5", 50));
    ASSERT_FALSE(lim.WouldExceedLimit("topic5", 60));

    for (size_t i = 0; i < 25; ++i) {
      ASSERT_FALSE(lim.WouldExceedLimit("topic7", 65));
    }

    ASSERT_FALSE(lim.WouldExceedLimit("topic5", 68));
    ASSERT_TRUE(lim.WouldExceedLimit("topic5", 69));

    ASSERT_FALSE(lim.WouldExceedLimit("blah", 70));
    ASSERT_FALSE(lim.WouldExceedLimit("blah", 71));
    ASSERT_FALSE(lim.WouldExceedLimit("blah", 71));
    ASSERT_FALSE(lim.WouldExceedLimit("blah", 71));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 71));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 71));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 71));
    ASSERT_TRUE(lim.WouldExceedLimit("blah", 71));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 71));
    ASSERT_TRUE(lim.WouldExceedLimit("topic6", 71));
    ASSERT_TRUE(lim.WouldExceedLimit("topic6", 72));

    /* Since the interval width for topic6 is 2, and the first message we sent
       to that topic was at time 71, all future intervals for that topic should
       start on an odd numbered time value.  Therefore the messages below sent
       to topic6 at time 172 will be in a different interval from those sent at
       time 173, and the messages at 173 will therefore not be discarded. */
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 172));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 172));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 172));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 172));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 173));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 173));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 174));
    ASSERT_FALSE(lim.WouldExceedLimit("topic6", 174));
    ASSERT_TRUE(lim.WouldExceedLimit("topic6", 174));
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
