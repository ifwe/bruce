/* <bruce/mock_kafka_server/prod_req/topic_group.h>

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

   The mock Kafka server uses this to represent a group of message sets with
   the same topic.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <string>
#include <vector>

#include <bruce/mock_kafka_server/prod_req/msg_set.h>

namespace Bruce {

  namespace MockKafkaServer {

    namespace ProdReq {

      class TTopicGroup final {
        public:
        explicit TTopicGroup(const std::string &topic)
            : Topic(topic) {
        }

        explicit TTopicGroup(std::string &&topic)
            : Topic(std::move(topic)) {
        }

        TTopicGroup(const TTopicGroup &) = default;

        TTopicGroup(TTopicGroup &&) = default;

        TTopicGroup &operator=(const TTopicGroup &) = default;

        TTopicGroup &operator=(TTopicGroup &&) = default;

        void AddMsgSet(const TMsgSet &msg_set) {
          assert(this);
          MsgSetVec.push_back(msg_set);
        }

        void AddMsgSet(TMsgSet &&msg_set) {
          assert(this);
          MsgSetVec.push_back(std::move(msg_set));
        }

        const std::string &GetTopic() const {
          assert(this);
          return Topic;
        }

        const std::vector<TMsgSet> &GetMsgSetVec() const {
          assert(this);
          return MsgSetVec;
        }

        private:
        std::string Topic;

        std::vector<TMsgSet> MsgSetVec;
      };  // TTopicGroup

    }  // ProdReq

  }  // MockKafkaServer

}  // Bruce
