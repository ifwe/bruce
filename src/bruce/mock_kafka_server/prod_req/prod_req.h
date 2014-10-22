/* <bruce/mock_kafka_server/prod_req/prod_req.h>

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

   The mock Kafka server uses this to represent a produce request.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

#include <bruce/mock_kafka_server/prod_req/topic_group.h>

namespace Bruce {

  namespace MockKafkaServer {

    namespace ProdReq {

      class TProdReq final {
        public:
        TProdReq(int32_t correlation_id, const std::string &client_id,
                 int16_t required_acks, int32_t replication_timeout)
            : CorrelationId(correlation_id),
              ClientId(client_id),
              RequiredAcks(required_acks),
              ReplicationTimeout(replication_timeout) {
        }

        TProdReq(int32_t correlation_id, std::string &&client_id,
                 int16_t required_acks, int32_t replication_timeout)
            : CorrelationId(correlation_id),
              ClientId(std::move(client_id)),
              RequiredAcks(required_acks),
              ReplicationTimeout(replication_timeout) {
        }

        TProdReq(const TProdReq &) = default;

        TProdReq(TProdReq &&) = default;

        TProdReq &operator=(const TProdReq &) = default;

        TProdReq &operator=(TProdReq &&) = default;

        void AddTopicGroup(const TTopicGroup &topic_group) {
          assert(this);
          TopicGroupVec.push_back(topic_group);
        }

        void AddTopicGroup(TTopicGroup &&topic_group) {
          assert(this);
          TopicGroupVec.push_back(std::move(topic_group));
        }

        int32_t GetCorrelationId() const {
          assert(this);
          return CorrelationId;
        }

        const std::string &GetClientId() const {
          assert(this);
          return ClientId;
        }

        int16_t GetRequiredAcks() const {
          assert(this);
          return RequiredAcks;
        }

        int32_t GetReplicationTimeout() const {
          assert(this);
          return ReplicationTimeout;
        }

        const std::vector<TTopicGroup> &GetTopicGroupVec() const {
          assert(this);
          return TopicGroupVec;
        }

        private:
        int32_t CorrelationId;

        std::string ClientId;

        int16_t RequiredAcks;

        int32_t ReplicationTimeout;

        std::vector<TTopicGroup> TopicGroupVec;
      };  // TProdReq

    }  // ProdReq

  }  // MockKafkaServer

}  // Bruce
