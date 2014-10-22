/* <bruce/test_util/mock_kafka_dispatcher.h>

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

   Mock Kafka dispatcher class for unit testing.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_set>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/batch/batch_config.h>
#include <bruce/config.h>
#include <bruce/debug/debug_setup.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/metadata.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/kafka_dispatcher_api.h>
#include <bruce/msg_state_tracker.h>

namespace Bruce {

  namespace TestUtil {

    /* Mock Kafka dispatcher class for unit testing. */
    class TMockKafkaDispatcher final
        : public MsgDispatch::TKafkaDispatcherApi {
      NO_COPY_SEMANTICS(TMockKafkaDispatcher);

      public:
      TMockKafkaDispatcher(const TConfig &config,
          const KafkaProto::TWireProtocol &kafka_protocol,
          TMsgStateTracker &msg_state_tracker,
          TAnomalyTracker &anomaly_tracker,
          const Batch::TBatchConfig &batch_config,
          std::unordered_set<std::string> &&batch_topic_filter,
          bool batch_topic_filter_exclude, size_t produce_request_data_limit,
          const Debug::TDebugSetup &debug_setup);

      virtual ~TMockKafkaDispatcher() noexcept { }

      virtual TState GetState() const override;

      virtual size_t GetBrokerCount() const override;

      virtual void Start(const std::shared_ptr<TMetadata> &md) override;

      virtual void Dispatch(std::list<std::list<TMsg::TPtr>> &&batch,
                            size_t broker_index) override;

      virtual void Dispatch(TMsg::TPtr &&msg, size_t broker_index) override;

      virtual void StartSlowShutdown(uint64_t start_time) override;

      virtual void StartFastShutdown() override;

      virtual const Base::TFd &GetPauseFd() const override;

      virtual const Base::TFd &GetShutdownWaitFd() const override;

      virtual void JoinAll() override;

      virtual TShutdownStatus GetShutdownStatus() const override;

      virtual std::list<std::list<TMsg::TPtr>>
      GetAckWaitQueueAfterShutdown(size_t broker_index) override;

      virtual std::list<std::list<TMsg::TPtr>>
      GetSendWaitQueueAfterShutdown(size_t broker_index) override;

      virtual size_t GetAckCount() const override;
    };  // TMockKafkaDispatcher

  }  // TestUtil

}  // Bruce
