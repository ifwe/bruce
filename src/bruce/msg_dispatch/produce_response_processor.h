/* <bruce/msg_dispatch/produce_response_processor.h>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 if(we)
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

   Class for processing a single produce response.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <string>
#include <utility>

#include <base/no_copy_semantics.h>
#include <bruce/debug/debug_logger.h>
#include <bruce/kafka_proto/produce_response_reader_api.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/common.h>
#include <bruce/msg_dispatch/dispatcher_shared_state.h>

namespace Bruce {

  namespace MsgDispatch {

    class TProduceResponseProcessor final {
      NO_COPY_SEMANTICS(TProduceResponseProcessor);

      public:
      /* Indicates action that caller should take as a result of processing
         produce response. */
      enum class TAction {
        /* Keep running normally. */
        KeepRunning,

        /* Initiate pause but keep processing responses until fast shutdown
           time limit expiry. */
        PauseAndDeferFinish,

        /* Initiate pause and stop running immediately. */
        PauseAndFinishNow
      };  // TAction

      TProduceResponseProcessor(
          KafkaProto::TProduceResponseReaderApi &response_reader,
          TDispatcherSharedState &ds, Debug::TDebugLogger &debug_logger,
          unsigned long my_broker_index, long my_broker_id)
          : MyBrokerIndex(my_broker_index),
            MyBrokerId(my_broker_id),
            Ds(ds),
            ResponseReader(response_reader),
            DebugLogger(debug_logger) {
      }

      ~TProduceResponseProcessor() noexcept {
        /* Not strictly necessary, but it's nice not to leave potentially
           dangling pointers inside the reader. */
        ResponseReader.Clear();
      }

      /* Process a produce response whose data is contained in the buffer given
         by 'response_buf' and 'response_buf_size'.  'request' contains the
         produce request that we are processing the response for.  On return,
         'request' will be empty.  The return value indicates the next action
         the caller should take.  On return, methods TakeMsgsWithoutAcks(),
         TakeImmediateResendAckMsgs(), and TakePauseAndResendAckMsgs() may
         be called to retrieve internally stored messages that did not receive
         successful ACKs. */
      TAction ProcessResponse(TProduceRequest &request, uint8_t *response_buf,
          size_t response_buf_size);

      /* Return all messages from the input produce request that we were unable
         to obtain any kind of ACK for.  These will need to be rerouted after
         the dispatcher shuts down and restarts.  This method doesn't need to
         be called unless ProcessResponse() returned
         TAction::PauseAndFinishNow. */
      std::list<std::list<TMsg::TPtr>> TakeMsgsWithoutAcks() {
        assert(this);
        return std::move(MsgsWithoutAcks);
      }

      /* Return all messages from the input produce request for which we got an
         error ACK indicating that they need to be rerouted after the
         dispatcher shuts down and restarts.  This method doesn't need to
         be called unless ProcessResponse() returned
         TAction::PauseAndFinishNow or TAction::PauseAndDeferFinish. */
      std::list<std::list<TMsg::TPtr>> TakePauseAndResendAckMsgs() {
        assert(this);
        return std::move(PauseAndResendAckMsgs);
      }

      /* Return all messages from the input produce request for which we got an
         error ACK indicating that the message can be resent immediately
         without rerouting based on new metadata.  This method must be called
         regardless of what value ProcessResponse() returned. */
      std::list<std::list<TMsg::TPtr>> TakeImmediateResendAckMsgs() {
        assert(this);
        return std::move(ImmediateResendAckMsgs);
      }

      private:
      using TAckResultAction = KafkaProto::TWireProtocol::TAckResultAction;

      void ReportBadResponseTopic(const std::string &topic) const;

      void ReportBadResponsePartition(int32_t partition) const;

      void ReportShortResponsePartitionList(const std::string &topic) const;

      void ReportShortResponseTopicList() const;

      void CountFailedDeliveryAttempt(std::list<TMsg::TPtr> &msg_set,
          const std::string &topic);

      void ProcessImmediateResendMsgSet(std::list<TMsg::TPtr> &&msg_set,
          const std::string &topic);

      void ProcessPauseAndResendMsgSet(std::list<TMsg::TPtr> &&msg_set,
          const std::string &topic);

      void ProcessNoAckMsgs(TAllTopics &all_topics);

      bool ProcessOneAck(std::list<TMsg::TPtr> &&msg_set, int16_t ack,
          const std::string &topic, int32_t partition);

      TAction ProcessResponseAcks(TProduceRequest &request);

      const unsigned long MyBrokerIndex;

      const long MyBrokerId;

      TDispatcherSharedState &Ds;

      KafkaProto::TProduceResponseReaderApi &ResponseReader;

      Debug::TDebugLogger &DebugLogger;

      /* Messages that we were unable to obtain any kind of ACK for. */
      std::list<std::list<TMsg::TPtr>> MsgsWithoutAcks;

      /* Messages that got an error ACK indicating that retransmission should
         not be attempted without rerouting based on new metadata.  These go
         back to the router thread once dispatcher shutdown has finished. */
      std::list<std::list<TMsg::TPtr>> PauseAndResendAckMsgs;

      /* Messages that got an error ACK indicating that retransmission is
         possible without updating metadata and rerouting. */
      std::list<std::list<TMsg::TPtr>> ImmediateResendAckMsgs;
    };  // TProduceResponseProcessor

  }  // MsgDispatch

}  // Bruce
