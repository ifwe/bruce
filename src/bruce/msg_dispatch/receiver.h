/* <bruce/msg_dispatch/receiver.h>

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

   Receive thread for Kafka dispatcher.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <vector>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/debug/debug_logger.h>
#include <bruce/kafka_proto/produce_response_reader_api.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/metadata.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/api_defs.h>
#include <bruce/msg_dispatch/common.h>
#include <bruce/msg_dispatch/connection_shared_state.h>
#include <bruce/msg_dispatch/dispatcher_shared_state.h>
#include <bruce/util/poll_array.h>
#include <bruce/util/worker_thread.h>

namespace Bruce {

  namespace MsgDispatch {

    class TReceiver final : public Util::TWorkerThread {
      NO_COPY_SEMANTICS(TReceiver);

      public:
      using TShutdownStatus = TDispatcherShutdownStatus;

      TReceiver(size_t my_broker_index, TDispatcherSharedState &ds,
                TConnectionSharedState &cs,
                const Base::TFd &sender_shutdown_wait);

      virtual ~TReceiver() noexcept;

      /* This must be called immediately before starting the thread. */
      void SetMetadata(const std::shared_ptr<TMetadata> &md) {
        assert(this);
        assert(md);
        Metadata = md;
      }

      void StartSlowShutdown(uint64_t start_time);

      void StartFastShutdown();

      void WaitForShutdownAck();

      TShutdownStatus GetShutdownStatus() const {
        assert(this);
        return ShutdownStatus;
      }

      void ExtractMsgs();

      protected:
      virtual void Run() override;

      private:
      using TAction = KafkaProto::TWireProtocol::TAckResultAction;

      bool WaitForConnect();

      void InitMainLoopPollArray();

      /* Result is in milliseconds */
      int ComputeMainLoopPollTimeout(uint64_t now) const;

      void SetFastShutdownState();

      void HandlePauseDetected();

      void HandleShutdownRequest();

      void MakeSpaceInBuf(size_t min_size);

      bool DoSockRead(size_t min_size);

      bool TryReadProduceResponses();

      void ReportBadResponseTopic(const std::string &topic) const;

      void ReportBadResponsePartition(int32_t partition) const;

      void ReportShortResponsePartitionList(const std::string &topic) const;

      void ReportShortResponseTopicList() const;

      void ProcessResendList(std::list<std::list<TMsg::TPtr>> &&resend_list);

      void ProcessPauseAndResendMsgSet(std::list<TMsg::TPtr> &msg_set,
          const std::string &topic);

      bool ProcessOneAck(std::list<TMsg::TPtr> &msg_set, int16_t ack,
          const std::string &topic, int32_t partition);

      bool ProcessResponseAcks(TProduceRequest &request);

      bool ProcessSingleProduceResponse(size_t response_size);

      bool TryProcessProduceResponses();

      bool HandleSockReadReady();

      void DoRun();

      const size_t MyBrokerIndex;

      TDispatcherSharedState &Ds;

      TConnectionSharedState &Cs;

      /* Becomes readable when send thread shutdown finishes. */
      const Base::TFd &SenderShutdownWait;

      /* This is the minimum amount of data required to determine the size of a
         produce response. */
      const size_t SizeFieldSize;

      Base::TOpt<TInProgressShutdown> OptInProgressShutdown;

      enum class TMainLoopPollItem {
        SockRead = 0,
        ShutdownRequest = 1,
        PauseButton = 2,
        SendFinishedQueue = 3,
        SendThreadTerminated = 4
      };  // TMainLoopPollItem

      Util::TPollArray<TMainLoopPollItem, 5> MainLoopPollArray;

      std::shared_ptr<TMetadata> Metadata;

      std::unique_ptr<KafkaProto::TProduceResponseReaderApi> ResponseReader;

      std::vector<uint8_t> ReceiveBuf;

      size_t ReceiveBufDataOffset;

      size_t ReceiveBufDataSize;

      /* This is set to true when we have determined that there is enough data
         in 'ReceiveBuf' for at least one complete produce response.  It is set
         to false when this condition no longer holds. */
      bool FullResponseInReceiveBuf;

      std::list<TProduceRequest> AckWaitQueue;

      /* Messages to be rerouted after we pause and update metadata. */
      std::list<std::list<TMsg::TPtr>> RerouteAfterPause;

      bool PauseInProgress;

      bool SendThreadTerminated;

      bool Destroying;

      Base::TOpt<TShutdownCmd> OptShutdownCmd;

      /* Receive thread pushes this to acknowledge receipt of shutdown request
         (but may continue executing until shutdown finished). */
      Base::TEventSemaphore ShutdownAck;

      TShutdownStatus ShutdownStatus;

      Debug::TDebugLogger DebugLogger;
    };  // TReceiver

  }  // MsgDispatch

}  // Bruce
