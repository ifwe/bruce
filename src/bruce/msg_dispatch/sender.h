/* <bruce/msg_dispatch/sender.h>

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

   Send thread for Kafka dispatcher.
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
#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/debug/debug_logger.h>
#include <bruce/kafka_proto/produce_request_writer_api.h>
#include <bruce/metadata.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/api_defs.h>
#include <bruce/msg_dispatch/broker_msg_queue.h>
#include <bruce/msg_dispatch/common.h>
#include <bruce/msg_dispatch/connection_shared_state.h>
#include <bruce/msg_dispatch/dispatcher_shared_state.h>
#include <bruce/msg_dispatch/produce_request_factory.h>
#include <bruce/util/poll_array.h>
#include <bruce/util/time_util.h>
#include <bruce/util/worker_thread.h>

namespace Bruce {

  namespace MsgDispatch {

    class TSender final : public Util::TWorkerThread {
      NO_COPY_SEMANTICS(TSender);

      public:
      using TShutdownStatus = TDispatcherShutdownStatus;

      TSender(size_t my_broker_index, TDispatcherSharedState &ds,
              TConnectionSharedState &cs);

      virtual ~TSender() noexcept;

      /* This must be called immediately before starting the thread. */
      void SetMetadata(const std::shared_ptr<TMetadata> &md) {
        assert(this);
        assert(md);
        Metadata = md;
        RequestFactory.Init(Ds.CompressionConf, md);
      }

      void Dispatch(TMsg::TPtr &&msg) {
        assert(this);
        InputQueue.Put(Util::GetEpochMilliseconds(), std::move(msg));
        assert(!msg);
      }

      void DispatchNow(TMsg::TPtr &&msg) {
        assert(this);
        InputQueue.PutNow(Util::GetEpochMilliseconds(), std::move(msg));
        assert(!msg);
      }

      void DispatchNow(std::list<std::list<TMsg::TPtr>> &&batch) {
        assert(this);
        InputQueue.PutNow(Util::GetEpochMilliseconds(), std::move(batch));
        assert(batch.empty());
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
      enum class TSockOpResult {
        OkContinue,  // operation completed, keep executing
        OkStop,  // operation completed, stop executing
        Error  // operation failed (socket error)
      };  // TSockOpResult

      bool DoConnect();

      bool ConnectToBroker();

      void InitMainLoopPollArray();

      void InitSendLoopPollArray();

      /* Result is in milliseconds. */
      int ComputeShutdownTimeout(uint64_t now) const;

      /* Result is in milliseconds. */
      int ComputeMainLoopPollTimeout(uint64_t now, int shutdown_timeout) const;

      void CheckInputQueue(bool pop_sem);

      void SetFastShutdownState();

      void HandlePauseDetected(bool sending);

      bool HandleShutdownRequest(bool sending);

      bool WaitForSendReady();

      TSockOpResult SendOneProduceRequest();

      TSockOpResult HandleSockWriteReady();

      void DoRun();

      const size_t MyBrokerIndex;

      TDispatcherSharedState &Ds;

      TConnectionSharedState &Cs;

      Base::TOpt<TInProgressShutdown> OptInProgressShutdown;

      Base::TOpt<TMsg::TTimestamp> OptNextBatchExpiry;

      enum class TMainLoopPollItem {
        SockWrite = 0,
        ShutdownRequest = 1,
        PauseButton = 2,
        InputQueue = 3,
        ResendQueue = 4
      };  // TMainLoopPollItem

      Util::TPollArray<TMainLoopPollItem, 5> MainLoopPollArray;

      enum class TSendLoopPollItem {
        SockWrite = 0,
        ShutdownRequest = 1
      };  // TSendLoopPollItem

      Util::TPollArray<TSendLoopPollItem, 2> SendLoopPollArray;

      std::shared_ptr<TMetadata> Metadata;

      /* Messages waiting to be taken by send thread for sending. */
      TBrokerMsgQueue InputQueue;

      Debug::TDebugLogger DebugLogger;

      TProduceRequestFactory RequestFactory;

      std::vector<uint8_t> SendBuf;

      bool PauseInProgress;

      bool Destroying;

      Base::TOpt<TShutdownCmd> OptShutdownCmd;

      /* Send thread pushes this to acknowledge receipt of shutdown request
         (but may continue executing until shutdown finished). */
      Base::TEventSemaphore ShutdownAck;

      TShutdownStatus ShutdownStatus;

      /* This becomes known when we are about to start writing a produce
         request into our send buffer.  It becomes unknown when we have
         finished sending the request and handed it off to the receive thread.
       */
      Base::TOpt<TProduceRequest> CurrentRequest;
    };  // TSender

  }  // MsgDispatch

}  // Bruce
