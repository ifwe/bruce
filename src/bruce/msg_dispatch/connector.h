/* <bruce/msg_dispatch/connector.h>

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

   Dispatcher class representing a connection to a Kafka broker.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <utility>
#include <vector>

#include <base/buf.h>
#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/debug/debug_logger.h>
#include <bruce/kafka_proto/produce_request_writer_api.h>
#include <bruce/kafka_proto/produce_response_reader_api.h>
#include <bruce/metadata.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/api_defs.h>
#include <bruce/msg_dispatch/broker_msg_queue.h>
#include <bruce/msg_dispatch/common.h>
#include <bruce/msg_dispatch/dispatcher_shared_state.h>
#include <bruce/msg_dispatch/produce_request_factory.h>
#include <bruce/util/poll_array.h>
#include <thread/fd_managed_thread.h>

namespace Bruce {

  namespace MsgDispatch {

    /* This class handles a TCP connection between Bruce and a single Kafka
       broker.  It uses a single thread for building and sending produce
       requests, as well as receiving and processing produce responses.  */
    class TConnector final : public Thread::TFdManagedThread {
      NO_COPY_SEMANTICS(TConnector);

      public:
      TConnector(size_t my_broker_index, TDispatcherSharedState &ds);

      virtual ~TConnector() noexcept;

      /* This must be called before starting the thread. */
      void SetMetadata(const std::shared_ptr<TMetadata> &md);

      void Dispatch(TMsg::TPtr &&msg) {
        assert(this);
        InputQueue.Put(Base::GetEpochMilliseconds(), std::move(msg));
        assert(!msg);
      }

      void DispatchNow(TMsg::TPtr &&msg) {
        assert(this);
        InputQueue.PutNow(Base::GetEpochMilliseconds(), std::move(msg));
        assert(!msg);
      }

      void DispatchNow(std::list<std::list<TMsg::TPtr>> &&batch) {
        assert(this);
        InputQueue.PutNow(Base::GetEpochMilliseconds(), std::move(batch));
        assert(batch.empty());
      }

      void StartSlowShutdown(uint64_t start_time);

      void StartFastShutdown();

      void WaitForShutdownAck();

      void CleanupAfterJoin();

      bool ShutdownWasOk() const {
        assert(this);
        return OkShutdown;
      }

      std::list<std::list<TMsg::TPtr>> GetNoAckQueueAfterShutdown() {
        assert(this);
        return std::move(NoAckAfterShutdown);
      }

      std::list<std::list<TMsg::TPtr>> GetSendWaitQueueAfterShutdown() {
        assert(this);
        return std::move(SendWaitAfterShutdown);
      }

      protected:
      virtual void Run() override;

      private:
      const TMetadata::TBroker &MyBroker() const {
        assert(this);
        return Metadata->GetBrokers()[MyBrokerIndex];
      }

      long MyBrokerId() const {
        assert(this);
        return MyBroker().GetId();
      }

      bool SendInProgress() const {
        assert(this);
        return !SendBuf.DataIsEmpty();
      }

      bool DoConnect();

      bool ConnectToBroker();

      void SetFastShutdownState();

      void HandleShutdownRequest();

      void SetPauseInProgress();

      void HandlePauseDetected();

      void CheckInputQueue(uint64_t now, bool pop_sem);

      bool TrySendProduceRequest();

      bool HandleSockWriteReady();

      bool DoSockRead(size_t min_size);

      bool TryReadProduceResponses();

      bool ProcessSingleProduceResponse(size_t response_size);

      bool TryProcessProduceResponses();

      bool HandleSockReadReady();

      bool PrepareForPoll(uint64_t now, int &poll_timeout);

      void DoRun();

      /* TCP socket to Kafka broker. */
      Base::TFd Sock;

      /* After connector thread is shut down, all messages waiting to be sent
         (including those waiting to be resent due to an error ACK) are moved
         to this list. */
      std::list<std::list<TMsg::TPtr>> SendWaitAfterShutdown;

      /* After connector thread is shut down, all sent messages waiting for an
         ACK are moved to this list. */
      std::list<std::list<TMsg::TPtr>> NoAckAfterShutdown;

      /* The TKafkaDispatcher object maintains a vector of TConnector objects,
         one for each active broker.  Here we store the vector index of this
         TConnector. */
      const size_t MyBrokerIndex;

      /* This is the minimum amount of data required to determine the size of a
         produce response. */
      const size_t SizeFieldSize;

      /* Dispatcher state shared by all TConnector objects. */
      TDispatcherSharedState &Ds;

      Debug::TDebugLogger DebugLoggerSend;

      Debug::TDebugLogger DebugLoggerReceive;

      /* Becomes known when a fast or slow dispatcher shutdown is in progress.
         A fast shutdown occurs when metadata needs to be updated and the
         dispatcher needs to be restarted with new metadata.  A slow shutdown
         occurs when Bruce is shutting down.

         During a fast shutdown, we finish sending any partially sent request
         (but don't send any more requests) and attempt to read and process
         responses for all outstanding requests, until the fast shutdown time
         limit expires.

         During a slow shutdown, we attempt to send all outstanding requests,
         and receive and process all outstanding responses, until the slow
         shutdown time limit expires. */
      Base::TOpt<TInProgressShutdown> OptInProgressShutdown;

      enum class TMainLoopPollItem {
        SockIo = 0,
        ShutdownRequest = 1,
        PauseButton = 2,
        InputQueue = 3
      };  // TMainLoopPollItem

      /* Used for poll() system call in connector thread main loop. */
      Util::TPollArray<TMainLoopPollItem, 4> MainLoopPollArray;

      std::shared_ptr<TMetadata> Metadata;

      /* This becomes known when a batch time limit is set for messages being
         batched inside the 'InputQueue' member for this connector thread (see
         below).  Once the time limit expires, we extract all ready messages
         from 'InputQueue' and add them to our 'RequestFactory' member, where
         they are immediately available to be bundled into a produce request.
       */
      Base::TOpt<TMsg::TTimestamp> OptNextBatchExpiry;

      /* Messages are deposited here by the router thread, where they wait to
         be claimed by this connector thread for sending.  Some may be
         immediately ready to send, and others may be need to be batched at the
         broker level. */
      TBrokerMsgQueue InputQueue;

      /* Contains messages ready to be sent immediately, and handles the
         details of bundling them into produce requests. */
      TProduceRequestFactory RequestFactory;

      /* Produce requests are serialized into this buffer immediately before
         being written to the socket.  Buffer never contains more than one
         request at a time. */
      Base::TBuf<uint8_t> SendBuf;

      /* A true value indicates that a pause is in progress and this thread is
         gracefully shutting down.  A connector thread triggers a pause when it
         receives a response from Kafka indicating that the metadata is
         outdated, or encounters a serious error such as a socket error that
         prevents further communication with the broker.  Whe one connector
         triggers a pause, all other connectors react by entering the fast
         shutdown state.  Once all connector threads have terminated, the
         router thread updates the metadata and restarts the dispatcher.  This
         flag is set only in the following cases:

             - The connector detects a pause initiated by another connector,
               and reacts by entering fast shutdown.

             - The connector receives an error ACK in a produce response,
               indicating that metadata needs updating, and reacts by entering
               fast shutdown.

         In both cases, communication with the broker is still possible, so we
         want to continue processing produce responses until there are no more
         to process, or the fast shutdown time limit expires.  In the case of a
         socket error or receipt of unexpected response data from Kafka
         (for instance, a mismatched correlation ID), reliable communication is
         no longer possible, so the connector shuts down immediately without
         setting this flag. */
      bool PauseInProgress;

      /* This flag is only set on destructor invocation.  If the connector
         thread is still executing at this point, then a fatal error has
         occurred, so it must shut down immediately. */
      bool Destroying;

      /* Known when the router thread has sent a fast or slow shutdown command,
         which the connector thread has not yet received.  When the connector
         receives the command, it sets 'OptInProgressShutdown' accordingly and
         returns this value to the unknown state. */
      Base::TOpt<TShutdownCmd> OptShutdownCmd;

      /* Connector thread pushes this to acknowledge receipt of shutdown
         request (but continues executing until shutdown finished). */
      Base::TEventSemaphore ShutdownAck;

      /* This becomes known when we are about to start writing a produce
         request into our send buffer.  It becomes unknown when we have
         finished sending the request and are waiting for the response. */
      Base::TOpt<TProduceRequest> CurrentRequest;

      /* This handles the details of reading and processing produce responses.
       */
      std::unique_ptr<KafkaProto::TProduceResponseReaderApi> ResponseReader;

      /* We read produce response data from the socket into this buffer.  For
         eficiency, we attempt to do large reads.  Therefore at any given
         instant, the buffer may contain data belonging to multiple produce
         responses. */
      Base::TBuf<uint8_t> ReceiveBuf;

      /* FIFO queue of sent produce requests waiting for responses. */
      std::list<TProduceRequest> AckWaitQueue;

      /* Messages that we got no ACK for, and need to be rerouted after pause
         finishes.  The router thread will reroute these and report them as
         possible duplicates. */
      std::list<std::list<TMsg::TPtr>> NoAckAfterPause;

      /* Messages for which we got an error ACK that requires rerouting based
         on new metadata.  The router thread will handle these after restarting
         the dispatcher. */
      std::list<std::list<TMsg::TPtr>> GotAckAfterPause;

      /* After connector has shut down, this is true if the thread shut down
         normally, or false otherwise.  A false value indicates a socket error
         or some other type of serious error (such as correlation ID mismatch).
         If the thread shut down due to an error ack or detection of a pause
         initiated by another connector, this will be true. */
      bool OkShutdown;
    };  // TConnector

  }  // MsgDispatch

}  // Bruce
