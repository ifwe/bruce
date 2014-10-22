/* <bruce/msg_dispatch/kafka_dispatcher_api.h>

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

   Class defining Kafka dispatcher API for bruce daemon.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>

#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/metadata.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/api_defs.h>

namespace Bruce {

  namespace MsgDispatch {

    /* This class is just an interface.  Subclasses are TKafkaDispatcher (the
       real Kafka dispatcher) and TMockKafkaDispatcher (for testing). */
    class TKafkaDispatcherApi {
      NO_COPY_SEMANTICS(TKafkaDispatcherApi);

      public:
      using TState = TDispatcherState;

      using TShutdownStatus = TDispatcherShutdownStatus;

      virtual ~TKafkaDispatcherApi() noexcept { }

      virtual TState GetState() const = 0;

      virtual size_t GetBrokerCount() const = 0;

      /* Create the send and receive threads for individual brokers and start
         them running, but don't wait for them to finish initialization.  If a
         dispatcher thread fails to connect to a broker, it will hit the pause
         button as it normally would on events such as socket errors.  The
         dispatcher holds its own shared pointer to the passed in metadata,
         which tells it how many send and receive threads to create. */
      virtual void Start(const std::shared_ptr<TMetadata> &md) = 0;

      /* Transfer a single message to the send thread for the broker given by
         'broker_index', which specifies the index of the broker in the broker
         vector of the metadata (not the Kafka broker ID).  The message was not
         taken by the per topic batcher, so it goes the broker's combined
         topics batcher, which may either batch it or reject it (in which case
         the message is ready to send immediately). */
      virtual void Dispatch(TMsg::TPtr &&msg, size_t broker_index) = 0;

      /* Same as above, but bypasses broker-level batching. */
      virtual void DispatchNow(TMsg::TPtr &&msg, size_t broker_index) = 0;

      /* Transfer a batch of messages to the send thread for the broker given
         by 'broker_index', which specifies the index of the broker in the
         broker vector of the metadata (not the Kafka broker ID).  The messages
         bypass all batching at the broker level. */
      virtual void DispatchNow(std::list<std::list<TMsg::TPtr>> &&batch,
                               size_t broker_index) = 0;

      /* Slow shutdown is used when Bruce receives a shutdown request.  Tell
         the send and receive threads to start slow shutdown.  In the case
         where the dispatcher was just restarted due to a pause event and we
         are continuing a previously in progress slow shutdown, 'start_time'
         will be the time (in the past) when the slow shutdown started. */
      virtual void StartSlowShutdown(uint64_t start_time) = 0;

      /* Fast shutdown is used for metadata updates. */
      virtual void StartFastShutdown() = 0;

      /* When an error occurs that requires updating the metadata, the send or
         receive thread that detects the error hits the pause button, causing
         all other send and receive threads to exit.  A send thread that
         detects a pause event will finish any request it is sending (unless
         the time limit expires) and then shut down.  A receive thread that
         detects a pause event will keep receiving ACKs until it empties its
         ACK queue or the time limit expires.  The last send or receive thread
         to expire notifies the router thread by making the shutdown wait FD
         readable, and then the router thread calls JoinAll().

         Shutdown mechanism for metadata refresh is similar to shutdown on
         pause, except that it is initiated by router thread rather than a send
         or receive thread.

         Slow shutdown mechanism is also similar to shutdown on pause, except
         send thread continues sending until it empties its queue or the time
         limit runs out.

         Emergency stop should never happen in practice.  In this case, both
         send and receive threads exit immediately, even if they are in the
         middle of a send or receive. */

      /* Becomes readable when a send or receive thread hits the pause button.
       */
      virtual const Base::TFd &GetPauseFd() const = 0;

      /* Becomes readable when all threads have shut down (due to pause,
         metadata refresh, slow shutdown, emergency shutdown).  Then router
         thread can call JoinAll() without blocking.  The last send or receive
         thread to shut down makes this readable. */
      virtual const Base::TFd &GetShutdownWaitFd() const = 0;    

      /* Once the FD returned by GetShutdownWaitFd() becomes readable, this can
         be called without blocking indefinitely. */
      virtual void JoinAll() = 0;

      /* Get status after shutdown finished.  This can be called once JoinAll()
         has been called. */
      virtual TShutdownStatus GetShutdownStatus() const = 0;

      /* After shutdown is finished, get all messages waiting for an ACK from
         the given broker. */
      virtual std::list<std::list<TMsg::TPtr>>
      GetAckWaitQueueAfterShutdown(size_t broker_index) = 0;

      /* After shutdown is finished, get all messages waiting to be sent to the
         given broker. */
      virtual std::list<std::list<TMsg::TPtr>>
      GetSendWaitQueueAfterShutdown(size_t broker_index) = 0;

      /* For testing. */
      virtual size_t GetAckCount() const = 0;

      protected:
      TKafkaDispatcherApi() = default;
    };  // TKafkaDispatcherApi

  }  // MsgDispatch

}  // Bruce
