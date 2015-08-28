/* <bruce/input_thread.h>

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

   Input thread for Bruce daemon.  Clients write messages to a UNIX domain
   datagram socket created by the input thread.  The input thread reads the
   messages and passes them to the router thread, which maps messages to Kafka
   brokers and forwards them to the dispatcher for sending.  As messages are
   successfully sent to brokers, the dispatcher returns them to the pool that
   the input thread allocated them from.  The pool enforces a global cap on the
   total amount of buffered message data.  Once the cap is reached, the input
   thread discards additional messages it reads from the UNIX domain socket.

   The intent is to keep the input thread as simple as possible and delegate
   more complex (and possibly time-consuming) behavior to the router thread and
   other threads managed by the router thread.  The input thread's only
   responsibilities are as follows:

     1.  Read messages from the UNIX domain socket and queue them for
         processing by the router thread.  Discard messages when the pool
         memory cap is reached.

     2.  Monitor a file descriptor that becomes readable when the main thread
         receives a shutdown request.  Once it becomes readable, the input
         thread terminates.

   It should be easy to visually inspect the input thread's implementation and
   verify that it will never force clients writing to the UNIX domain socket to
   block for a substantial length of time.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <netinet/in.h>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/config.h>
#include <bruce/msg.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/util/gate_put_api.h>
#include <bruce/util/worker_thread.h>
#include <capped/pool.h>
#include <socket/named_unix_socket.h>

namespace Bruce {

  class TInputThread final : public Util::TWorkerThread {
    NO_COPY_SEMANTICS(TInputThread);

    public:
    enum class TShutdownStatus {
      Normal,
      Error
    };  // TShutdownStatus

    TInputThread(const TConfig &config, Capped::TPool &pool,
        TMsgStateTracker &msg_state_tracker, TAnomalyTracker &anomaly_tracker,
        Util::TGatePutApi<TMsg::TPtr> &output_queue);

    virtual ~TInputThread() noexcept;

    /* Return a file descriptor that becomes readable when the input thread has
       finished its initialization and is open for business. */
    const Base::TFd &GetInitWaitFd() const {
      assert(this);
      return InitFinishedSem.GetFd();
    }

    TShutdownStatus GetShutdownStatus() const {
      assert(this);
      return ShutdownStatus;
    }

    /* Used for testing. */
    size_t GetMsgReceivedCount() const {
      assert(this);
      return MsgReceivedCount;
    }

    protected:
    virtual void Run() override;

    private:
    void DoRun();

    void OpenUnixSocket();

    TMsg::TPtr ReadOneMsg();

    void ForwardMessages();

    const TConfig &Config;

    bool Destroying;

    /* This becomes readable when the input thread has finished its
       initialization and is open for business. */
    Base::TEventSemaphore InitFinishedSem;

    /* After the input thread terminates, this indicates whether it terminated
       normally or with an error. */
    TShutdownStatus ShutdownStatus;

    /* Blocks for TBlob objects containing message data get allocated from
       here. */
    Capped::TPool &Pool;

    TMsgStateTracker &MsgStateTracker;

    /* For tracking discarded messages and possible duplicates. */
    TAnomalyTracker &AnomalyTracker;

    /* This is the UNIX domain datagram socket that web clients write to. */
    Socket::TNamedUnixSocket InputSocket;

    /* We read from the UNIX datagram socket into this buffer. */
    std::vector<uint8_t> InputBuf;

    /* Messages are queued here for the router thread. */
    Util::TGatePutApi<TMsg::TPtr> &OutputQueue;

    /* Used for testing. */
    std::atomic<size_t> MsgReceivedCount;
  };  // TInputThread

}  // Bruce
