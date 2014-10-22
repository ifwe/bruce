/* <bruce/web_interface.h>

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

   This implements bruce's HTTP interface, used mostly for status monitoring.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <ostream>
#include <string>

#include <netinet/in.h>

#include <base/event_semaphore.h>
#include <base/indent.h>
#include <base/no_copy_semantics.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/debug/debug_setup.h>
#include <bruce/metadata_timestamp.h>
#include <bruce/msg_state_tracker.h>
#include <third_party/mongoose/mongoose.h>

namespace Bruce {

  class TWebInterface final : public Mongoose::TMongoose {
    NO_COPY_SEMANTICS(TWebInterface);

    public:
    TWebInterface(in_port_t port, TMsgStateTracker &msg_state_tracker,
                  TAnomalyTracker &anomaly_tracker,
                  const TMetadataTimestamp &metadata_timestamp,
                  Base::TEventSemaphore &metadata_update_request_sem,
                  Debug::TDebugSetup &debug_setup)
        : Port(port),
          HttpServerStarted(false),
          MsgStateTracker(msg_state_tracker),
          AnomalyTracker(anomaly_tracker),
          MetadataTimestamp(metadata_timestamp),
          MetadataUpdateRequestSem(metadata_update_request_sem),
          DebugSetup(debug_setup) {
    }

    virtual ~TWebInterface() noexcept {
      assert(this);
      StopHttpServer();
    }

    void StartHttpServer() {
      assert(this);

      if (!HttpServerStarted) {
        DoStartHttpServer();
        HttpServerStarted = true;
      }
    }

    void StopHttpServer() {
      assert(this);

      if (HttpServerStarted) {
        DoStopHttpServer();
        HttpServerStarted = false;
      }
    }

    protected:
    virtual void *OnEvent(mg_event event, mg_connection *conn,
        const mg_request_info *request_info) override;

    private:
    enum class TRequestType {
      UNIMPLEMENTED_REQUEST_METHOD,
      UNKNOWN_GET_REQUEST,
      UNKNOWN_POST_REQUEST,
      TOP_LEVEL_PAGE,
      GET_SERVER_INFO,
      GET_COUNTERS,
      GET_DISCARDS,
      GET_METADATA_FETCH_TIME,
      GET_QUEUE_STATS,
      MSG_DEBUG_GET_TOPICS,
      MSG_DEBUG_ADD_ALL_TOPICS,
      MSG_DEBUG_DEL_ALL_TOPICS,
      MSG_DEBUG_TRUNCATE_FILES,
      MSG_DEBUG_ADD_TOPIC,
      MSG_DEBUG_DEL_TOPIC,
      METADATA_UPDATE
    };  // TRequestType

    static const char *ToErrorBlurb(TRequestType request_type);

    void DoStartHttpServer();

    void WriteFrontPage(std::ostream &os);

    void HandleHttpRequest(mg_connection *conn,
        const mg_request_info *request_info, TRequestType &request_type);

    void DoStopHttpServer() {
      assert(this);
      Stop();
    }

    const in_port_t Port;

    bool HttpServerStarted;

    TMsgStateTracker &MsgStateTracker;

    TAnomalyTracker &AnomalyTracker;

    const TMetadataTimestamp &MetadataTimestamp;

    Base::TEventSemaphore &MetadataUpdateRequestSem;

    Debug::TDebugSetup &DebugSetup;
  };  // TWebInterface

}  // Bruce
