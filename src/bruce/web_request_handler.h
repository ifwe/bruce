/* <bruce/web_request_handler.h>

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

   Class for handling requests received by Bruce's web interface.
 */

#pragma once

#include <ostream>

#include <base/event_semaphore.h>
#include <base/indent.h>
#include <base/no_copy_semantics.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/debug/debug_setup.h>
#include <bruce/metadata_timestamp.h>
#include <bruce/msg_state_tracker.h>

namespace Bruce {

  class TWebRequestHandler final {
    NO_COPY_SEMANTICS(TWebRequestHandler);

    public:
    TWebRequestHandler() = default;

    void HandleGetServerInfoRequestPlain(std::ostream &os);

    void HandleGetServerInfoRequestJson(std::ostream &os);

    void HandleGetCountersRequestPlain(std::ostream &os);

    void HandleGetCountersRequestJson(std::ostream &os);

    void HandleGetDiscardsRequestPlain(std::ostream &os,
        const TAnomalyTracker &tracker);

    void HandleGetDiscardsRequestJson(std::ostream &os,
        const TAnomalyTracker &tracker);

    void HandleMetadataFetchTimeRequestPlain(std::ostream &os,
        const TMetadataTimestamp &metadata_timestamp);

    void HandleMetadataFetchTimeRequestJson(std::ostream &os,
        const TMetadataTimestamp &metadata_timestamp);

    void HandleQueueStatsRequestPlain(std::ostream &os,
        const TMsgStateTracker &tracker);

    void HandleQueueStatsRequestJson(std::ostream &os,
        const TMsgStateTracker &tracker);

    void HandleGetDebugTopicsRequest(std::ostream &os,
        const Debug::TDebugSetup &debug_setup);

    void HandleDebugAddAllTopicsRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup);

    void HandleDebugDelAllTopicsRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup);

    void HandleDebugTruncateFilesRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup);

    void HandleDebugAddTopicRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup, const char *topic);

    void HandleDebugDelTopicRequest(std::ostream &os,
        Debug::TDebugSetup &debug_setup, const char *topic);

    void HandleMetadataUpdateRequest(std::ostream &os,
        Base::TEventSemaphore &update_request_sem);

    private:
    void WriteDiscardReportPlain(std::ostream &os,
        const TAnomalyTracker::TInfo &info);

    void WriteDiscardReportJson(std::ostream &os,
        const TAnomalyTracker::TInfo &info, Base::TIndent &ind0);
  };  // TWebRequestHandler

}  // Bruce
