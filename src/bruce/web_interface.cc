/* <bruce/web_interface.cc>

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

   Implements <bruce/web_interface.h>.
 */

#include <bruce/web_interface.h>

#include <algorithm>
#include <cstring>
#include <sstream>
#include <vector>

#include <syslog.h>

#include <base/error_utils.h>
#include <base/no_default_case.h>
#include <bruce/web_request_handler.h>
#include <server/counter.h>
#include <server/url_decode.h>
#include <signal/masker.h>
#include <signal/set.h>

using namespace Base;
using namespace Bruce;
using namespace Server;

SERVER_COUNTER(MongooseEventLog);
SERVER_COUNTER(MongooseGetServerInfoRequest);
SERVER_COUNTER(MongooseGetCountersRequest);
SERVER_COUNTER(MongooseGetDiscardsRequest);
SERVER_COUNTER(MongooseGetMetadataFetchTimeRequest);
SERVER_COUNTER(MongooseGetQueueStatsRequest);
SERVER_COUNTER(MongooseHttpRequest);
SERVER_COUNTER(MongooseStdException);
SERVER_COUNTER(MongooseUnknownException);
SERVER_COUNTER(MongooseUrlDecodeError);

const char *TWebInterface::ToErrorBlurb(TRequestType request_type) {
  switch (request_type) {
    case TRequestType::UNIMPLEMENTED_REQUEST_METHOD: {
      break;
    }
    case TRequestType::UNKNOWN_GET_REQUEST: {
      return "Unknown GET request";
    }
    case TRequestType::UNKNOWN_POST_REQUEST: {
      return "Unknown POST request";
    }
    case TRequestType::TOP_LEVEL_PAGE: {
      return "Top level page";
    }
    case TRequestType::GET_SERVER_INFO: {
      return "Get server info";
    }
    case TRequestType::GET_COUNTERS: {
      return "Get counters";
    }
    case TRequestType::GET_DISCARDS: {
      return "Get discards";
    }
    case TRequestType::GET_METADATA_FETCH_TIME: {
      return "Get metadata fetch time";
    }
    case TRequestType::GET_QUEUE_STATS: {
      return "Get queue stats";
    }
    case TRequestType::MSG_DEBUG_GET_TOPICS: {
      return "Msg debug get topics";
    }
    case TRequestType::MSG_DEBUG_ADD_ALL_TOPICS: {
      return "Msg debug add all topics";
    }
    case TRequestType::MSG_DEBUG_DEL_ALL_TOPICS: {
      return "Msg debug del all topics";
    }
    case TRequestType::MSG_DEBUG_TRUNCATE_FILES: {
      return "Msg debug truncate files";
    }
    case TRequestType::MSG_DEBUG_ADD_TOPIC: {
      return "Msg debug add topic";
    }
    case TRequestType::MSG_DEBUG_DEL_TOPIC: {
      return "Msg debug del topic";
    }
    case TRequestType::METADATA_UPDATE: {
      return "Metadata update";
    }
    NO_DEFAULT_CASE;
  }

  return "Unimplemented request method";
}

void *TWebInterface::OnEvent(mg_event event, mg_connection *conn,
    const mg_request_info *request_info) {
  assert(this);
  bool is_handled = false;
  TRequestType request_type = TRequestType::UNIMPLEMENTED_REQUEST_METHOD;
  const char *error_blurb = "";

  try {
    switch (event) {
      case MG_NEW_REQUEST: {
        MongooseHttpRequest.Increment();

        try {
          HandleHttpRequest(conn, request_info, request_type);
        } catch (...) {
          error_blurb = ToErrorBlurb(request_type);
          throw;
        }

        is_handled = true;
        break;
      }
      case MG_EVENT_LOG: {
        error_blurb = "Mongoose event log";
        MongooseEventLog.Increment();
        syslog(LOG_ERR, "Mongoose error: %s %s %s", request_info->log_message,
               request_info->uri, request_info->query_string);
        is_handled = true;
        break;
      }
      default: {
        break;
      }
    }
  } catch (const TUrlDecodeError &x) {
    MongooseUrlDecodeError.Increment();
    mg_printf(conn, "HTTP/1.1 400 BAD REQUEST\r\n"
                    "Content-Type: text/plain\r\n\r\n"
                    "[URL decode error: %s at query string offset %d][%s]",
              x.what(), x.GetOffset(), error_blurb);
    is_handled = true;
  } catch (const std::exception &x) {
    MongooseStdException.Increment();
    mg_printf(conn, "HTTP/1.1 400 BAD REQUEST\r\n"
                    "Content-Type: text/plain\r\n\r\n[%s][std::exception][%s]",
              error_blurb, x.what());
    is_handled = true;
  } catch (...) {
    MongooseUnknownException.Increment();
    mg_printf(conn, "HTTP/1.1 400 BAD REQUEST\r\n"
                    "Content-Type: text/plain\r\n\r\n[%s][unknown exception]",
              error_blurb);
    is_handled = true;
  }

  return is_handled ? const_cast<char *>("") : nullptr;
}

void TWebInterface::WriteFrontPage(std::ostream &os) {
  assert(this);
  os << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << std::endl
      << "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\""
      << std::endl
      << "    \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">"
      << std::endl
      << "<html xmlns=\"http://www.w3.org/1999/xhtml\">" << std::endl
      << "  <head>" << std::endl
      << "    <title>Bruce</title>" << std::endl
      << "  </head>" << std::endl
      << "  <body>" << std::endl
      << "    <h1>Server Status</h1>" << std::endl
      << "    <div>" << std::endl
      << "      Get server info: [<a href=\"/server_info/plain\">"
      << "plain</a>]" << std::endl
      << "          [<a href=\"/server_info/json\">JSON</a>]<br/>"
      << std::endl
      << "      Get counter values: [<a href=\"/counters/plain\">"
      << "plain</a>]" << std::endl
      << "          [<a href=\"/counters/json\">JSON</a>]<br/>" << std::endl
      << "      Get discard info: [<a href=\"/discards/plain\">"
      << "plain</a>]" << std::endl
      << "          [<a href=\"/discards/json\">JSON</a>]<br/>" << std::endl
      << "      Get queued message info: [<a href=\"/queues/plain\">"
      << "plain</a>]" << std::endl
      << "          [<a href=\"/queues/json\">JSON</a>]<br/>" << std::endl
      << "      Get metadata fetch time:" << std::endl
      << "          [<a href=\"/metadata_fetch_time/plain\">plain</a>]"
      << std::endl
      << "          [<a href=\"/metadata_fetch_time/json\">JSON</a>]<br/>"
      << std::endl
      << "    </div>" << std::endl
      << "    <h1>Server Management</h1>" << std::endl
      << "    <form action=\"/metadata_update\" method=\"post\">" << std::endl
      << "      <div>" << std::endl
      << "        <input type=\"submit\" value=\"Update Metadata\"/>"
      << std::endl
      << "      </div>" << std::endl
      << "    </form>" << std::endl
      << "  </body>" << std::endl
      << "</html>" << std::endl;
}

void TWebInterface::HandleHttpRequest(mg_connection *conn,
    const mg_request_info *request_info, TRequestType &request_type) {
  assert(this);

  /* For each request type handled below, set this as soon as the request type
     is identified.  Then the caller will have that information for error
     reporting even if an exception is thrown. */
  request_type = TRequestType::UNIMPLEMENTED_REQUEST_METHOD;

  /* Response content goes here. */
  std::ostringstream oss;

  enum class TResponseType {
    Text,
    Json,
    Html
  } response_type = TResponseType::Text;

  if (!std::strcmp(request_info->request_method, "GET")) {
    static const char add_debug_topic_prefix[] =
        "/msg_debug/add_topic/";
    static const char del_debug_topic_prefix[] =
        "/msg_debug/del_topic/";
    static const size_t add_debug_topic_prefix_len =
        std::strlen(add_debug_topic_prefix);
    static const size_t del_debug_topic_prefix_len =
        std::strlen(del_debug_topic_prefix);

    if (!std::strcmp(request_info->uri, "/server_info/plain")) {
      request_type = TRequestType::GET_SERVER_INFO;
      MongooseGetServerInfoRequest.Increment();
      TWebRequestHandler().HandleGetServerInfoRequestPlain(oss);
    } else if (!std::strcmp(request_info->uri, "/server_info/json")) {
      request_type = TRequestType::GET_SERVER_INFO;
      MongooseGetServerInfoRequest.Increment();
      TWebRequestHandler().HandleGetServerInfoRequestJson(oss);
      response_type = TResponseType::Json;
    } else if (!std::strcmp(request_info->uri, "/counters/plain")) {
      request_type = TRequestType::GET_COUNTERS;
      MongooseGetCountersRequest.Increment();
      TWebRequestHandler().HandleGetCountersRequestPlain(oss);
    } else if (!std::strcmp(request_info->uri, "/counters/json")) {
      request_type = TRequestType::GET_COUNTERS;
      MongooseGetCountersRequest.Increment();
      TWebRequestHandler().HandleGetCountersRequestJson(oss);
      response_type = TResponseType::Json;
    } else if (!std::strcmp(request_info->uri, "/discards/plain")) {
      request_type = TRequestType::GET_DISCARDS;
      MongooseGetDiscardsRequest.Increment();
      TWebRequestHandler().HandleGetDiscardsRequestPlain(oss, AnomalyTracker);
    } else if (!std::strcmp(request_info->uri, "/discards/json")) {
      request_type = TRequestType::GET_DISCARDS;
      MongooseGetDiscardsRequest.Increment();
      TWebRequestHandler().HandleGetDiscardsRequestJson(oss, AnomalyTracker);
      response_type = TResponseType::Json;
    } else if (!std::strcmp(request_info->uri,
                            "/metadata_fetch_time/plain")) {
      request_type = TRequestType::GET_METADATA_FETCH_TIME;
      MongooseGetMetadataFetchTimeRequest.Increment();
      TWebRequestHandler().HandleMetadataFetchTimeRequestPlain(oss,
          MetadataTimestamp);
    } else if (!std::strcmp(request_info->uri,
                            "/metadata_fetch_time/json")) {
      request_type = TRequestType::GET_METADATA_FETCH_TIME;
      MongooseGetMetadataFetchTimeRequest.Increment();
      TWebRequestHandler().HandleMetadataFetchTimeRequestJson(oss,
          MetadataTimestamp);
      response_type = TResponseType::Json;
    } else if (!std::strcmp(request_info->uri, "/queues/plain")) {
      request_type = TRequestType::GET_QUEUE_STATS;
      MongooseGetQueueStatsRequest.Increment();
      TWebRequestHandler().HandleQueueStatsRequestPlain(oss, MsgStateTracker);
    } else if (!std::strcmp(request_info->uri, "/queues/json")) {
      request_type = TRequestType::GET_QUEUE_STATS;
      MongooseGetQueueStatsRequest.Increment();
      TWebRequestHandler().HandleQueueStatsRequestJson(oss, MsgStateTracker);
      response_type = TResponseType::Json;
    } else if (!std::strcmp(request_info->uri, "/msg_debug/get_topics")) {
      request_type = TRequestType::MSG_DEBUG_GET_TOPICS;
      TWebRequestHandler().HandleGetDebugTopicsRequest(oss, DebugSetup);
    } else if (!std::strcmp(request_info->uri,
                            "/msg_debug/add_all_topics")) {
      request_type = TRequestType::MSG_DEBUG_ADD_ALL_TOPICS;
      TWebRequestHandler().HandleDebugAddAllTopicsRequest(oss, DebugSetup);
    } else if (!std::strcmp(request_info->uri, "/msg_debug/del_all_topics")) {
      request_type = TRequestType::MSG_DEBUG_DEL_ALL_TOPICS;
      TWebRequestHandler().HandleDebugDelAllTopicsRequest(oss, DebugSetup);
    } else if (!std::strcmp(request_info->uri, "/msg_debug/truncate_files")) {
      request_type = TRequestType::MSG_DEBUG_TRUNCATE_FILES;
      TWebRequestHandler().HandleDebugTruncateFilesRequest(oss, DebugSetup);
    } else if (!std::strncmp(request_info->uri, add_debug_topic_prefix,
                             add_debug_topic_prefix_len)) {
      request_type = TRequestType::MSG_DEBUG_ADD_TOPIC;
      const char *topic = request_info->uri + add_debug_topic_prefix_len;
      TWebRequestHandler().HandleDebugAddTopicRequest(oss, DebugSetup, topic);
    } else if (!std::strncmp(request_info->uri, del_debug_topic_prefix,
                             del_debug_topic_prefix_len)) {
      request_type = TRequestType::MSG_DEBUG_DEL_TOPIC;
      const char *topic = request_info->uri + del_debug_topic_prefix_len;
      TWebRequestHandler().HandleDebugDelTopicRequest(oss, DebugSetup, topic);
    } else {
      request_type = std::strcmp(request_info->uri, "/") ?
          TRequestType::UNKNOWN_GET_REQUEST : TRequestType::TOP_LEVEL_PAGE;
      WriteFrontPage(oss);
      response_type = TResponseType::Html;
    }
  } else if (!std::strcmp(request_info->request_method, "POST")) {
    if (!std::strcmp(request_info->uri, "/metadata_update")) {
      request_type = TRequestType::METADATA_UPDATE;
      TWebRequestHandler().HandleMetadataUpdateRequest(oss,
          MetadataUpdateRequestSem);
    } else {
      request_type = TRequestType::UNKNOWN_POST_REQUEST;
      mg_printf(conn, "HTTP/1.1 404 NOT FOUND\r\n"
                      "Content-Type: text/plain\r\n\r\n"
                      "[not found: try /metadata_update]");
      return;
    }
  } else {
    request_type = TRequestType::UNIMPLEMENTED_REQUEST_METHOD;
    mg_printf(conn, "HTTP/1.1 501 NOT IMPLEMENTED\r\n"
                    "Content-Type: text/plain\r\n\r\n"
                    "[request method %s not implemented]",
              request_info->request_method);
    return;
  }

  const char *resp_type = "text/plain";

  switch (response_type) {
    case TResponseType::Text: break;
    case TResponseType::Json: resp_type = "application/json"; break;
    case TResponseType::Html: resp_type = "text/html"; break;
    NO_DEFAULT_CASE;
  }

  std::string response(oss.str());
  mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
                  "Content-Length: %d\r\n\r\n", resp_type,
            response.size());
  mg_write(conn, response.data(), response.size());
}

void TWebInterface::DoStartHttpServer() {
  assert(this);
  std::ostringstream oss;
  oss << Port;
  std::string port_str(oss.str());

  const char *opts[] = {
    "enable_directory_listing", "no",
    "listening_ports", port_str.c_str(),
    "num_threads", "1",
    nullptr
  };

  /* We want any threads created by Mongoose to have all signals blocked.
     Bruce's main thread handles signals. */
  const Signal::TSet block_all(Signal::TSet::Full);
  Signal::TMasker masker(*block_all);
  Start(opts);
}
