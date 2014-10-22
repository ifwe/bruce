/* <bruce/mock_kafka_server/cmd_worker.cc>

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

   Implements <bruce/mock_kafka_server/cmd_worker.h>.
 */

#include <bruce/mock_kafka_server/cmd_worker.h>

#include <cassert>

#include <syslog.h>

#include <base/debug_log.h>
#include <base/field_access.h>
#include <base/no_default_case.h>
#include <bruce/mock_kafka_server/serialize_cmd.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::MockKafkaServer;

TCmdWorker::~TCmdWorker() noexcept {
  /* This will shut down the thread if something unexpected happens. */
  ShutdownOnDestroy();
}

void TCmdWorker::Run() {
  assert(this);
  DEBUG_LOG("got connection on command port");
  const TFd &shutdown_request_fd = GetShutdownRequestFd();

  while (!shutdown_request_fd.IsReadable() && GetCmd()) {
    Ss.CmdBucket.Put(Cmd);

    if (!SendReply(true)) {
      break;
    }
  }
}

bool TCmdWorker::GetCmd() {
  assert(this);

  /* Start by reading just enough data to determine the size of the entire
     command, and then read the rest of the command.  It would be more
     efficient to avoid reading in little pieces, but efficiency is not a big
     concern here. */
  size_t initial_read_size = BytesNeededToGetCmdSize();
  InputBuf.resize(initial_read_size);

  switch (TryReadExactlyOrShutdown(ClientSocket, &InputBuf[0],
                                   InputBuf.size())) {
    case TIoResult::Success: {
      break;
    }
    case TIoResult::Disconnected:
    case TIoResult::EmptyReadUnexpectedEnd:
    case TIoResult::UnexpectedEnd:
    case TIoResult::GotShutdownRequest: {
      return false;
    }
    NO_DEFAULT_CASE;
  }

  size_t cmd_size = GetCmdSize(InputBuf);
  assert(cmd_size >= initial_read_size);
  InputBuf.resize(cmd_size);
  size_t bytes_left = cmd_size - initial_read_size;

  switch (TryReadExactlyOrShutdown(ClientSocket, &InputBuf[initial_read_size],
                                   bytes_left)) {
    case TIoResult::Success: {
      break;
    }
    case TIoResult::Disconnected:
    case TIoResult::EmptyReadUnexpectedEnd:
    case TIoResult::UnexpectedEnd:
    case TIoResult::GotShutdownRequest: {
      return false;
    }
    NO_DEFAULT_CASE;
  }

  if (!DeserializeCmd(InputBuf, Cmd)) {
    syslog(LOG_ERR, "Mock Kafka server got unknown command");
    SendReply(false);
    return false;
  }

  return true;
}

bool TCmdWorker::SendReply(bool success) {
  assert(this);
  uint8_t reply = success ? 0 : 1;

  switch (TryWriteExactlyOrShutdown(ClientSocket, &reply, sizeof(reply))) {
    case TIoResult::Success: {
      break;
    }
    case TIoResult::Disconnected:
    case TIoResult::EmptyReadUnexpectedEnd:
    case TIoResult::UnexpectedEnd:
    case TIoResult::GotShutdownRequest: {
      return false;
    }
    NO_DEFAULT_CASE;
  }

  return true;
}
