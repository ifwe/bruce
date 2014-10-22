/* <bruce/mock_kafka_server/cmd_bucket.h>

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

   A container for commands (for error injection, etc.) waiting to be snarfed
   by mock Kafka server worker threads.  This is a rather limited error
   injection mechanism, but should be good enough for testing.
 */

#pragma once

#include <list>
#include <mutex>
#include <utility>

#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/mock_kafka_server/cmd.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TCmdBucket final {
      NO_COPY_SEMANTICS(TCmdBucket);

      public:
      TCmdBucket()
          : SequenceNumCounter(0) {
      }

      /* Put 'cmd' in the bucket, appending it to the queue. */
      void Put(const TCmd &cmd);

      /* Copy to the caller whatever command (if any) the bucket has as its
         first queued item, but leave it in the bucket.  Return true if a
         command was copied, or false if the bucket was empty.  If true was
         returned, then on return, 'sequence_num' will contain the sequence
         number of the command and 'out_cmd' will contain a copy of the
         command. */
      bool CopyOut(size_t &sequence_num, TCmd &out_cmd) const;

      /* Attempt to remove (and discard) the command with sequence number
         'sequence_num' from the front of the bucket's queue.  Return true if
         the command was successfully removed.  Return false if the bucket was
         empty or the first command in its queue had a nonmatching sequence
         number.  A false return value implies that the bucket contents were
         unchanged. */
      bool Remove(size_t sequence_num);

      private:
      mutable std::mutex Mutex;

      /* The first item in the pair is a sequence number. */
      std::list<std::pair<size_t, TCmd>> CmdQueue;

      /* For generating sequence numbers. */
      size_t SequenceNumCounter;
    };  // TCmdBucket

  }  // MockKafkaServer

}  // Bruce
