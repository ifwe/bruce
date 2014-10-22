/* <bruce/mock_kafka_server/setup.h>

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

   Mock Kafka server setup information obtained from a file.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <netinet/in.h>

namespace Bruce {

  namespace MockKafkaServer {

    class TSetup final {
      public:
      class TFileFormatError final : public std::runtime_error {
        public:
        TFileFormatError(size_t line_num, const char *msg);

        virtual ~TFileFormatError() noexcept { }

        size_t GetLineNum() const {
          return LineNum;
        }

        private:
        size_t LineNum;
      };  // TFileFormatError

      /* The mock server listens on a range of consecutive port numbers, with
         each port simulating a separate Kafka broker.  For each simulated
         broker we can inject periodic delays before reading a request and/or
         sending an ACK. */
      struct TPort {
        size_t ReadDelay;  // delay in milliseconds before socket read
        size_t ReadDelayInterval;  // how often to impose read delay
        size_t AckDelay;  // delay in milliseconds before sending ACK
        size_t AckDelayInterval;  // how often to impose ACK delay

        TPort()
            : ReadDelay(0),
              ReadDelayInterval(1),
              AckDelay(0),
              AckDelayInterval(1) {
        }
      };  // TPort

      /* For each partition within a topic, we can inject periodic ACK errors.
       */
      struct TPartition {
        int16_t AckError;
        size_t AckErrorInterval;

        TPartition()
            : AckError(0),
              AckErrorInterval(1) {
        }
      };  // TPartition

      struct TTopic {
        std::vector<TPartition> Partitions;

        size_t FirstPortOffset;

        TTopic()
            : FirstPortOffset(0) {
        }
      };  // TTopic

      struct TInfo {
        /* first port in range of consecutive port numbers */
        in_port_t BasePort;

        /* Item 0 is BasePort, item 1 is (BasePort + 1), etc. */
        std::vector<TPort> Ports;

        /* Key is topic name. */
        std::unordered_map<std::string, TTopic> Topics;

        TInfo()
            : BasePort(0) {
        }

        void Clear() {
          BasePort = 0;
          Ports.clear();
          Topics.clear();
        }
      };  // TInfo

      TSetup();

      /* Read info from 'setup_file_path' and store it in 'out'.
         The setup file format looks like this:

           ports 10000 3
           port 10001 read_delay 5000:100 ack_delay 6000:50
           topic foo 4 2
           partition_error foo 3 5 100

         The first line specifies that the server will listen on a range of 3
         consecutive ports starting at 10000.

         The second line specifies that for the server instance listening on
         port 10001, a delay of 5000 milliseconds will be injected once every
         100 times it reads a request, and a delay of 6000 milliseconds will be
         injected once every 50 times right before it sends an ACK.

         The third line specifies a topic named "foo" with 4 partitions.  The
         partitions are numbered consecutively starting at 0, and are assigned
         consecutive ports starting at offset 2 from the starting port (10000
         in this case).  For this example, the mapping from partitions to ports
         is therefore as follows:

             topic/partition                  port
             ---------------                  ----
             foo/0                            10002
             foo/1                            10000
             foo/2                            10001
             foo/3                            10002

         Notice that the port numbers wrap around once the end of the range is
         reached.

         The fourth line specifies that for topic "foo", partition 3, an error
         ACK with value 5 will be injected once every 100 ACKs.

         Empty lines, lines containing nothing but whitespace, and comment
         lines are discarded.  A comment line is a line consisting of nothing
         but 0 or more whitespace characters, followed by a '#' character,
         followed by optional text.  Once all such lines have been discarded,
         the remaining lines must be ordered as follows:

             1.  The first line must be the 'ports' line as illustrated above.
             2.  0 or more 'port' lines then follow.
             3.  1 or more 'topic' lines then follow.
             4.  0 or more 'partition_error' lines then follow.
       */
      void Get(const std::string &setup_file_path, TInfo &out);

      private:
      void NextInterestingLine(std::istream &in);

      std::string ErrorBlurb();

      void GetPortsLine(std::istream &in);

      void GetPortLines(std::istream &in);

      void GetTopicLines(std::istream &in);

      void GetPartitionErrorLines(std::istream &in);

      void FillResult(std::istream &in);

      size_t LineNum;

      std::vector<std::string> CurrentLineTokens;

      TInfo Result;
    };  // TSetup

  }  // MockKafkaServer

}  // Bruce
