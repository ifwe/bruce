/* <bruce/mock_kafka_server/error_injector.h>

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

   Class for sending error injection commands to mock Kafka server.
 */

#pragma once

#include <cassert>

#include <netinet/in.h>

#include <base/fd.h>
#include <bruce/mock_kafka_server/cmd.h>
#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace MockKafkaServer {

    /* Class for sending error injection commands to mock Kafka server.
       Connects to mock server and sends commands over socket. */
    class TErrorInjector final {
      NO_COPY_SEMANTICS(TErrorInjector);

      public:
      static TCmd MakeCmdAckError(int16_t error_code,
          const char *msg_body_to_match, const char *client_addr_to_match);

      static TCmd MakeCmdDisconnectBeforeAck(const char *msg_body_to_match,
          const char *client_addr_to_match);

      static TCmd MakeCmdMetadataResponseError(int16_t error_code,
          const char *topic, const char *client_addr_to_match);

      static TCmd MakeCmdAllTopicsMetadataResponseError(int16_t error_code,
          const char *topic, const char *client_addr_to_match);

      static TCmd MakeCmdDisconnectBeforeMetadataResponse(const char *topic,
          const char *client_addr_to_match);

      static TCmd MakeCmdDisconnectBeforeAllTopicsMetadataResponse(
          const char *client_addr_to_match);

      TErrorInjector() = default;

      /* Return true on success, or false or failure. */
      bool Connect(const char *host, in_port_t port);

      void Disconnect() {
        assert(this);
        Sock.Reset();
      }

      /* Return true on success or false on error.  If msg_body_to_match is
         null then match any message body. */
      bool InjectAckError(int16_t error_code, const char *msg_body_to_match,
          const char *client_addr_to_match) {
        assert(this);
        return InjectCmd(MakeCmdAckError(error_code, msg_body_to_match,
                                         client_addr_to_match));
      }

      /* Return true on success or false on error.  If msg_body_to_match is
         null then match any message body. */
      bool InjectDisconnectBeforeAck(const char *msg_body_to_match,
          const char *client_addr_to_match) {
        assert(this);
        return InjectCmd(MakeCmdDisconnectBeforeAck(msg_body_to_match,
                                                    client_addr_to_match));
      }

      /* Return true on success or false on error. */
      bool InjectMetadataResponseError(int16_t error_code, const char *topic,
          const char *client_addr_to_match) {
        assert(this);
        return InjectCmd(MakeCmdMetadataResponseError(error_code, topic,
                                                      client_addr_to_match));
      }

      /* Return true on success or false on error.  The injected error will be
         associated with a particular topic, so we have to specify which topic
         gets the error. */
      bool InjectAllTopicsMetadataResponseError(int16_t error_code,
          const char *topic, const char *client_addr_to_match) {
        assert(this);
        return InjectCmd(MakeCmdAllTopicsMetadataResponseError(
            error_code, topic, client_addr_to_match));
      }

      /* Return true on success or false on error. */
      bool InjectDisconnectBeforeMetadataResponse(const char *topic,
          const char *client_addr_to_match) {
        assert(this);
        return InjectCmd(MakeCmdDisconnectBeforeMetadataResponse(topic,
            client_addr_to_match));
      }

      /* Return true on success or false on error. */
      bool InjectDisconnectBeforeAllTopicsMetadataResponse(
          const char *client_addr_to_match) {
        assert(this);
        return InjectCmd(MakeCmdDisconnectBeforeAllTopicsMetadataResponse(
            client_addr_to_match));
      }

      /* Return true on success or false on error. */
      bool InjectCmd(const TCmd &cmd) {
        assert(this);
        assert(Sock.IsOpen());
        return SendCmd(cmd) && ReceiveAck();
      }

      /* TIter is a forward iterator for a sequence of TCmd objects.  Return
         0 if all commands successfully sent.  If 1 is returned, then first
         TCmd send failed.  If 2 is returned, 2nd TCmd send failed, etc.  If -1
         is returned, then first ACK receive failed.  If -2 is returned, then
         second ACK receive failed, etc. */
      template <typename TIter>
      int InjectCmdSeq(TIter seq_begin, TIter seq_end) {
        int count = 0;

        for (count = 0; seq_begin != seq_end; ++count, ++seq_begin) {
          if (!SendCmd(*seq_begin)) {
            return count + 1;
          }
        }

        for (int i = 0; i < count; ++i) {
          if (!ReceiveAck()) {
            return -(i + 1);
          }
        }

        return 0;
      }

      private:
      bool SendCmd(const TCmd &cmd);

      bool ReceiveAck();

      Base::TFd Sock;
    };  // TErrorInjector

  }  // MockKafkaServer

}  // Bruce
