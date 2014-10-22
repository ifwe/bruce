/* <bruce/mock_kafka_server/received_request_tracker.h>

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

   A container for tracking requests (produce requests or metadata requests)
   received from clients.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <string>

#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/conf/compression_type.h>
#include <bruce/util/gate.h>

namespace Bruce {

  namespace MockKafkaServer {

    /* A container for tracking requests (produce requests or metadata
       requests) received from clients.  This is used only for unit tests.  The
       mock Kafka server adds request information on received requests to the
       request tracker, and the unit test code consumes the information. */
    class TReceivedRequestTracker final {
      NO_COPY_SEMANTICS(TReceivedRequestTracker);

      public:
      /* FIXME: The name of this class is misleading.  It contains information
         for a single message set within a produce request, not an entire
         produce request. */
      struct TProduceRequestInfo {
        std::string Topic;

        int32_t Partition;

        Conf::TCompressionType CompressionType;

        size_t MsgCount;

        std::string FirstMsgKey;

        std::string FirstMsgValue;

        int16_t ReturnedErrorCode;

        TProduceRequestInfo()
            : Partition(0),
              CompressionType(Conf::TCompressionType::None),
              MsgCount(0),
              ReturnedErrorCode(0) {
        }

        TProduceRequestInfo(const TProduceRequestInfo &) = default;

        TProduceRequestInfo(TProduceRequestInfo &&) = default;

        TProduceRequestInfo &operator=(const TProduceRequestInfo &) = default;

        TProduceRequestInfo &operator=(TProduceRequestInfo &&) = default;
      };  // TProduceRequestInfo

      struct TMetadataRequestInfo {
        /* Empty indicates all topics. */
        std::string Topic;

        int16_t ReturnedErrorCode;

        TMetadataRequestInfo()
            : ReturnedErrorCode(0) {
        }

        TMetadataRequestInfo(const TMetadataRequestInfo &) = default;

        TMetadataRequestInfo &operator=(const TMetadataRequestInfo &) =
            default;

        TMetadataRequestInfo(TMetadataRequestInfo &&that)
            : Topic(std::move(that.Topic)),
              ReturnedErrorCode(that.ReturnedErrorCode) {
          that.ReturnedErrorCode = 0;
        }
      };  // TMetadataRequestInfo

      /* Exactly one of { ProduceRequestInfo, MetadataRequestInfo } will be
         known. */
      struct TRequestInfo {
        Base::TOpt<TProduceRequestInfo> ProduceRequestInfo;

        Base::TOpt<TMetadataRequestInfo> MetadataRequestInfo;

        TRequestInfo() = default;

        TRequestInfo(const TRequestInfo &) = default;

        TRequestInfo &operator=(const TRequestInfo &) = default;

        TRequestInfo(TRequestInfo &&that)
            : ProduceRequestInfo(std::move(that.ProduceRequestInfo)),
              MetadataRequestInfo(std::move(that.MetadataRequestInfo)) {
        }
      };  // TRequestInfo

      TReceivedRequestTracker() = default;

      void PutRequestInfo(TRequestInfo &&info) {
        assert(this);
        Queue.Put(std::move(info));
      }

      /* Append items to 'result'. */
      void GetRequestInfo(std::list<TRequestInfo> &result) {
        assert(this);
        result.splice(result.end(), Queue.Get());
      }

      /* Append items to 'result', but don't block if nothing is available. */
      void NonblockingGetRequestInfo(std::list<TRequestInfo> &result) {
        assert(this);
        result.splice(result.end(), Queue.NonblockingGet());
      }

      private:
      using TQueue = Bruce::Util::TGate<TRequestInfo>;

      TQueue Queue;
    };  // TReceivedRequestTracker

  }  // MockKafkaServer

}  // Bruce
