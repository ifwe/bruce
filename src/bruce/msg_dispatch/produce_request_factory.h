/* <bruce/msg_dispatch/produce_request_factory.h>

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

   Object responsible for serializing produce requests.  Each send thread owns
   one of these.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <bruce/batch/global_batch_config.h>
#include <bruce/compress/compression_codec_api.h>
#include <bruce/compress/get_compression_codec.h>
#include <bruce/conf/compression_conf.h>
#include <bruce/conf/compression_type.h>
#include <bruce/config.h>
#include <bruce/debug/debug_logger.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/metadata.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/any_partition_chooser.h>
#include <bruce/msg_dispatch/common.h>
#include <bruce/util/msg_util.h>

namespace Bruce {

  namespace MsgDispatch {

    class TProduceRequestFactory final {
      NO_COPY_SEMANTICS(TProduceRequestFactory);

      public:
      TProduceRequestFactory(const TConfig &config,
          const Batch::TGlobalBatchConfig &batch_config,
          const Conf::TCompressionConf &compression_conf,
          const KafkaProto::TWireProtocol &kafka_protocol, size_t broker_index,
          bool add_timestamp, bool use_old_output_format);

      void Init(const Conf::TCompressionConf &compression_conf,
                const std::shared_ptr<TMetadata> &md);

      void Reset();

      bool IsEmpty() const {
        assert(this);
        return InputQueue.empty();
      }

      /* Queue input message as a single item batch. */
      void Put(TMsg::TPtr &&msg);

      /* Queue a single batch. */
      void Put(std::list<TMsg::TPtr> &&batch) {
        assert(this);
        InputQueue.push_back(std::move(batch));
      }

      /* Queue multiple batches. */
      void Put(std::list<std::list<TMsg::TPtr>> &&batch_list) {
        assert(this);
        InputQueue.splice(InputQueue.end(), std::move(batch_list));
      }

      /* Used for resending messages. */
      void PutFront(std::list<TMsg::TPtr> &&batch) {
        assert(this);
        InputQueue.push_front(std::move(batch));
      }

      /* Used for resending messages. */
      void PutFront(std::list<std::list<TMsg::TPtr>> &&batch_list) {
        assert(this);
        InputQueue.splice(InputQueue.begin(), std::move(batch_list));
      }

      std::list<std::list<TMsg::TPtr>> GetAll() {
        assert(this);
        return std::move(InputQueue);
      }

      /* Build a produce request containing messages stored in the factory by
         previous calls to the above Put() and PutFront() methods.  If the
         factory contains no messages (testable by calling IsEmpty() method),
         then the returned optional produce request will be in the unknown
         state and output buffer 'dst' will be left unmodified.  Otherwise,
         build and return a produce request containing some or all of the
         messages stored within, and serialize the produce request to output
         buffer 'dst', which will be resized to the exact size of the
         serialized request.

         We only assign partitions to AnyPartition messages here, since the
         router thread has already assigned partitions to PartitionKey
         messages.  For each topic, all AnyPartition messages are assigned to
         the same partition.  This partition is chosen in a round-robin manner
         by rotating through all of the topic's partitions that reside on the
         broker we are sending to.

         When Kafka sends us a produce response, the ordering of the topics in
         the response may differ from the ordering of the topics in the
         request.  Therefore, if we sent a produce request containing two
         message sets with identical topics and partitions, it would be
         impossible to determine which ACK from the response corresponds to
         which message set.  To eliminate this ambiguity, we create the produce
         request so that all messages are grouped first by topic and then by
         partition.  Then each message set has a unique topic/partition
         combination.  A single message set may contain a mixture of
         AnyPartition and PartitionKey messages. */
      Base::TOpt<TProduceRequest> BuildRequest(std::vector<uint8_t> &dst);

      private:
      struct TTopicData {
        /* This is null in the case where no compression is used. */
        const Compress::TCompressionCodecApi * const CompressionCodec;

        /* Minimum total size of uncompressed message bodies required for
           compression to be used. */
        const size_t MinCompressionSize;

        const int8_t CompressionAttributes;

        TAnyPartitionChooser AnyPartitionChooser;

        TTopicData(const Conf::TCompressionConf::TConf &compression_conf,
            const KafkaProto::TWireProtocol &kafka_protocol)
            : CompressionCodec(
                  Compress::GetCompressionCodec(compression_conf.Type)),
              MinCompressionSize(compression_conf.MinSize),
              CompressionAttributes(kafka_protocol.GetCompressionAttributes(
                  compression_conf.Type)) {
        }
      };  // TTopicData

      void InitTopicDataMap(const Conf::TCompressionConf &compression_conf);

      TTopicData &GetTopicData(const std::string &topic);

      size_t AddFirstMsg(TAllTopics &result);

      bool TryConsumeFrontMsg(std::list<TMsg::TPtr> &next_batch,
          const std::string &topic, TTopicData &topic_data,
          size_t &result_data_size, TAllTopics &result);

      TAllTopics BuildRequestContents();

      void SerializeUncompressedMsgSet(const std::list<TMsg::TPtr> &msg_set,
          std::vector<uint8_t> &dst);

      void SerializeToCompressionBuf(const std::list<TMsg::TPtr> &msg_set);

      void WriteOneMsgSet(const TMsgSet &msg_set, const TTopicData &topic_data,
          std::vector<uint8_t> &dst);

      const TConfig &Config;

      const size_t BrokerIndex;

      const KafkaProto::TWireProtocol &KafkaProtocol;

      const size_t ProduceRequestDataLimit;

      const size_t MessageMaxBytes;

      /* If (compressed message set size / uncompressed message set size)
         exceeds this value, then we send it uncompressed so the broker avoids
         spending CPU cycles dealing with the compression. */
      const float MaxCompressionRatio;

      const int16_t RequiredAcks;

      const int32_t ReplicationTimeout;

      /* Temporary hack to support legacy output format. */
      const bool AddTimestamp;

      /* Temporary hack to support legacy output format. */
      const bool UseOldOutputFormat;

      const std::unique_ptr<KafkaProto::TProduceRequestWriterApi>
          RequestWriter;

      const std::unique_ptr<KafkaProto::TMsgSetWriterApi> MsgSetWriter;

      Conf::TCompressionConf::TConf DefaultTopicConf;

      std::shared_ptr<TMetadata> Metadata;

      /* Correlation ID counter. */
      int32_t CorrIdCounter;

      /* Batches of messages to be combined into produce requests. */
      std::list<std::list<TMsg::TPtr>> InputQueue;

      /* Key is topic and value is TTopicData pertaining to topic. */
      std::unordered_map<std::string, TTopicData> TopicDataMap;

      /* Compression work area.  A message set is first written here, and then
         compressed into the destination buffer for the serialized produce
         request. */
      std::vector<uint8_t> CompressionBuf;
    };  // TProduceRequestFactory

  }  // MsgDispatch

}  // Bruce
