/* <bruce/metadata.h>

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

   This represents metadata information obtained from a metadata response sent
   by a Kafka broker.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <base/no_copy_semantics.h>
#include <base/thrower.h>

namespace Bruce {

  class TMetadata final {
    NO_COPY_SEMANTICS(TMetadata);

    public:
    /* Exception base class for metadata errors. */
    class TBadMetadata : public std::runtime_error {
      public:
      virtual ~TBadMetadata() noexcept { }

      protected:
      explicit TBadMetadata(const char *msg)
          : std::runtime_error(msg) {
      }
    };  // TBadMetadata

    DEFINE_ERROR(TDuplicateBroker, TBadMetadata,
        "Metadata contains duplicate broker ID");

    DEFINE_ERROR(TDuplicateTopic, TBadMetadata,
        "Metadata contains duplicate topic");

    DEFINE_ERROR(TPartitionHasUnknownBroker, TBadMetadata,
        "Metadata contains partition with unknown broker ID");

    DEFINE_ERROR(TDuplicatePartition, TBadMetadata,
        "Metadata contains duplicate partition for topic");

    class TBroker final {
      public:
      int32_t GetId() const {
        assert(this);
        return Id;
      }

      const std::string &GetHostname() const {
        assert(this);
        return Hostname;
      }

      uint16_t GetPort() const {
        assert(this);
        return Port;
      }

      bool IsInService() const {
        assert(this);
        return InService;
      }

      TBroker(const TBroker &) = default;

      TBroker(TBroker &&) = default;

      TBroker &operator=(const TBroker &) = default;

      TBroker &operator=(TBroker &&) = default;

      bool operator==(const TBroker &that) const;

      bool operator!=(const TBroker &that) const {
        assert(this);
        return !(*this == that);
      }

      private:
      TBroker(int32_t id, std::string &&hostname, uint16_t port)
          : Id(id),
            Hostname(std::move(hostname)),
            Port(port),
            InService(false) {
      }

      void MarkInService() {
        assert(this);
        InService = true;
      }

      /* Broker ID from Kafka. */
      int32_t Id;

      /* Host to connect to. */
      std::string Hostname;

      /* Port to connect to. */
      uint16_t Port;

      /* True if broker has at least one partition that can receive
         messages. */
      bool InService;

      friend class TMetadata;
    };  // TBroker

    class TPartition final {
      public:
      int32_t GetId() const {
        assert(this);
        return Id;
      }

      size_t GetBrokerIndex() const {
        assert(this);
        return BrokerIndex;
      }

      int16_t GetErrorCode() const {
        assert(this);
        return ErrorCode;
      }

      TPartition(const TPartition &) = default;

      TPartition &operator=(const TPartition &) = default;

      private:
      TPartition(int32_t id, size_t broker_index, int16_t error_code)
          : Id(id),
            BrokerIndex(broker_index),
            ErrorCode(error_code) {
      }

      /* Partition ID from Kafka. */
      int32_t Id;

      /* Vector index of TBroker describing where this partition resides. */
      size_t BrokerIndex;

      /* Kafka error code. */
      int16_t ErrorCode;

      friend class TMetadata;
    };  // TPartition

    private:
    class TPartitionChoices final {
      public:
      size_t GetTopicBrokerVecIndex() const {
        assert(this);
        return TopicBrokerVecIndex;
      }

      size_t GetTopicBrokerVecNumItems() const {
        assert(this);
        return TopicBrokerVecNumItems;
      }

      TPartitionChoices(const TPartitionChoices &) = default;

      TPartitionChoices &operator=(const TPartitionChoices &) = default;

      TPartitionChoices(size_t topic_broker_vec_index,
          size_t topic_broker_vec_num_items)
          : TopicBrokerVecIndex(topic_broker_vec_index),
            TopicBrokerVecNumItems(topic_broker_vec_num_items) {
      }

      private:
      /* Start index in TopicBrokerVec of item range providing partitions to
         choose from for a particular topic/broker combination. */
      size_t TopicBrokerVecIndex;

      /* Item count of item range in TopicBrokerVec providing partitions to
         choose from for a particular topic/broker combination. */
      size_t TopicBrokerVecNumItems;
    };  // TPartitionChoices

    public:
    class TTopic final {
      public:
      const std::vector<TPartition> &GetOkPartitions() const {
        assert(this);
        return OkPartitions;
      }

      const std::vector<TPartition> &GetOutOfServicePartitions() const {
        assert(this);
        return OutOfServicePartitions;
      }

      const std::vector<TPartition> &GetAllPartitions() const {
        assert(this);
        return AllPartitions;
      }

      TTopic(TTopic &&) = default;

      TTopic &operator=(TTopic &&) = default;

      private:
      TTopic() = default;

      /* Partitions for this topic that messages can be sent to (error codes
         are OK according to CanUsePartition()). */
      std::vector<TPartition> OkPartitions;

      /* Partitions that are currently unavailable. */
      std::vector<TPartition> OutOfServicePartitions;

      /* This contains the union of 'OkPartitions' and
         'OutOfServicePartitions', sorted in ascending order by Kafka partition
         ID.  It is used for choosing a partition for PartitionKey messages.
         For a message with partition key k, we first examine item
         AllPartitions[k % AllPartitions.size()].  If this partition is in
         service, we choose it.  Otherwise we walk forward in the vector
         (wrapping around if we reach the end) until we find an in service
         partition.  This ensures that if partition p becomes temporarily
         unavailable, keys that previously mapped to p are instead mapped to a
         deterministically chosen healthy partition while keys that previously
         mapped to other partitions retain their mappings. */
      std::vector<TPartition> AllPartitions;

      /* Key is broker index (in vector returned by TMetadata::GetBrokers()).
         For a given broker, this indicates which partitions the broker has for
         this topic. */
      std::unordered_map<size_t, TPartitionChoices> PartitionChoiceMap;

      friend class TMetadata;
    };  // TTopic

    class TBuilder final {
      NO_COPY_SEMANTICS(TBuilder);

      public:
      TBuilder();

      void OpenBrokerList();

      void AddBroker(int32_t kafka_id, std::string &&hostname, uint16_t port);

      void CloseBrokerList();

      void OpenTopic(const std::string &name);

      void AddPartitionToTopic(int32_t partition_id, int32_t broker_id,
          int16_t error_code);

      void CloseTopic();

      TMetadata *Build();

      void Reset() {
        assert(this);
        *this = TBuilder();
      }

      private:
      enum class TState {
        Initial,
        AddingBrokers,
        AddingTopics,
        AddingOneTopic
      };  // TState

      TBuilder &operator=(TBuilder &&) = default;

      void GroupInServiceBrokers();

      std::default_random_engine RandomEngine;

      TState State;

      /* Key is Kafka ID, and value is index in 'Brokers' vector. */
      std::unordered_map<int32_t, size_t> BrokerMap;

      size_t CurrentTopicIndex;

      std::unordered_set<int32_t> CurrentTopicPartitions;

      std::vector<TBroker> Brokers;

      size_t InServiceBrokerCount;

      std::vector<int32_t> TopicBrokerVec;

      std::vector<TTopic> Topics;

      std::unordered_map<std::string, size_t> TopicNameToIndex;
    };  // TBuilder

    TMetadata(TMetadata &&) = default;

    TMetadata &operator=(TMetadata &&) = default;

    bool operator==(const TMetadata &that) const;

    bool operator!=(const TMetadata &that) const {
      assert(this);
      return !(*this == that);
    }

    const std::vector<TBroker> &GetBrokers() const {
      assert(this);
      return Brokers;
    }

    size_t NumInServiceBrokers() const {
      assert(this);
      return InServiceBrokerCount;
    }

    const std::vector<TTopic> &GetTopics() const {
      assert(this);
      return Topics;
    }

    const std::unordered_map<std::string, size_t> &GetTopicNameMap() const {
      assert(this);
      return TopicNameToIndex;
    }

    /* Return index of given topic in vector returned by GetTopics(), or -1 if
       topic not found.

       TODO: Consider an alternative means of reporting "topic not found".
       It's easy to accidentally store the return value in a variable of type
       size_t, test the value for negativity, and not detect a case where a
       topic doesn't exist. */
    int FindTopicIndex(const std::string &topic) const;

    /* For the given topic and broker (identified by index in vector returned
       by GetBrokers()), return a pointer to an array of partition IDs to
       choose from, or nullptr if topic has no partitions whose leader resides
       on the given broker.  On return, 'num_choices' will contain # of
       elements in returned array, or 0 in case where nullptr is returned. */
    const int32_t *FindPartitionChoices(const std::string &topic,
        size_t broker_index, size_t &num_choices) const;

    /* For testing.  Return true if the sanity test passes, or false otherwise.
     */
    bool SanityCheck() const;

    private:
    /* FIXME: this should go in protocol-specific code. */
    static bool CanUsePartition(int16_t error_code) {
      /* An error code of 9 means "replica not available".  In this case, it is
         still OK to send to the leader. */
      return (error_code == 0) || (error_code == 9);
    }

    TMetadata(std::vector<TBroker> &&brokers, size_t in_service_broker_count,
        std::vector<int32_t> &&topic_broker_vec, std::vector<TTopic> &&topics,
        std::unordered_map<std::string, size_t> &&topic_name_to_index)
        : Brokers(std::move(brokers)),
          InServiceBrokerCount(in_service_broker_count),
          TopicBrokerVec(std::move(topic_broker_vec)),
          Topics(std::move(topics)),
          TopicNameToIndex(std::move(topic_name_to_index)) {
    }

    bool CompareBrokers(const TMetadata &that) const;

    bool SingleTopicCompare(const TMetadata &that, const TTopic &this_topic,
                            const TTopic &that_topic) const;

    bool CompareTopics(const TMetadata &that) const;

    /* Kafka brokers.  All brokers that are not in service are at the end. */
    std::vector<TBroker> Brokers;

    /* Number of brokers having at least one partition that can receive
       messages. */
    size_t InServiceBrokerCount;

    /* Vector of contiguous chunks of items, where each chunk represents all
       healthy partitions (with error code of 0 or 9) for a given topic/broker
       combination.  Each chunk is referenced by a TPartitionChoices struct. */
    std::vector<int32_t> TopicBrokerVec;

    /* All topics (including those with nonzero error codes). */
    std::vector<TTopic> Topics;

    /* Key is topic name.  Value is index of TTopic in 'Topics' vector. */
    std::unordered_map<std::string, size_t> TopicNameToIndex;
  };  // TMetadata

}  // Bruce
