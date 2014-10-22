/* <bruce/msg.h>

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

   This is a message received from a client.  It gets delivered to a Kafka
   broker.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <base/no_copy_semantics.h>
#include <capped/blob.h>

namespace Bruce {

  /* A message to send to a Kafka broker. */
  class TMsg final {
    NO_COPY_SEMANTICS(TMsg);

    public:
    /* Convenience. */
    using TPtr = std::unique_ptr<TMsg>;

    enum class TRoutingType {
      AnyPartition,
      PartitionKey
    };  // TRoutingType

    /* Message state. */
    enum class TState {
      /* The message was just received from the input socket, and has not yet
         been batched or routed for delivery to a broker. */
      New,

      /* The message is being batched. */
      Batching,

      /* The message has been batched if appropriate, and routed for delivery
         to a broker.  It is now waiting to be sent. */
      SendWait,

      /* The message has been sent to a broker, and is waiting for an ACK. */
      AckWait,

      /* The message has been processed.  In other words, we either got a
         sucessful ACK or have decided to discard it.  The message is about to
         be destroyed.  As a sanity check, the destructor verifies that the
         message is in this state. */
      Processed
    };  // TState

    /* A timestamp for a message.  Clients include timestamps with the messages
       they write to the input socket.  The units are milliseconds since the
       epoch.  We use a signed type because many clients are likely to be
       written in Java, Scala, or other JVM-based languages that suffer from
       the lack of native unsigned types.  Bruce uses the timestamps in its
       discard reports. */
    using TTimestamp = int64_t;

    TRoutingType GetRoutingType() const {
      assert(this);
      return RoutingType;
    }

    int32_t GetPartitionKey() const {
      assert(this);
      return PartitionKey;
    }

    /* Return the client's timestamp from the input datagram. */
    TTimestamp GetTimestamp() const {
      assert(this);
      return Timestamp;
    }

    /* Return our own monotonic raw timestamp that we use for per-topic message
       rate limiting. */
    uint64_t GetCreationTimestamp() const {
      assert(this);
      return CreationTimestamp;
    }

    /* Accessor for the Kafka topic string. */
    const std::string &GetTopic() const {
      assert(this);
      return Topic;
    }

    /* Accessor for the Kafka partition. */
    int32_t GetPartition() const {
      assert(this);
      return Partition;
    }

    /* Sets the Kafka partition to the given value. */
    void SetPartition(int32_t partition) {
      assert(this);
      Partition = partition;
    }

    bool GetErrorAckReceived() const {
      assert(this);
      return ErrorAckReceived;
    }

    void SetErrorAckReceived(bool value) {
      assert(this);
      ErrorAckReceived = value;
    }

    /* Accessor method for the message body. */
    const Capped::TBlob &GetKeyAndValue() const {
      assert(this);
      return KeyAndValue;
    }

    size_t GetKeySize() const {
      assert(this);
      assert(KeySize <= KeyAndValue.Size());
      return KeySize;
    }

    size_t GetValueSize() const {
      assert(this);
      assert(KeySize <= KeyAndValue.Size());
      return KeyAndValue.Size() - KeySize;
    }

    /* Returns true iff. the message body is truncated.  This happens to
       messages that exceed the maximum allowed length.

       TODO: remove this method once legacy input format goes away */
    bool BodyIsTruncated() const {
      assert(this);
      return BodyTruncated;
    }

    size_t CountFailedDeliveryAttempt() {
      assert(this);
      return ++FailedDeliveryAttemptCount;
    }

    size_t GetFailedDeliveryAttemptCount() const {
      assert(this);
      return FailedDeliveryAttemptCount;
    }

    TState GetState() const {
      assert(this);
      return State;
    }

    void SetState(TState state) {
      assert(this);
      State = state;
    }

    ~TMsg() noexcept;

    private:
    /* Create a message with the given topic and body.  'topic_begin' points to
       the first byte of the topic, and 'topic_end' points one byte past the
       last byte of the topic.  The topic and body contents are copied into the
       blob, with memory for the body being allocated from 'pool'.  Use routing
       type of 'AnyPartition'.

       Throws Capped::TMemoryCapReached if the pool doesn't contain enough
       memory to create the message. */
    static TPtr CreateAnyPartitionMsg(TTimestamp timestamp,
        const void *topic_begin, const void *topic_end, const void *key,
        size_t key_size, const void *value, size_t value_size,
        bool body_truncated, Capped::TPool &pool);

    /* Same as above, but use routing type of 'PartitionKey'. */
    static TPtr CreatePartitionKeyMsg(int32_t partition_key,
        TTimestamp timestamp, const void *topic_begin, const void *topic_end,
        const void *key, size_t key_size, const void *value, size_t value_size,
        bool body_truncated, Capped::TPool &pool);

    /* Constructor is used only by static Create() method. */
    TMsg(TRoutingType routing_type, int32_t partition_key,
         TTimestamp timestamp, const void *topic_begin, const void *topic_end,
         const void *key, size_t key_size, const void *value,
         size_t value_size, bool body_truncated, Capped::TPool &pool);

    const TRoutingType RoutingType;

    const int32_t PartitionKey;

    /* Message timestamp from input UNIX domain datagram. */
    const TTimestamp Timestamp;

    /* This timestamp is used for per-topic message rate limiting, and is set
       based on a call to clock_gettime() with a clock type of
       CLOCK_MONOTONIC_RAW.  We use this clock type specifically because it is
       unaffected by changes made to the system wall clock.  If we used
       CLOCK_REALTIME and someone manually set the clock back, this could cause
       large numbers of discards until the clock catches back up to its
       previous setting.  We don't use the timestamp value provided by the
       client's input UNIX domain datagram because we have no idea what kind of
       clock was used to generate that timestamp. */
    const uint64_t CreationTimestamp;

    /* State of message.  Destructor verifies that value is TState::Processed.
     */
    TState State;

    /* Number of failed deliveries. */
    size_t FailedDeliveryAttemptCount;

    /* The Kafka topic to deliver to. */
    const std::string Topic;

    /* The Kafka partition (within the specified topic) to deliver to. */
    int32_t Partition;

    bool ErrorAckReceived;

    /* The key and value stored as a single sequence of bytes.  The value
       immediately follows the key. */
    const Capped::TBlob KeyAndValue;

    /* The first 'KeySize' bytes of 'KeyAndValue' are the key.  The remaining
       bytes are the value. */
    size_t KeySize;

    /* True iff. the body was truncated.  This happens to messages that exceed
       the maximum allowed length. */
    const bool BodyTruncated;

    friend class TMsgCreator;
  };  // TMsg

}  // Bruce
