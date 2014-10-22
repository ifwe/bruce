/* <bruce/router_thread.h>

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

   Router thread for bruce daemon.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <netinet/in.h>
#include <poll.h>

#include <base/event_semaphore.h>
#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <base/timer_fd.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/batch/global_batch_config.h>
#include <bruce/batch/per_topic_batcher.h>
#include <bruce/conf/conf.h>
#include <bruce/conf/topic_rate_conf.h>
#include <bruce/config.h>
#include <bruce/debug/debug_logger.h>
#include <bruce/debug/debug_setup.h>
#include <bruce/kafka_proto/wire_protocol.h>
#include <bruce/metadata_timestamp.h>
#include <bruce/metadata.h>
#include <bruce/metadata_fetcher.h>
#include <bruce/msg.h>
#include <bruce/msg_dispatch/kafka_dispatcher_api.h>
#include <bruce/msg_rate_limiter.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/router_thread_api.h>
#include <bruce/util/bruce_rate_limiter.h>
#include <bruce/util/gate.h>
#include <bruce/util/host_and_port.h>
#include <bruce/util/poll_array.h>
#include <bruce/util/worker_thread.h>

namespace Bruce {

  class TRouterThread final : public Util::TWorkerThread,
                              public TRouterThreadApi {
    NO_COPY_SEMANTICS(TRouterThread);

    public:
    TRouterThread(const TConfig &config, const Conf::TConf &conf,
        const KafkaProto::TWireProtocol &kafka_protocol,
        TAnomalyTracker &anomaly_tracker, TMsgStateTracker &msg_state_tracker,
        const Batch::TGlobalBatchConfig &batch_config,
        const Debug::TDebugSetup &debug_setup,
        MsgDispatch::TKafkaDispatcherApi &dispatcher);

    virtual ~TRouterThread() noexcept;

    /* Return a file descriptor that becomes readable when the router thread
       has finished its initialization and is open for business.

       This method is used only by unit test code.  The input thread does not
       wait for the router thread to finish its initialization, since the input
       thread must immediately be ready to read datagrams from its socket.  In
       the case where the Kafka cluster is temporarily unavailable, router
       thread initialization can take arbitrarily long. */
    virtual const Base::TFd &GetInitWaitFd() const override;

    virtual TShutdownStatus GetShutdownStatus() const override;

    virtual TMsgChannel &GetMsgChannel() override;

    /* Returns the total number of ACKs received from Kafka.  Used for testing.
     */
    virtual size_t GetAckCount() const override;

    virtual Base::TEventSemaphore &GetMetadataUpdateRequestSem() override;

    const TMetadataTimestamp &GetMetadataTimestamp() const {
      assert(this);
      return MetadataTimestamp;
    }

    protected:
    virtual void Run() override;

    private:
    class TShutdownOnDestroy final : public std::runtime_error {
      public:
      TShutdownOnDestroy() 
          : std::runtime_error("TShutdownOnDestroy") {
      }

      virtual ~TShutdownOnDestroy() noexcept { }
    };  // TShutdownOnDestroy

    using TKafkaBroker = Util::THostAndPort;

    static size_t ComputeRetryDelay(size_t mean_delay, size_t div);

    void StartShutdown();

    void Discard(TMsg::TPtr &&msg, TAnomalyTracker::TDiscardReason reason);

    void Discard(std::list<TMsg::TPtr> &&msg_list,
        TAnomalyTracker::TDiscardReason reason);

    void Discard(std::list<std::list<TMsg::TPtr>> &&batch_list,
        TAnomalyTracker::TDiscardReason reason);

    bool UpdateMetadataAfterTopicAutocreate(const std::string &topic);

    /* A false return value indicates that we started a metadata fetch after
       successful automatic topic creation, and the shutdown delay expired
       during metadata fetch.  Therefore we should terminate execution.  A true
       return value means "keep executing".  In the above-mentioned case where
       false is returned, or in case of topic autocreate failure, 'msg' will be
       discarded, and empty on return.  Otherwise 'msg' retains its
       contents. */
    bool AutocreateTopic(TMsg::TPtr &msg);

    /* A false return value indicates that we started a metadata fetch due to
       automatic topic creation, and the shutdown delay expired during metadata
       fetch.  Therefore we should terminate execution.  A true return value
       means "keep executing".  In the above-mentioned case where false is
       returned, or in case of validation failure, 'msg' will be discarded and
       empty on return.  Otherwise 'msg' retains its contents. */
    bool ValidateNewMsg(TMsg::TPtr &msg);

    void ValidateBeforeReroute(std::list<TMsg::TPtr> &msg_list);

    /* Parameter 'topic' _must_ be known to be valid.  Look up topic in
       metadata and return its index. */
    size_t LookupValidTopicIndex(const std::string &topic) const;

    /* Parameter 'topic' _must_ be known to be valid.  Look up topic in
       metadata and return its metadata. */
    const TMetadata::TTopic &GetValidTopicMetadata(
        const std::string &topic) const {
      assert(this);
      assert(Metadata);
      return Metadata->GetTopics()[LookupValidTopicIndex(topic)];
    }

    size_t ChooseAnyPartitionBrokerIndex(const std::string &topic);

    const TMetadata::TPartition &ChoosePartitionByKey(
        const TMetadata::TTopic &topic_meta, int32_t partition_key);

    const TMetadata::TPartition &ChoosePartitionByKey(const std::string &topic,
        int32_t partition_key) {
      assert(this);
      assert(Metadata);

      /* All topics are validated before routing, so parameter 'topic' should
         always be valid. */
      return ChoosePartitionByKey(GetValidTopicMetadata(topic), partition_key);
    }

    size_t AssignBroker(TMsg::TPtr &msg);

    /* Route a single message.  Batch if appropriate. */
    void Route(TMsg::TPtr &&msg);

    /* Route a single message, but do not batch. */
    void RouteNow(TMsg::TPtr &&msg);

    /* Route a list of message batches.  For each batch, all messages have the
       same topic, and all have routing type AnyPartition.  Batching at the
       broker level will be bypassed. */
    void RouteAnyPartitionNow(std::list<std::list<TMsg::TPtr>> &&batch_list);

    /* Route a list of message batches.  For each batch, all messages have the
       same topic, and all have routing type PartitionKey.  Batching at the
       broker level will be bypassed. */
    void RoutePartitionKeyNow(std::list<std::list<TMsg::TPtr>> &&batch_list);

    /* Reroute a list of message batches obtained from the dispatcher after it
       has shut down in preparation for new metadata.  For each batch, all
       messages have the same topic, although their routing types may differ.
       Batching at the broker level will be bypassed.  Before routing,
       revalidate all messages based on the updated metadata. */
    void Reroute(std::list<std::list<TMsg::TPtr>> &&batch_list);

    void RouteFinalMsgs();

    void DiscardFinalMsgs();

    bool Init();

    void GetDispatcherShutdownStatus();

    bool ReplaceMetadataOnRefresh(std::shared_ptr<TMetadata> &&meta);

    bool RefreshMetadata();

    std::list<std::list<TMsg::TPtr>> EmptyDispatcher();

    bool RespondToPause();

    void DiscardOnShutdownDuringMetadataUpdate(TMsg::TPtr &&msg);

    void DiscardOnShutdownDuringMetadataUpdate(
        std::list<TMsg::TPtr> &&msg_list);

    void DiscardOnShutdownDuringMetadataUpdate(
        std::list<std::list<TMsg::TPtr>> &&batch_list);

    bool HandleMetadataUpdate();

    void ContinueShutdown();

    int ComputeMainLoopPollTimeout();

    void InitMainLoopPollArray();

    void DoRun();

    void HandleShutdownFinished();

    void HandleBatchExpiry(uint64_t now);

    void HandleMsgAvailable(uint64_t now);

    bool HandlePause();

    void UpdateKnownBrokers(const TMetadata &md);

    /* Returned shared_ptr contains a TMetadata on success, or nothing on
       failure. */
    std::shared_ptr<TMetadata> TryGetMetadata();

    void InitMetadataRefreshTimer();

    /* Perform the initial metadata request during startup.  Keep trying to get
       metadata until we succeed or get a shutdown request.  Returned
       shared_ptr contains a TMetadata on success, or nothing if our attempts
       were cut short by a shutdown request.  This behavior can probably be
       improved on, but it should be good enough for now. */
    std::shared_ptr<TMetadata> GetInitialMetadata();

    std::shared_ptr<TMetadata> GetMetadataBeforeSlowShutdown();

    std::shared_ptr<TMetadata> GetMetadataDuringSlowShutdown();

    /* Keep trying to get metadata until we succeed or get a shutdown request.
       Returned shared_ptr contains a TMeta on success, or nothing if our
       attempts were cut short by a shutdown request.  This behavior can
       probably be improved on, but it should be good enough for now. */
    std::shared_ptr<TMetadata> GetMetadata();

    void UpdateBatchStateForNewMetadata(const TMetadata &old_md,
        const TMetadata &new_md);

    void SetMetadata(std::shared_ptr<TMetadata> &&meta,
        bool record_update = true);

    const TConfig &Config;

    /* Configuration for per-topic message rate limiting. */
    Conf::TTopicRateConf TopicRateConf;

    /* Limits message rates according to 'TopicRateConf'. */
    TMsgRateLimiter MsgRateLimiter;

    /* Header overhead for a single message.  For checking message size. */
    const size_t SingleMsgOverhead;

    /* Maximum total message size (key + value + header space (see
       'SingleMsgOverhead' above)) allowed by Kafka brokers. */
    const size_t MessageMaxBytes;

    /* For tracking discarded messages and possible duplicates. */
    TAnomalyTracker &AnomalyTracker;

    TMsgStateTracker &MsgStateTracker;

    const Debug::TDebugSetup &DebugSetup;

    /* This becomes readable when the router thread has finished its
       initialization and is open for business. */
    Base::TEventSemaphore InitFinishedSem;

    bool Destroying;

    /* Set to true when StartShutdown() has been called but ContinueShutdown()
       still needs to be called. */
    bool NeedToContinueShutdown;

    /* After the router thread has shut down, this indicates whether it shut
       down normally or with an error. */
    TShutdownStatus ShutdownStatus;

    /* The router thread receives messages from the input thread through this
       channel. */
    Util::TGate<TMsg::TPtr> MsgChannel;

    /* Object responsible for getting metadata requests from brokers. */
    TMetadataFetcher MetadataFetcher;

    /* List of known Kafka brokers.  We pick one of these when we need to send
       a metadata request. */
    std::vector<TKafkaBroker> KnownBrokers;

    /* Metadata used for routing messages to brokers. */
    std::shared_ptr<TMetadata> Metadata;

    /* The vector item indexes correspond to the topic indexes in the metadata.
       Each time a message or batch of messages is routed, the counter for that
       topic is incremented.  The counter values are used for broker selection.
       The value of a counter doesn't matter, as long as it increments each
       time a message for the corresponding topic is routed. */
    std::vector<size_t> RouteCounters;

    /* Per-topic batching for AnyPartition messages is done here, before
       messages get routed to a broker.  Per-topic batching for PartitionKey
       messages is done at the broker level. */
    Batch::TPerTopicBatcher PerTopicBatcher;

    /* Key is broker index (not ID) and value is list of messages grouped by
       topic.  Used as temporary storage when routing messages. */
    std::unordered_map<size_t, std::list<std::list<TMsg::TPtr>>> TmpBrokerMap;

    /* This becomes known whwnever the batcher has an expiration time.  It
       indicates the earliest expiration time of any topic batch. */
    Base::TOpt<TMsg::TTimestamp> OptNextBatchExpiry;

    /* The dispatcher handles the details of sending messages and receiving
       ACKs.  Once we decide which broker a message goes to, the dispatcher
       handles the rest. */
    MsgDispatch::TKafkaDispatcherApi &Dispatcher;

    enum class TMainLoopPollItem {
      Pause = 0,
      ShutdownRequest = 1,
      MsgAvailable = 2,
      MdUpdateRequest = 3,
      MdRefresh = 4,
      ShutdownFinished = 5
    };  // TMainLoopPollItem

    Util::TPollArray<TMainLoopPollItem, 6> MainLoopPollArray;

    /* This becomes known when a slow shutdown starts.  The units are
       milliseconds since the epoch. */
    Base::TOpt<uint64_t> ShutdownStartTime;

    /* When this FD befcomes readable, we refresh our metadata. */
    std::unique_ptr<Base::TTimerFd> MetadataRefreshTimer;

    /* Keeps track of when we last got metadata. */
    TMetadataTimestamp MetadataTimestamp;

    /* Prevents bruce from getting into a tight pause loop if something goes
       seriously wrong, and imposes a minimum delay before responding to a
       pause. */
    std::unique_ptr<Util::TBruceRateLimiter> PauseRateLimiter;

    /* Push to tell daemon to update its metadata. */
    Base::TEventSemaphore MetadataUpdateRequestSem;

    Debug::TDebugLogger DebugLogger;
  };  // TRouterThread

}  // Bruce
