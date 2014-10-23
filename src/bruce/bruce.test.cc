/* <bruce/bruce.test.cc>

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

   End to end test for bruce daemon using mock Kafka server.
 */

#include <bruce/input_thread.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <boost/lexical_cast.hpp>
#include <netinet/in.h>
#include <syslog.h>
#include <unistd.h>

#include <base/error_utils.h>
#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/field_access.h>
#include <base/no_copy_semantics.h>
#include <base/opt.h>
#include <base/tmp_file.h>
#include <base/tmp_file_name.h>
#include <bruce/anomaly_tracker.h>
#include <bruce/bruce_server.h>
#include <bruce/client/bruce_client.h>
#include <bruce/client/bruce_client_socket.h>
#include <bruce/client/status_codes.h>
#include <bruce/config.h>
#include <bruce/debug/debug_setup.h>
#include <bruce/input_thread.h>
#include <bruce/kafka_proto/choose_proto.h>
#include <bruce/kafka_proto/v0/wire_proto.h>
#include <bruce/metadata_timestamp.h>
#include <bruce/msg_state_tracker.h>
#include <bruce/router_thread.h>
#include <bruce/test_util/mock_kafka_config.h>
#include <bruce/util/misc_util.h>
#include <bruce/util/time_util.h>
#include <bruce/util/worker_thread.h>
#include <capped/pool.h>

#include <gtest/gtest.h>

using namespace Base;
using namespace Bruce;
using namespace Bruce::Client;
using namespace Bruce::Conf;
using namespace Bruce::Debug;
using namespace Bruce::KafkaProto;
using namespace Bruce::KafkaProto::V0;
using namespace Bruce::MockKafkaServer;
using namespace Bruce::TestUtil;
using namespace Bruce::Util;
using namespace Capped;

namespace {

  class TBruceTestServer final : public TWorkerThread {
    NO_COPY_SEMANTICS(TBruceTestServer);

    public:
    TBruceTestServer(in_port_t broker_port, size_t msg_buffer_max_kb,
        const std::string &bruce_conf)
        : BrokerPort(broker_port),
          MsgBufferMaxKb(msg_buffer_max_kb),
          BruceConf(bruce_conf),
          BruceReturnValue(EXIT_FAILURE),
          DoNotInit(false) {
    }

    TBruceTestServer(in_port_t broker_port, size_t msg_buffer_max_kb,
        std::string &&bruce_conf)
        : BrokerPort(broker_port),
          MsgBufferMaxKb(msg_buffer_max_kb),
          BruceConf(std::move(bruce_conf)),
          BruceReturnValue(EXIT_FAILURE),
          DoNotInit(false) {
    }

    virtual ~TBruceTestServer() noexcept;

    const char *GetUnixSocketName() const {
      assert(this);
      return UnixSocketName;
    }

    /* This must not be called until Start() method of TWorkerThread base class
       has been called.  Returns pointer to bruce server object, or nullptr on
       bruce server initialization failure. */
    TBruceServer *GetBruce();

    virtual void RequestShutdown() override;

    int GetBruceReturnValue() const {
      assert(this);
      return BruceReturnValue;
    }

    protected:
    virtual void Run() override;

    private:
    TTmpFileName UnixSocketName;

    in_port_t BrokerPort;

    size_t MsgBufferMaxKb;

    std::string BruceConf;

    int BruceReturnValue;

    TEventSemaphore InitSem;

    std::mutex InitMutex;

    bool DoNotInit;

    std::unique_ptr<TBruceServer> Bruce;
  };  // TBruceTestServer

  TBruceTestServer::~TBruceTestServer() noexcept {
    /* This will shut down the thread if something unexpected happens. */
    ShutdownOnDestroy();
  }

  TBruceServer *TBruceTestServer::GetBruce() {
    assert(this);

    if (!InitSem.GetFd().IsReadable(30000)) {
      std::cerr << "Bruce server failed to start after 30 seconds."
          << std::endl;
      return nullptr;
    }

    if (!Bruce) {
      return nullptr;
    }

    if (!Bruce->GetInitWaitFd().IsReadable(30000)) {
      std::cerr << "Bruce server failed to create UNIX socket after 30 "
          << "seconds." << std::endl;
      return nullptr;
    }

    return Bruce.get();
  }

  void TBruceTestServer::RequestShutdown() {
    assert(this);

    {
      std::lock_guard<std::mutex> lock(InitMutex);

      if (!Bruce) {
        /* If we get here, either the server thread has not yet tried to create
           the TBruceServer object, or it has tried and failed.  In the former
           case, setting this flag will cause it to terminate immediately
           rather than creating the server.  In the latter case, setting this
           flag has no effect but we have nothing to do since the server thread
           is already terminating or has terminated. */
        DoNotInit = true;
        return;
      }
    }

    /* If we get this far, then the server thread has created the server, so we
       must take action. */

    /* We must do this because the server thread isn't paying attention to the
       FD returned by TWorkerThread::GetShutdownRequestFd(). */
    Bruce->RequestShutdown();

    /* In this case, we don't really need to call this method, but calling it
       does no harm, and it follows the usual pattern for dealing with
       TWorkerThread objects. */
    TWorkerThread::RequestShutdown();
  }

  void TBruceTestServer::Run() {
    assert(this);
    BruceReturnValue = EXIT_FAILURE;
    TTmpFile tmp_file;
    tmp_file.SetDeleteOnDestroy(true);
    std::ofstream ofs(tmp_file.GetName());
    ofs << BruceConf;
    ofs.close();
    std::string msg_buffer_max_str =
        boost::lexical_cast<std::string>(MsgBufferMaxKb);
    std::vector<const char *> args;
    args.push_back("bruce");
    args.push_back("--config_path");
    args.push_back(tmp_file.GetName());
    args.push_back("--msg_buffer_max");
    args.push_back(msg_buffer_max_str.c_str());
    args.push_back("--receive_socket_name");
    args.push_back(UnixSocketName);
    args.push_back("--log_level");
    args.push_back("LOG_INFO");
    // args.push_back("--log_echo");
    args.push_back(nullptr);

    TOpt<TBruceServer::TServerConfig> bruce_config;

    try {
      bool large_sendbuf_required = false;
      bruce_config.MakeKnown(TBruceServer::CreateConfig(
          args.size() - 1, const_cast<char **>(&args[0]),
          large_sendbuf_required));
      const Bruce::TConfig &config = bruce_config->GetCmdLineConfig();
      InitSyslog(args[0], config.LogLevel, config.LogEcho);

      {
        std::lock_guard<std::mutex> lock(InitMutex);

        if (DoNotInit) {
          /* We have already received a shutdown request.  Quietly terminate.
           */
          BruceReturnValue = EXIT_SUCCESS;
          return;
        }

        Bruce.reset(new TBruceServer(std::move(*bruce_config)));
      }

      bruce_config.Reset();
      Bruce->BindStatusSocket(true);
      InitSem.Push();
      BruceReturnValue = Bruce->Run();
    } catch (const std::exception &x) {
      std::cerr << "Server error: " << x.what() << std::endl;
      InitSem.Push();
    } catch (...) {
      std::cerr << "Unknown server error" << std::endl;
      InitSem.Push();
    }
  }

   /* Create simple configuration with batching and compression disabled. */
  std::string CreateSimpleBruceConf(in_port_t broker_port) {
    std::ostringstream os;
    os << "<?xml version=\"1.0\" encoding=\"US-ASCII\"?>" << std::endl
       << "<bruceConfig>" << std::endl
       << "    <batching>" << std::endl
       << "        <produceRequestDataLimit value=\"0\" />" << std::endl
       << "        <messageMaxBytes value=\"1024k\" />" << std::endl
       << "        <combinedTopics enable=\"false\" />" << std::endl
       << "        <defaultTopic action=\"disable\" />" << std::endl
       << "    </batching>" << std::endl
       << "    <compression>" << std::endl
       << "        <namedConfigs>" << std::endl
       << "            <config name=\"noComp\" type=\"none\" />" << std::endl
       << "        </namedConfigs>" << std::endl
       << std::endl
       << "        <defaultTopic config=\"noComp\" />" << std::endl
       << "    </compression>" << std::endl
       << "    <initialBrokers>" << std::endl
       << "        <broker host=\"localhost\" port=\"" << broker_port <<"\" />"
       << std::endl
       << "    </initialBrokers>" << std::endl
       << "</bruceConfig>" << std::endl;
    return os.str();
  }

  void CreateKafkaConfig(const char *topic, std::vector<std::string> &result) {
    result.clear();

    /* Add contents to a setup file for the mock Kafka server.
       This line tells the server to simulate 2 brokers on consecutive ports
       starting at 10000.  During the test, bruce will connect to these ports
       and forward messages it gets from the UNIX domain socket. */
    result.push_back("ports 10000 2");

    /* This line tells the mock Kafka server to create a single topic with the
       given name, containing 2 partitions.  The 0 at the end specifies that
       the first partition should be on the broker whose port is at offset 0
       from the starting port (10000 above).  The remaining partitions are
       distributed among the brokers in round-robin fashion on consecutive
       ports. */
    std::string line("topic ");
    line += topic;
    line += " 2 0";
    result.push_back(line);
  }

  void MakeDg(std::vector<uint8_t> &dg, const std::string &topic,
      const std::string &body) {
    size_t dg_size = 0;
    int ret = bruce_find_any_partition_msg_size(topic.size(), 0,
            body.size(), &dg_size);
    ASSERT_EQ(ret, BRUCE_OK);
    dg.resize(dg_size);
    ret = bruce_write_any_partition_msg(&dg[0], dg.size(), topic.c_str(),
            GetEpochMilliseconds(), nullptr, 0, body.data(), body.size());
    ASSERT_EQ(ret, BRUCE_OK);
  }

  void MakeDg(std::vector<uint8_t> &dg, const std::string &topic,
      const std::string &key, const std::string &value) {
    size_t dg_size = 0;
    int ret = bruce_find_any_partition_msg_size(topic.size(), key.size(),
            value.size(), &dg_size);
    ASSERT_EQ(ret, BRUCE_OK);
    dg.resize(dg_size);
    ret = bruce_write_any_partition_msg(&dg[0], dg.size(), topic.c_str(),
            GetEpochMilliseconds(), key.data(), key.size(), value.data(),
            value.size());
    ASSERT_EQ(ret, BRUCE_OK);
  }

  void GetKeyAndValue(TBruceServer &bruce,
      Bruce::MockKafkaServer::TMainThread &mock_kafka,
      const std::string &topic, const std::string &key,
      const std::string &value, size_t expected_ack_count,
      size_t expected_msg_count,
      TCompressionType compression_type = TCompressionType::None) {
    for (size_t i = 0;
         (bruce.GetAckCount() < expected_ack_count) && (i < 3000);
         ++i) {
      SleepMilliseconds(10);
    }

    using TTracker = TReceivedRequestTracker;
    std::list<TTracker::TRequestInfo> received;
    bool got_msg_set = false;

    for (size_t i = 0; i < 3000; ++i) {
      mock_kafka.NonblockingGetHandledRequests(received);

      for (auto &item : received) {
        if (item.MetadataRequestInfo.IsKnown()) {
          ASSERT_EQ(item.MetadataRequestInfo->ReturnedErrorCode, 0);
        } else if (item.ProduceRequestInfo.IsKnown()) {
          ASSERT_FALSE(got_msg_set);
          const TTracker::TProduceRequestInfo &info = *item.ProduceRequestInfo;
          ASSERT_EQ(info.Topic, topic);
          ASSERT_EQ(info.ReturnedErrorCode, 0);
          ASSERT_EQ(info.FirstMsgKey, key);
          ASSERT_EQ(info.FirstMsgValue, value);
          ASSERT_EQ(info.MsgCount, expected_msg_count);
          ASSERT_TRUE(info.CompressionType == compression_type);
          got_msg_set = true;
        } else {
          ASSERT_TRUE(false);
        }
      }

      received.clear();

      if (got_msg_set) {
        break;
      }

      SleepMilliseconds(10);
    }

    ASSERT_TRUE(got_msg_set);
  }

  /* Fixture for end to end Bruce unit test. */
  class TBruceTest : public ::testing::Test {
    protected:
    TBruceTest() {
    }

    virtual ~TBruceTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TBruceTest

  TEST_F(TBruceTest, SuccessfulDeliveryTest) {
    std::string topic("scooby_doo");
    std::vector<std::string> kafka_config;
    CreateKafkaConfig(topic.c_str(), kafka_config);
    TMockKafkaConfig kafka(kafka_config);
    kafka.StartKafka();
    Bruce::MockKafkaServer::TMainThread &mock_kafka = *kafka.MainThread;

    /* Translate virtual port from the mock Kafka server setup file into a
       physical port.  See big comment in <bruce/mock_kafka_server/port_map.h>
       for an explanation of what is going on here. */
    in_port_t port = mock_kafka.VirtualPortToPhys(10000);

    assert(port);
    TBruceTestServer server(port, 1024, CreateSimpleBruceConf(port));
    server.Start();
    TBruceServer *bruce = server.GetBruce();
    ASSERT_TRUE(bruce != nullptr);

    if (bruce == nullptr) {
      return;
    }

    TBruceClientSocket sock;
    int ret = sock.Bind(server.GetUnixSocketName());
    ASSERT_EQ(ret, BRUCE_OK);
    std::vector<std::string> topics;
    std::vector<std::string> bodies;
    topics.push_back(topic);
    bodies.push_back("Scooby");
    topics.push_back(topic);
    bodies.push_back("Shaggy");
    topics.push_back(topic);
    bodies.push_back("Velma");
    topics.push_back(topic);
    bodies.push_back("Daphne");
    std::vector<uint8_t> dg_buf;

    for (size_t i = 0; i < topics.size(); ++i) {
      MakeDg(dg_buf, topics[i], bodies[i]);
      ret = sock.Send(&dg_buf[0], dg_buf.size());
      ASSERT_EQ(ret, BRUCE_OK);
    }

    for (size_t i = 0; (bruce->GetAckCount() < 4) && (i < 3000); ++i) {
      SleepMilliseconds(10);
    }

    ASSERT_EQ(bruce->GetAckCount(), 4U);
    using TTracker = TReceivedRequestTracker;
    std::list<TTracker::TRequestInfo> received;
    std::vector<std::string> expected_msgs;
    assert(topics.size() == bodies.size());

    for (size_t i = 0; i < topics.size(); ++i) {
      expected_msgs.push_back(bodies[i]);
    }

    for (size_t i = 0; i < 3000; ++i) {
      mock_kafka.NonblockingGetHandledRequests(received);

      for (auto &item : received) {
        if (item.MetadataRequestInfo.IsKnown()) {
          ASSERT_EQ(item.MetadataRequestInfo->ReturnedErrorCode, 0);
        } else if (item.ProduceRequestInfo.IsKnown()) {
          const TTracker::TProduceRequestInfo &info = *item.ProduceRequestInfo;
          ASSERT_EQ(info.Topic, topic);
          ASSERT_EQ(info.ReturnedErrorCode, 0);
          auto iter = std::find(expected_msgs.begin(), expected_msgs.end(),
                                info.FirstMsgValue);

          if (iter == expected_msgs.end()) {
            ASSERT_TRUE(false);
          } else {
            expected_msgs.erase(iter);
          }
        } else {
          ASSERT_TRUE(false);
        }
      }

      received.clear();

      if (expected_msgs.empty()) {
        break;
      }

      SleepMilliseconds(10);
    }

    ASSERT_TRUE(expected_msgs.empty());

    TAnomalyTracker::TInfo bad_stuff;
    bruce->GetAnomalyTracker().GetInfo(bad_stuff);
    ASSERT_EQ(bad_stuff.DiscardTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.DuplicateTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
    ASSERT_EQ(bad_stuff.MalformedMsgCount, 0U);
    ASSERT_EQ(bad_stuff.UnsupportedVersionMsgCount, 0U);

    server.RequestShutdown();
    server.Join();
    ASSERT_EQ(server.GetBruceReturnValue(), EXIT_SUCCESS);
  }

  TEST_F(TBruceTest, KeyValueTest) {
    std::string topic("scooby_doo");
    std::vector<std::string> kafka_config;
    CreateKafkaConfig(topic.c_str(), kafka_config);
    TMockKafkaConfig kafka(kafka_config);
    kafka.StartKafka();
    Bruce::MockKafkaServer::TMainThread &mock_kafka = *kafka.MainThread;

    /* Translate virtual port from the mock Kafka server setup file into a
       physical port.  See big comment in <bruce/mock_kafka_server/port_map.h>
       for an explanation of what is going on here. */
    in_port_t port = mock_kafka.VirtualPortToPhys(10000);

    assert(port);
    TBruceTestServer server(port, 1024, CreateSimpleBruceConf(port));
    server.Start();
    TBruceServer *bruce = server.GetBruce();
    ASSERT_TRUE(bruce != nullptr);

    if (bruce == nullptr) {
      return;
    }

    TBruceClientSocket sock;
    int ret = sock.Bind(server.GetUnixSocketName());
    ASSERT_EQ(ret, BRUCE_OK);
    std::string key, value;
    std::vector<uint8_t> dg_buf;
    size_t expected_ack_count = 0;

    /* empty key and value */
    MakeDg(dg_buf, topic, key, value);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);
    GetKeyAndValue(*bruce, mock_kafka, topic, key, value, ++expected_ack_count,
                   1);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);
    GetKeyAndValue(*bruce, mock_kafka, topic, key, value, ++expected_ack_count,
                   1);

    /* nonempty key and empty value */
    key = "Scooby";
    value = "";
    MakeDg(dg_buf, topic, key, value);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);
    GetKeyAndValue(*bruce, mock_kafka, topic, key, value, ++expected_ack_count,
                   1);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);
    GetKeyAndValue(*bruce, mock_kafka, topic, key, value, ++expected_ack_count,
                   1);

    /* empty key and nonempty value */
    key = "";
    value = "Shaggy";
    MakeDg(dg_buf, topic, key, value);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);
    GetKeyAndValue(*bruce, mock_kafka, topic, key, value, ++expected_ack_count,
                   1);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);
    GetKeyAndValue(*bruce, mock_kafka, topic, key, value, ++expected_ack_count,
                   1);

    /* nonempty key and nonempty value */
    key = "Velma";
    value = "Daphne";
    MakeDg(dg_buf, topic, key, value);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);
    GetKeyAndValue(*bruce, mock_kafka, topic, key, value, ++expected_ack_count,
                   1);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);
    GetKeyAndValue(*bruce, mock_kafka, topic, key, value, ++expected_ack_count,
                   1);

    TAnomalyTracker::TInfo bad_stuff;
    bruce->GetAnomalyTracker().GetInfo(bad_stuff);
    ASSERT_EQ(bad_stuff.DiscardTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.DuplicateTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
    ASSERT_EQ(bad_stuff.MalformedMsgCount, 0U);
    ASSERT_EQ(bad_stuff.UnsupportedVersionMsgCount, 0U);

    server.RequestShutdown();
    server.Join();
    ASSERT_EQ(server.GetBruceReturnValue(), EXIT_SUCCESS);
  }

  TEST_F(TBruceTest, AckErrorTest) {
    std::string topic("scooby_doo");
    std::vector<std::string> kafka_config;
    CreateKafkaConfig(topic.c_str(), kafka_config);
    TMockKafkaConfig kafka(kafka_config);
    kafka.StartKafka();
    Bruce::MockKafkaServer::TMainThread &mock_kafka = *kafka.MainThread;

    /* Translate virtual port from the mock Kafka server setup file into a
       physical port.  See big comment in <bruce/mock_kafka_server/port_map.h>
       for an explanation of what is going on here. */
    in_port_t port = mock_kafka.VirtualPortToPhys(10000);

    assert(port);
    TBruceTestServer server(port, 1024, CreateSimpleBruceConf(port));
    server.Start();
    TBruceServer *bruce = server.GetBruce();
    ASSERT_TRUE(bruce != nullptr);

    if (bruce == nullptr) {
      return;
    }

    std::string msg_body("rejected on 1st attempt");

    /* Error code 6 is "not leader for partition", which causes the ProdMngr to
       push the pause button. */
    bool success = kafka.Inj.InjectAckError(6, msg_body.c_str(), nullptr);
    ASSERT_TRUE(success);

    /* Inject a metadata response error.  In response to the pause, the router
       thread will request metadata, and get this injected error.  Since the
       error will leave no remaining valid topics, bruce will send another
       metadata request. */
    success = kafka.Inj.InjectAllTopicsMetadataResponseError(-1, topic.c_str(),
                                                             nullptr);
    ASSERT_TRUE(success);

    /* Kafka is having a really bad day today.  To make things interesting,
       make the mock Kafka server disconnect rather than sending a response on
       the second attempted metadata request from bruce.  bruce should try
       again and succeed on the third attempt. */
    success =
        kafka.Inj.InjectDisconnectBeforeAllTopicsMetadataResponse(nullptr);
    ASSERT_TRUE(success);

    TBruceClientSocket sock;
    int ret = sock.Bind(server.GetUnixSocketName());
    ASSERT_EQ(ret, BRUCE_OK);
    std::vector<uint8_t> dg_buf;
    MakeDg(dg_buf, topic, msg_body);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);

    std::cout << "This part of the test is expected to take a while ..."
        << std::endl;

    /* We should get 2 ACKs: the first will be the injected error and the
       second will indicate successful redelivery. */
    for (size_t i = 0; (bruce->GetAckCount() < 2) && (i < 3000); ++i) {
      SleepMilliseconds(10);
    }

    ASSERT_EQ(bruce->GetAckCount(), 2U);
    using TTracker = TReceivedRequestTracker;
    std::list<TTracker::TRequestInfo> received;

    for (size_t i = 0; (received.size() < 6) && (i < 3000); ++i) {
      mock_kafka.NonblockingGetHandledRequests(received);
      SleepMilliseconds(10);
    }

    ASSERT_EQ(received.size(), 6U);

    /* initial metadata request from daemon startup */
    TTracker::TRequestInfo *req_info = &received.front();
    ASSERT_TRUE(req_info->MetadataRequestInfo.IsKnown());
    ASSERT_EQ(req_info->MetadataRequestInfo->ReturnedErrorCode, 0);
    received.pop_front();

    /* injected error ACK */
    req_info = &received.front();
    ASSERT_TRUE(req_info->ProduceRequestInfo.IsKnown());
    TTracker::TProduceRequestInfo *prod_req_info =
        &*req_info->ProduceRequestInfo;
    ASSERT_EQ(prod_req_info->Topic, topic);
    ASSERT_EQ(prod_req_info->FirstMsgValue, msg_body);
    ASSERT_EQ(prod_req_info->ReturnedErrorCode, 6);
    received.pop_front();

    /* metadata request due to pause (injected metadata response error) */
    req_info = &received.front();
    ASSERT_TRUE(req_info->MetadataRequestInfo.IsKnown());
    ASSERT_EQ(req_info->MetadataRequestInfo->ReturnedErrorCode, -1);
    received.pop_front();

    /* failed metadata request retry after injected metadata response error */
    req_info = &received.front();
    ASSERT_TRUE(req_info->MetadataRequestInfo.IsKnown());
    ASSERT_EQ(req_info->MetadataRequestInfo->ReturnedErrorCode, 0);
    received.pop_front();

    /* successful metadata request retry after injected disconnect */
    req_info = &received.front();
    ASSERT_TRUE(req_info->MetadataRequestInfo.IsKnown());
    ASSERT_EQ(req_info->MetadataRequestInfo->ReturnedErrorCode, 0);
    received.pop_front();

    /* successful redelivery ACK */
    req_info = &received.front();
    ASSERT_TRUE(req_info->ProduceRequestInfo.IsKnown());
    prod_req_info = &*req_info->ProduceRequestInfo;
    ASSERT_EQ(prod_req_info->Topic, topic);
    ASSERT_EQ(prod_req_info->FirstMsgValue, msg_body);
    ASSERT_EQ(prod_req_info->ReturnedErrorCode, 0);
    received.pop_front();

    ASSERT_TRUE(received.empty());

    /* Send another message (this time with no error injection) to make sure
       bruce is still healthy. */
    msg_body = "another message";
    MakeDg(dg_buf, topic, msg_body);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);

    /* The ACK count should be incremented from its previous value of 2. */
    for (size_t i = 0; (bruce->GetAckCount() < 3) && (i < 3000); ++i) {
      SleepMilliseconds(10);
    }

    ASSERT_EQ(bruce->GetAckCount(), 3U);

    for (size_t i = 0; received.empty() && (i < 3000); ++i) {
      mock_kafka.NonblockingGetHandledRequests(received);
      SleepMilliseconds(10);
    }

    ASSERT_FALSE(received.empty());

    /* successful delivery ACK */
    req_info = &received.front();
    ASSERT_TRUE(req_info->ProduceRequestInfo.IsKnown());
    prod_req_info = &*req_info->ProduceRequestInfo;
    ASSERT_EQ(prod_req_info->Topic, topic);
    ASSERT_EQ(prod_req_info->FirstMsgValue, msg_body);
    ASSERT_EQ(prod_req_info->ReturnedErrorCode, 0);
    received.pop_front();

    ASSERT_TRUE(received.empty());

    TAnomalyTracker::TInfo bad_stuff;
    bruce->GetAnomalyTracker().GetInfo(bad_stuff);
    ASSERT_EQ(bad_stuff.DiscardTopicMap.size(), 0U);

    /* Because of the message redelivery due to the injected error, the daemon
       currently reports 1 here.  This is overly pessimistic, since the ACK
       error clearly indicated failed delivery rather than some ambiguous
       result.  Overly pessimistic is ok, but overly optimistic is not.
       However we can still improve this behavior eventually. */
    ASSERT_LE(bad_stuff.DuplicateTopicMap.size(), 1U);

    ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
    ASSERT_EQ(bad_stuff.MalformedMsgCount, 0U);
    ASSERT_EQ(bad_stuff.UnsupportedVersionMsgCount, 0U);

    server.RequestShutdown();
    server.Join();
    ASSERT_EQ(server.GetBruceReturnValue(), EXIT_SUCCESS);
  }

  TEST_F(TBruceTest, DisconnectTest) {
    std::string topic("scooby_doo");
    std::vector<std::string> kafka_config;
    CreateKafkaConfig(topic.c_str(), kafka_config);
    TMockKafkaConfig kafka(kafka_config);
    kafka.StartKafka();
    Bruce::MockKafkaServer::TMainThread &mock_kafka = *kafka.MainThread;

    /* Translate virtual port from the mock Kafka server setup file into a
       physical port.  See big comment in <bruce/mock_kafka_server/port_map.h>
       for an explanation of what is going on here. */
    in_port_t port = mock_kafka.VirtualPortToPhys(10000);

    assert(port);
    TBruceTestServer server(port, 1024, CreateSimpleBruceConf(port));
    server.Start();
    TBruceServer *bruce = server.GetBruce();
    ASSERT_TRUE(bruce != nullptr);

    if (bruce == nullptr) {
      return;
    }

    std::string msg_body("rejected on 1st attempt");

    /* Make the mock Kafka server close the TCP connection rather than send an
       ACK (simulated broker crash). */
    bool success = kafka.Inj.InjectDisconnectBeforeAck(msg_body.c_str(),
                                                       nullptr);
    ASSERT_TRUE(success);

    TBruceClientSocket sock;
    int ret = sock.Bind(server.GetUnixSocketName());
    ASSERT_EQ(ret, BRUCE_OK);
    std::vector<uint8_t> dg_buf;
    MakeDg(dg_buf, topic, msg_body);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);

    /* We should get a single ACK when the message is successfully redelivered
       after the simulated broker crash. */
    for (size_t i = 0; (bruce->GetAckCount() < 1) && (i < 3000); ++i) {
      SleepMilliseconds(10);
    }

    ASSERT_EQ(bruce->GetAckCount(), 1U);
    using TTracker = TReceivedRequestTracker;
    std::list<TTracker::TRequestInfo> received;

    for (size_t i = 0; (received.size() < 3) && (i < 3000); ++i) {
      mock_kafka.NonblockingGetHandledRequests(received);
      SleepMilliseconds(10);
    }

    ASSERT_EQ(received.size(), 3U);

    /* initial metadata request from daemon startup */
    TTracker::TRequestInfo *req_info = &received.front();
    ASSERT_TRUE(req_info->MetadataRequestInfo.IsKnown());
    ASSERT_EQ(req_info->MetadataRequestInfo->ReturnedErrorCode, 0);
    received.pop_front();

    /* metadata request due to pause */
    req_info = &received.front();
    ASSERT_TRUE(req_info->MetadataRequestInfo.IsKnown());
    ASSERT_EQ(req_info->MetadataRequestInfo->ReturnedErrorCode, 0);
    received.pop_front();

    /* successful redelivery ACK */
    req_info = &received.front();
    ASSERT_TRUE(req_info->ProduceRequestInfo.IsKnown());
    TTracker::TProduceRequestInfo *prod_req_info =
        &*req_info->ProduceRequestInfo;
    ASSERT_EQ(prod_req_info->Topic, topic);
    ASSERT_EQ(prod_req_info->FirstMsgValue, msg_body);
    ASSERT_EQ(prod_req_info->ReturnedErrorCode, 0);
    received.pop_front();

    ASSERT_TRUE(received.empty());

    /* Send another message (this time with no error injection) to make sure
       bruce is still healthy. */
    msg_body = "another message";
    MakeDg(dg_buf, topic, msg_body);
    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);

    /* The ACK count should be incremented from its previous value of 1. */
    for (size_t i = 0; (bruce->GetAckCount() < 2) && (i < 3000); ++i) {
      SleepMilliseconds(10);
    }

    ASSERT_EQ(bruce->GetAckCount(), 2U);

    for (size_t i = 0; received.empty() && (i < 3000); ++i) {
      mock_kafka.NonblockingGetHandledRequests(received);
      SleepMilliseconds(10);
    }

    ASSERT_FALSE(received.empty());

    /* successful delivery ACK */
    req_info = &received.front();
    ASSERT_TRUE(req_info->ProduceRequestInfo.IsKnown());
    prod_req_info = &*req_info->ProduceRequestInfo;
    ASSERT_EQ(prod_req_info->Topic, topic);
    ASSERT_EQ(prod_req_info->FirstMsgValue, msg_body);
    ASSERT_EQ(prod_req_info->ReturnedErrorCode, 0);
    received.pop_front();

    ASSERT_TRUE(received.empty());

    TAnomalyTracker::TInfo bad_stuff;
    bruce->GetAnomalyTracker().GetInfo(bad_stuff);
    ASSERT_EQ(bad_stuff.DiscardTopicMap.size(), 0U);

    /* This count is 1 due to the simulated broker crash.  Since the broker
       "crashed" before sending an ACK, bruce doesn't know whether the broker
       received the message.  Therefore bruce resends it, possibly creating a
       duplicate. */
    ASSERT_EQ(bad_stuff.DuplicateTopicMap.size(), 1U);

    ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
    ASSERT_EQ(bad_stuff.MalformedMsgCount, 0U);
    ASSERT_EQ(bad_stuff.UnsupportedVersionMsgCount, 0U);

    server.RequestShutdown();
    server.Join();
    ASSERT_EQ(server.GetBruceReturnValue(), EXIT_SUCCESS);
  }

  TEST_F(TBruceTest, MalformedMsgTest) {
    std::string topic("scooby_doo");
    std::vector<std::string> kafka_config;
    CreateKafkaConfig(topic.c_str(), kafka_config);
    TMockKafkaConfig kafka(kafka_config);
    kafka.StartKafka();
    Bruce::MockKafkaServer::TMainThread &mock_kafka = *kafka.MainThread;

    /* Translate virtual port from the mock Kafka server setup file into a
       physical port.  See big comment in <bruce/mock_kafka_server/port_map.h>
       for an explanation of what is going on here. */
    in_port_t port = mock_kafka.VirtualPortToPhys(10000);

    assert(port);
    TBruceTestServer server(port, 1024, CreateSimpleBruceConf(port));
    server.Start();
    TBruceServer *bruce = server.GetBruce();
    ASSERT_TRUE(bruce != nullptr);

    if (bruce == nullptr) {
      return;
    }

    /* This message will get discarded because it's malformed. */
    std::string msg_body("I like scooby snacks");
    TBruceClientSocket sock;
    int ret = sock.Bind(server.GetUnixSocketName());
    ASSERT_EQ(ret, BRUCE_OK);
    std::vector<uint8_t> dg_buf;
    MakeDg(dg_buf, topic, msg_body);

    /* Overwrite the size field with an incorrect value. */
    ASSERT_GE(dg_buf.size(), sizeof(int32_t));
    WriteInt32ToHeader(&dg_buf[0], dg_buf.size() - 1);

    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);

    for (size_t num_tries = 0; ; ++num_tries) {
      if (num_tries > 30) {
        /* Test timed out. */
        ASSERT_TRUE(false);
        break;
      }

      SleepMilliseconds(1000);
      TAnomalyTracker::TInfo bad_stuff;
      bruce->GetAnomalyTracker().GetInfo(bad_stuff);

      if (bad_stuff.MalformedMsgCount == 0) {
        continue;
      }

      ASSERT_EQ(bad_stuff.MalformedMsgCount, 1U);
      ASSERT_EQ(bad_stuff.MalformedMsgs.size(), 1U);

      ASSERT_EQ(bad_stuff.UnsupportedVersionMsgCount, 0U);
      ASSERT_EQ(bad_stuff.DuplicateTopicMap.size(), 0U);
      ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
      break;  // success
    }

    server.RequestShutdown();
    server.Join();
    ASSERT_EQ(server.GetBruceReturnValue(), EXIT_SUCCESS);
  }

  TEST_F(TBruceTest, UnsupportedVersionMsgTest) {
    std::string topic("scooby_doo");
    std::vector<std::string> kafka_config;
    CreateKafkaConfig(topic.c_str(), kafka_config);
    TMockKafkaConfig kafka(kafka_config);
    kafka.StartKafka();
    Bruce::MockKafkaServer::TMainThread &mock_kafka = *kafka.MainThread;

    /* Translate virtual port from the mock Kafka server setup file into a
       physical port.  See big comment in <bruce/mock_kafka_server/port_map.h>
       for an explanation of what is going on here. */
    in_port_t port = mock_kafka.VirtualPortToPhys(10000);

    assert(port);
    TBruceTestServer server(port, 1024, CreateSimpleBruceConf(port));
    server.Start();
    TBruceServer *bruce = server.GetBruce();
    ASSERT_TRUE(bruce != nullptr);

    if (bruce == nullptr) {
      return;
    }

    /* This message will get discarded because it's malformed. */
    std::string msg_body("I like scooby snacks");
    TBruceClientSocket sock;
    int ret = sock.Bind(server.GetUnixSocketName());
    ASSERT_EQ(ret, BRUCE_OK);
    std::vector<uint8_t> dg_buf;
    MakeDg(dg_buf, topic, msg_body);

    /* Overwrite the version field with a bad value. */
    ASSERT_GE(dg_buf.size(), sizeof(int32_t) + sizeof(int8_t));
    dg_buf[4] = -1;

    ret = sock.Send(&dg_buf[0], dg_buf.size());
    ASSERT_EQ(ret, BRUCE_OK);

    for (size_t num_tries = 0; ; ++num_tries) {
      if (num_tries > 30) {
        /* Test timed out. */
        ASSERT_TRUE(false);
        break;
      }

      SleepMilliseconds(1000);
      TAnomalyTracker::TInfo bad_stuff;
      bruce->GetAnomalyTracker().GetInfo(bad_stuff);

      if (bad_stuff.UnsupportedVersionMsgCount == 0) {
        continue;
      }

      ASSERT_EQ(bad_stuff.UnsupportedVersionMsgCount, 1U);
      auto iter = bad_stuff.UnsupportedVersionMsgs.find(-1);
      ASSERT_FALSE(iter == bad_stuff.UnsupportedVersionMsgs.end());
      ASSERT_EQ(iter->first, -1);

      ASSERT_EQ(bad_stuff.MalformedMsgCount, 0U);
      ASSERT_EQ(bad_stuff.MalformedMsgs.size(), 0U);
      ASSERT_EQ(bad_stuff.DuplicateTopicMap.size(), 0U);
      ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
      break;  // success
    }

    server.RequestShutdown();
    server.Join();
    ASSERT_EQ(server.GetBruceReturnValue(), EXIT_SUCCESS);
  }

  std::string CreateCompressionTestConf(in_port_t broker_port,
      size_t compression_min_size) {
    std::ostringstream os;
    os << "<?xml version=\"1.0\" encoding=\"US-ASCII\"?>" << std::endl
       << "<bruceConfig>" << std::endl
       << "    <batching>" << std::endl
       << "        <namedConfigs>" << std::endl
       << "            <config name=\"config1\">" << std::endl
       << "                <time value=\"disable\" />" << std::endl
       << "                <messages value=\"10\" />" << std::endl
       << "                <bytes value=\"disable\" />" << std::endl
       << "            </config>" << std::endl
       << "        </namedConfigs>" << std::endl
       << "        <produceRequestDataLimit value=\"1024k\" />" << std::endl
       << "        <messageMaxBytes value=\"1024k\" />" << std::endl
       << "        <combinedTopics enable=\"false\" />" << std::endl
       << "        <defaultTopic action=\"perTopic\" config=\"config1\" />"
       << std::endl
       << "    </batching>" << std::endl
       << "    <compression>" << std::endl
       << "        <namedConfigs>" << std::endl
       << "            <config name=\"config1\" type=\"snappy\" minSize=\""
       << boost::lexical_cast<std::string>(compression_min_size) << "\" />"
       << std::endl
       << "        </namedConfigs>" << std::endl
       << std::endl
       << "        <defaultTopic config=\"config1\" />" << std::endl
       << "    </compression>" << std::endl
       << "    <initialBrokers>" << std::endl
       << "        <broker host=\"localhost\" port=\"" << broker_port <<"\" />"
       << std::endl
       << "    </initialBrokers>" << std::endl
       << "</bruceConfig>" << std::endl;
    return os.str();
  }

  TEST_F(TBruceTest, CompressionTest) {
    TWireProto proto(0, 0, false);
    std::string topic("scooby_doo");
    std::vector<std::string> kafka_config;
    CreateKafkaConfig(topic.c_str(), kafka_config);
    TMockKafkaConfig kafka(kafka_config);
    kafka.StartKafka();
    Bruce::MockKafkaServer::TMainThread &mock_kafka = *kafka.MainThread;

    /* Translate virtual port from the mock Kafka server setup file into a
       physical port.  See big comment in <bruce/mock_kafka_server/port_map.h>
       for an explanation of what is going on here. */
    in_port_t port = mock_kafka.VirtualPortToPhys(10000);

    assert(port);
    std::string msg_body_1("123456789");
    size_t data_size = msg_body_1.size() + proto.GetSingleMsgOverhead();
    TBruceTestServer server(port, 1024,
        CreateCompressionTestConf(port, 1 + (10 * data_size)));
    server.Start();
    TBruceServer *bruce = server.GetBruce();
    ASSERT_TRUE(bruce != nullptr);

    if (bruce == nullptr) {
      return;
    }

    TBruceClientSocket sock;
    int ret = sock.Bind(server.GetUnixSocketName());
    ASSERT_EQ(ret, BRUCE_OK);
    std::vector<std::string> topics;
    std::vector<std::string> bodies;

    /* These will be batched together as a single message set, but compression
       will not be used because of the size threshold. */
    for (size_t i = 0; i < 10; ++i) {
      topics.push_back(topic);
      bodies.push_back(msg_body_1);
    }

    std::vector<uint8_t> dg_buf;

    for (size_t i = 0; i < topics.size(); ++i) {
      MakeDg(dg_buf, topics[i], bodies[i]);
      ret = sock.Send(&dg_buf[0], dg_buf.size());
      ASSERT_EQ(ret, BRUCE_OK);
    }

    GetKeyAndValue(*bruce, mock_kafka, topic, "", msg_body_1, 1, 10,
                               TCompressionType::None);

    /* This will push the total size to the threshold and cause compression. */
    bodies[9].push_back('0');

    for (size_t i = 0; i < topics.size(); ++i) {
      MakeDg(dg_buf, topics[i], bodies[i]);
      ret = sock.Send(&dg_buf[0], dg_buf.size());
      ASSERT_EQ(ret, BRUCE_OK);
    }

    GetKeyAndValue(*bruce, mock_kafka, topic, "", msg_body_1, 1, 10,
                   TCompressionType::Snappy);

    TAnomalyTracker::TInfo bad_stuff;
    bruce->GetAnomalyTracker().GetInfo(bad_stuff);
    ASSERT_EQ(bad_stuff.DiscardTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.DuplicateTopicMap.size(), 0U);
    ASSERT_EQ(bad_stuff.BadTopics.size(), 0U);
    ASSERT_EQ(bad_stuff.MalformedMsgCount, 0U);
    ASSERT_EQ(bad_stuff.UnsupportedVersionMsgCount, 0U);

    server.RequestShutdown();
    server.Join();
    ASSERT_EQ(server.GetBruceReturnValue(), EXIT_SUCCESS);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
