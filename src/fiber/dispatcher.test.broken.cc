/* <fiber/dispatcher.test.cc>
 
   ----------------------------------------------------------------------------
   Copyright 2013 if(we)

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
 
   Unit test for <fiber/dispatcher.h>.
 */

#include <fiber/dispatcher.h>
  
#include <sstream>
#include <string>
#include <thread>
  
#include <base/error_utils.h>
#include <base/fd.h>
#include <socket/address.h>
  
#include <gtest/gtest.h>
  
using namespace std;
using namespace chrono;
using namespace Base;
using namespace Fiber;
using namespace Socket;

namespace {

  /* A simple TCP server to use for our tests.
     The server uses a single dispatcher to run all of its I/O.
     The server accepts any number of connections.
     Any data sent by a client is echoed back by the server.
     If a client is quiet for too long, the server hangs up on it. */
  class TServer final {
    NO_COPY_SEMANTICS(TServer);
    public:
    /* We shutdown on this signal. */
    static constexpr int SignalNumber = SIGUSR1;
  
    /* Construct a server which will accept connections on a system-assigned
       port.  Return the port number via out-param. */
    TServer(in_port_t &port)
        : Timeouts(0) {
      /* Construct an acceptor (below) and get the port number from it.
         The acceptor registers itself with the dispatcher so we don't need to
         keep track of it. */
      port = (new TAcceptor(this))->GetPort();
    }
  
    /* See TDispatcher::GetHandlerCount(). */
    size_t GetHandlerCount() const {
      assert(this);
      return Dispatcher.GetHandlerCount();
    }
  
    /* The number of times we have hung up on a client that was quiet for too
       long. */
    size_t GetTimeouts() const {
      assert(this);
      return Timeouts;
    }
  
    /* See TDispatcher::Run(). */
    void Run() {
      assert(this);
      Dispatcher.Run(seconds(2), {}, SignalNumber);
    }
  
    /* See TDispatcher::Shutdown(). */
    void Shutdown(thread &t) {
      assert(this);
      Dispatcher.Shutdown(t, SignalNumber);
    }
  
    private:
    /* A handler for I/O with a connected client. */
    class TWorker final
        : public TDispatcher::THandler {
      public:
      /* We are constructed by the acceptor when it accepts a connection.
         It passes us an already-connected socket, which we take ownership of.
         After that we accept messages from the client, which we echo back. */
      TWorker(TServer *server, TFd &&fd)
          : Server(server), Fd(move(fd)), Limit(Buffer), Shutdown(false) {
        /* Register as a handler of ready-to-read events on our socket. */
        assert(server);
        Register(&(server->Dispatcher), Fd, POLLIN);
        SetDeadline(Timeout);
      }
  
      /* Unregister from the dispatcher before we go.  This is mandatory. */
      virtual ~TWorker() noexcept {
        assert(this);
        Unregister();
      }
  
      /* Called by the dispatcher when our client has been quiet for too long.
       */
      virtual void OnDeadline() override {
        assert(this);
        ++(Server->Timeouts);
        delete this;
      }
  
      /* Called by the dispatcher when our socket has a connection waiting. */
      virtual void OnEvent(int, short) override {
        assert(this);
        if (Limit == Buffer) {
          /* Read data from the client.  If we get some, register to write it
             back. */
          ssize_t result = recv(Fd, Buffer, sizeof(Buffer), 0);

          if (result > 0) {
            /* We got some data.  Switch over to writing so we can echo it
               back. */
            Limit = Buffer + result;
            ChangeEvent(Fd, POLLOUT);
            SetDeadline(Timeout);
          } else {
            /* We didn't get data, so it's time to go. */
            delete this;
          }
        } else {
          /* Write our data to the client.  If successful, register to read
             again. */
          ssize_t result = send(Fd, Buffer, Limit - Buffer, MSG_NOSIGNAL);

          if (result > 0) {
            /* We sent some data. */
            if (Shutdown) {
              /* If there's a shutdown underway, don't try to read from the
                 client again. */
              delete this;
            } else {
              /* Go back to reading from the client. */
              Limit = Buffer;
              ChangeEvent(Fd, POLLIN);
              SetDeadline(Timeout);
            }
          } else {
            /* We failed to send any data, so it's time to go. */
            delete this;
          }
        }
      }
  
      /* Called by the dispatcher when a shutdown begins. */
      virtual void OnShutdown() override {
        assert(this);
        if (Limit > Buffer) {
          /* We are waiting to echo to the client, so don't die yet. */
          Shutdown = true;
        } else {
          /* We have nothing better to do than die. */
          delete this;
        }
      }
  
      private:
      /* The server of which we are a part. */
      TServer *Server;
  
      /* The socket that's connected to the client. */
      TFd Fd;
  
      /* A workspace for reading and writing. */
      char Buffer[1000];
  
      /* Points into Buffer, above, to indicate where the last read message
         ends. */
      char *Limit;
  
      /* If true, then we should self-destruct after we reply to the client. */
      bool Shutdown;
  
      /* The amount of time we'll wait for I/O with the client. */
      static const TTimeout Timeout;
  
    };  // TServer::TWorker
  
    /* We are constructed by the server when it itself constructs.
       We handle accept requests. */
    class TAcceptor final
        : public TDispatcher::THandler {
      public:
  
      /* Construct and register. */
      TAcceptor(TServer *server)
          : Server(server) {
        assert(server);
        /* Make a socket on which to accept connections. */
        TAddress address(TAddress::IPv4Any);
        Fd = TFd(socket(address.GetFamily(), SOCK_STREAM, 0));
        int flag = true;
        IfLt0(setsockopt(Fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag)));
        /* Bind to a system-assigned port number and start listening. */
        Bind(Fd, address);
        IfLt0(listen(Fd, 100));
        /* Cache the port number we were assigned. */
        address = GetSockName(Fd);
        Port = address.GetPort();
        /* Register as a handler of connection events on our socket. */
        Register(&(server->Dispatcher), Fd, POLLIN);
      }
  
      /* Unregister from the dispatcher before we go.  This is mandatory. */
      virtual ~TAcceptor() noexcept {
        assert(this);
        Unregister();
      }
  
      /* The port number on which we're listening. */
      in_port_t GetPort() const noexcept {
        assert(this);
        return Port;
      }
  
      private:
  
      /* Called by the dispatcher when our socket has a connection waiting. */
      virtual void OnEvent(int, short) {
        assert(this);
        new TWorker(Server, TFd(accept(Fd, nullptr, nullptr)));
      }
  
      /* Called by the dispatcher when a shutdown begins. */
      virtual void OnShutdown() {
        delete this;
      }
  
      /* The server of which we are a part. */
      TServer *Server;
  
      /* The socket on which we're listening. */
      TFd Fd;
  
      /* See accessor. */
      in_port_t Port;
  
    };  // TServer::TAcceptor
  
    /* See accessor. */
    size_t Timeouts;
  
    /* The dispatcher managing all the server's I/O. */
    TDispatcher Dispatcher;
  };  // TServer
  
  /* See declaration. */
  const TServer::TWorker::TTimeout TServer::TWorker::Timeout = milliseconds(500);
  
  /* Client threads enter here. */
  void ClientMain(
      size_t idx,  // the unique id number of this client
      in_port_t port,  // the port on which the server is listening
      int io_count,  // the number of I/O rounds to try with the server, or -1
                     // to go forever
      atomic_size_t &pass_count,  // counts successful I/O rounds
      atomic_size_t &fail_count,  // counts unsuccessful I/O rounds
      atomic_size_t &rude_count) { // counts number of threads which end rudely
    try {
      /* Connect to the server. */
      TFd fd(socket(AF_INET, SOCK_STREAM, 0));
      Connect(fd, TAddress(TAddress::IPv4Any, port));
      /* Do some rounds of I/O. */
      char actual[1000];

      for (int i = 0; i < io_count || io_count < 0; ++i) {
        /* Compose a unique message to send. */
        string expected;

        /* extra */ {
          ostringstream strm;
          strm << "client #" << idx << ", call #" << i;
          expected = strm.str();
        }

        /* Send the message and expect to get it echoed back. */
        IfLt0(send(fd, expected.data(), expected.size(), MSG_NOSIGNAL));
        memset(actual, 0, sizeof(actual));
        IfLt0(recv(fd, actual, sizeof(actual), 0));
        ++((expected == actual) ? pass_count : fail_count);
      }
    } catch (...) {
      ++rude_count;
    }
  }

  /* The fixture for testing class TDispatcher. */
  class TDispatcherTest : public ::testing::Test {
    protected:
    TDispatcherTest() {
    }

    virtual ~TDispatcherTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
  };  // TDispatcherTest

  TEST_F(TDispatcherTest, ShutdownAfterClients) {
    const size_t
        repeat_count = 10,  // number of times to repeat the whole test
        client_count = 10,  // number of clients connecting to the test server
        io_count     = 10;  // number of I/O rounds conducted by each client
    /* Repeat the whole test a few times. */
    for (size_t repeat = 0; repeat < repeat_count; ++repeat) {
      /* Construct a server and launch it in a background thread. */
      in_port_t port;
      TServer server(port);
      auto server_thread = thread(&TServer::Run, &server);
      /* Launch some client threads.
         These clients will disconnect on their own after completing their I/O
         rounds. */
      atomic_size_t pass_count(0), fail_count(0), rude_count(0);
      vector<thread> clients;

      for (size_t idx = 0; idx < client_count; ++idx) {
        clients.push_back(thread(ClientMain, idx, port, io_count,
            ref(pass_count), ref(fail_count), ref(rude_count)));
      }

      /* Wait for the clients to finish. */
      for (auto &client: clients) {
        client.join();
      }

      /* Shut down the server and make sure it went cleanly. */
      server.Shutdown(server_thread);
      ASSERT_EQ(server.GetHandlerCount(), 0u);
      ASSERT_EQ(server.GetTimeouts(), 0u);
      /* How did we do? */
      ASSERT_EQ(pass_count, client_count * io_count);
      ASSERT_EQ(fail_count, 0u);
      ASSERT_EQ(rude_count, 0u);
    }
  }
  
  TEST_F(TDispatcherTest, ShutdownBeforeClients) {
    const size_t
        repeat_count = 10,  // number of times to repeat the whole test
        client_count = 10;  // number of clients connecting to the test server

    /* Repeat the whole test a few times. */
    for (size_t repeat = 0; repeat < repeat_count; ++repeat) {
      /* Construct a server and launch it in a background thread. */
      in_port_t port;
      TServer server(port);
      auto server_thread = thread(&TServer::Run, &server);
      /* Launch some client threads.
         These clients will never voluntarily disconnect. */
      atomic_size_t pass_count(0), fail_count(0), rude_count(0);
      vector<thread> clients;

      for (size_t idx = 0; idx < client_count; ++idx) {
        clients.push_back(thread(ClientMain, idx, port, -1, ref(pass_count),
            ref(fail_count), ref(rude_count)));
      }

      /* Shut down the server and make sure it went cleanly. */
      server.Shutdown(server_thread);
      ASSERT_EQ(server.GetHandlerCount(), 0u);
      ASSERT_EQ(server.GetTimeouts(), 0u);

      /* Wait for the clients to finish. */
      for (auto &client: clients) {
        client.join();
      }

      /* How did we do? */
      ASSERT_EQ(rude_count, client_count);
    }
  }
  
  TEST_F(TDispatcherTest, UpAndDown) {
      /* Construct a server and launch it in a background thread. */
    in_port_t port;
    TServer server(port);
    auto server_thread = thread(&TServer::Run, &server);
    /* Connect to the server, do nothing for too long, and be hung up on. */
    TFd fd(socket(AF_INET, SOCK_STREAM, 0));
    Connect(fd, TAddress(TAddress::IPv4Any, port));
    sleep(1);
    char buf[1];
    ASSERT_EQ(recv(fd, buf, sizeof(buf), 0), 0);
    /* Shut down the server and make sure it went cleanly. */
    server.Shutdown(server_thread);
    ASSERT_EQ(server.GetHandlerCount(), 0u);
    ASSERT_EQ(server.GetTimeouts(), 1u);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
