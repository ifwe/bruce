/* <socket/option.h>

   ----------------------------------------------------------------------------
   Copyright 2010-2013 if(we)

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

   Objects representing socket options.

   Constant instances of the standard socket objects appear at the bottom of
   this header.  Use these objects when you want to set options on your
   sockets:

       int my_sock = socket(...);
       Socket::KeepAlive.Set(my_sock, true);  // Turn on 'keep_alive' option.

   You can also use these objects to query the option's value for your socket:

      if (Socket::KeepAlive.Get(my_sock)) { ... }

      -- or --

      bool flag;
      Socket::KeepAlive.Get(my_sock, flag);

   You can also use these objects to dump the value of the option for your
   socket as a human-readable string:

      Socket::KeepAlive.Dump(cout, my_sock);

   Or you can see the values for all the options for your socket:

      Socket::DumpAllOptions(cout, my_sock);

      -- or --

      Socket::LogAllOptions(LOG_INFO, my_sock);
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <chrono>
#include <ostream>
#include <string>
#include <utility>

#include <net/if.h>
#include <sys/socket.h>
#include <sys/time.h>

#include <base/opt.h>
#include <base/no_construction.h>
#include <base/no_copy_semantics.h>
#include <base/error_utils.h>

namespace Socket {

  /**
   *  Wrappers for the OS functions to make them typesafe.
   **/

  /* A typesafe wrapper for getsockopt(), q.v.
     This version assumes that the argument is a fixed-size data structure,
     which is fine for everything but strings. */
  template <typename TArg>
  void GetSockOpt(int sock, int code, TArg &arg) {
    assert(&arg);
    socklen_t dummy = sizeof(arg);
    Base::IfLt0(getsockopt(sock, SOL_SOCKET, code, &arg, &dummy));
  }

  /* A typesafe wrapper for setsockopt(), q.v.
     This version assumes that the argument is a fixed-size data structure,
     which is fine for everything but strings. */
  template <typename TArg>
  void SetSockOpt(int sock, int code, const TArg &arg) {
    assert(&arg);
    Base::IfLt0(setsockopt(sock, SOL_SOCKET, code, &arg, sizeof(arg)));
  }

  /**
   *  Type definitions used for socket options.
   **/

  /* This is an intentionally incomplete type we use to specify explicit
     specializations.  It represents a string argument of the given maximum
     size.  The size includes the null-terminator. */
  template <size_t MaxSize>
  class Str;

  /* Used by SO_LINGER to represent the amount of time to linger.
     The unknown value here means that the lingering feature is turned off. */
  using TLinger = Base::TOpt<std::chrono::seconds>;

  /* Used by SO_RCVTIMEO to represent timeout durations. */
  using TTimeout = std::chrono::microseconds;

  /**
   *  Type converters for making the OS functions more C++-friendly.
   **/

  /* This is a look-up template used to convert between the low-level types
     expected by the OS and the types we would prefer to use in C++.  Explicit
     specializations exist for the following mappings:

        OS Type  C++ Type
        ~~~~~~~  ~~~~~~~~
        int      bool
        int      int
        linger   TLinger
        ucred    ucred
        timeval  TTimeout
        char *   Str<N>

    NOTE: There are times when the OS intends an int to be interpreted as a
    bool.  This is handled on a per-case basis in the instantiations of the
    option objects.

    Each specialization contains a definition of GetSockOpt() and SetSockOpt()
    which take the appropriate type.  The specialization for Str<MaxSize> is
    slightly different in that it its functions take std::string but limit the
    string to the requested size. */
  template <typename TSelector>
  class Conv;

  /* Explicit specialization for bool. */
  template <>
  class Conv<bool> final {
    NO_CONSTRUCTION(Conv);

    public:
    /* The type taken by GetSockOpt() and GetSockOpt(). */
    using TVal = bool;

    /* See forward declaration of generic Conv<>. */
    static void GetSockOpt(int sock, int code, TVal &val) {
      assert(&val);
      int temp;
      Socket::GetSockOpt(sock, code, temp);
      val = (temp != 0);
    }

    /* See forward declaration of generic Conv<>. */
    static void SetSockOpt(int sock, int code, TVal val) {
      int temp = val ? 1 : 0;
      Socket::SetSockOpt(sock, code, temp);
    }
  };  // Conv<bool>

  /* Explicit specialization for int. */
  template <>
  class Conv<int> final {
    NO_CONSTRUCTION(Conv);

    public:
    /* The type taken by GetSockOpt() and GetSockOpt(). */
    using TVal = int;

    /* See forward declaration of generic Conv<>. */
    static void GetSockOpt(int sock, int code, TVal &val) {
      Socket::GetSockOpt(sock, code, val);
    }

    /* See forward declaration of generic Conv<>. */
    static void SetSockOpt(int sock, int code, TVal val) {
      Socket::SetSockOpt(sock, code, val);
    }
  };  // Conv<int>

  /* Explicit specialization for TLinger. */
  template <>
  class Conv<TLinger> final {
    NO_CONSTRUCTION(Conv);

    public:
    /* The type taken by GetSockOpt() and GetSockOpt(). */
    using TVal = TLinger;

    /* See forward declaration of generic Conv<>. */
    static void GetSockOpt(int sock, int code, TVal &val) {
      assert(&val);
      linger temp;
      Socket::GetSockOpt(sock, code, temp);
      if (temp.l_onoff) {
        val = std::chrono::seconds(temp.l_linger);
      } else {
        val.Reset();
      }
    }

    /* See forward declaration of generic Conv<>. */
    static void SetSockOpt(int sock, int code, const TVal &val) {
      assert(&val);
      linger temp;
      temp.l_onoff = val;
      temp.l_linger = (temp.l_onoff ? val->count() : 0);
      Socket::SetSockOpt(sock, code, temp);
    }
  };  // Conv<TLinger>

  /* Explicit specialization for cred. */
  template <>
  class Conv<ucred> final {
    NO_CONSTRUCTION(Conv);

    public:
    /* The type taken by GetSockOpt() and GetSockOpt(). */
    using TVal = ucred;

    /* See forward declaration of generic Conv<>. */
    static void GetSockOpt(int sock, int code, TVal &val) {
      Socket::GetSockOpt(sock, code, val);
    }

    /* See forward declaration of generic Conv<>. */
    static void SetSockOpt(int sock, int code, const TVal &val) {
      Socket::SetSockOpt(sock, code, val);
    }
  };  // Conv<cred>

  /* Explicit specialization for TTimeout. */
  template <>
  class Conv<TTimeout> final {
    NO_CONSTRUCTION(Conv);

    public:
    /* The type taken by GetSockOpt() and GetSockOpt(). */
    using TVal = TTimeout;

    /* See forward declaration of generic Conv<>. */
    static void GetSockOpt(int sock, int code, TVal &val) {
      assert(&val);
      timeval temp;
      Socket::GetSockOpt(sock, code, temp);
      val = TTimeout(temp.tv_sec * 1000000 + temp.tv_usec);
    }

    /* See forward declaration of generic Conv<>. */
    static void SetSockOpt(int sock, int code, const TVal &val) {
      assert(&val);
      auto count = val.count();
      timeval temp;
      temp.tv_sec  = count / 1000000;
      temp.tv_usec = count % 1000000;
      Socket::SetSockOpt(sock, code, temp);
    }
  };  // Conv<TTimeout>

  /* Explicit specialization for Str<MaxSize>. */
  template <size_t MaxSize>
  class Conv<Str<MaxSize>> final {
    NO_CONSTRUCTION(Conv);

    public:
    /* The type taken by GetSockOpt() and GetSockOpt(). */
    using TVal = std::string;

    /* See forward declaration of generic Conv<>. */
    static void GetSockOpt(int sock, int code, TVal &val) {
      assert(&val);
      char temp[MaxSize];
      socklen_t size = MaxSize;
      Base::IfLt0(getsockopt(sock, SOL_SOCKET, code, temp, &size));
      val.assign(temp, size);
    }

    /* See forward declaration of generic Conv<>. */
    static void SetSockOpt(int sock, int code, const TVal &val) {
      assert(&val);
      Base::IfLt0(setsockopt(sock, SOL_SOCKET, code, val.c_str(), val.size()));
    }
  };  // Conv<TLinger>

  /**
   *  Pretty-printers for the types used by socket options.
   **/

  /* This is a look-up template used to dump socket option values to a stream.
     Explicit specializations exist for the following types:

        bool
        int
        TLinger
        ucred
        TTimeout
        std::string

    Each specialization contains a definition of Dump() which takes a stream
    and a value of the appropriate type. */
  template <typename TVal>
  class Format;

  /* Explicit specialization for bool. */
  template <>
  class Format<bool> final {
    NO_CONSTRUCTION(Format);

    public:
    /* See forward declaration of generic Format<>. */
    static void Dump(std::ostream &strm, bool val) {
      assert(&strm);
      strm << std::boolalpha << val;
    }
  };  // Format<bool>

  /* Explicit specialization for int. */
  template <>
  class Format<int> final {
    NO_CONSTRUCTION(Format);

    public:
    /* See forward declaration of generic Format<>. */
    static void Dump(std::ostream &strm, int val) {
      assert(&strm);
      strm << val;
    }
  };  // Format<int>

  /* Explicit specialization for TLinger. */
  template <>
  class Format<TLinger> final {
    NO_CONSTRUCTION(Format);

    public:
    /* See forward declaration of generic Format<>. */
    static void Dump(std::ostream &strm, const TLinger &val) {
      assert(&strm);
      assert(&val);
      if (val) {
        strm << val->count() << " sec(s)";
      } else {
        strm << "off";
      }
    }
  };  // Format<TLinger>

  /* Explicit specialization for cred. */
  template <>
  class Format<ucred> final {
    NO_CONSTRUCTION(Format);

    public:
    /* See forward declaration of generic Format<>. */
    static void Dump(std::ostream &strm, const ucred &val) {
      assert(&strm);
      assert(&val);
      strm << "{ pid: " << val.pid << ", uid: " << val.uid << ", gid: "
          << val.gid << " }";
    }
  };  // Format<cred>

  /* Explicit specialization for TTimeout. */
  template <>
  class Format<TTimeout> final {
    NO_CONSTRUCTION(Format);

    public:
    /* See forward declaration of generic Format<>. */
    static void Dump(std::ostream &strm, const TTimeout &val) {
      assert(&strm);
      assert(&val);
      strm << val.count() << " usec(s)";
    }
  };  // Format<TTimeout>

  /* Explicit specialization for std::string. */
  template <>
  class Format<std::string> final {
    NO_CONSTRUCTION(Format);

    public:
    /* See forward declaration of generic Format<>. */
    static void Dump(std::ostream &strm, const std::string &val) {
      assert(&strm);
      assert(&val);
      strm << '"' << val << '"';
    }
  };  // Format<TLinger>

  /**
   *  Classes used to represent the socket options themselves.
   **/

  /* The base class for all socket option objects. */
  class TAnyOption {
    public:
    /* Do-little. */
    virtual ~TAnyOption();

    /* The code number used by getsockopt() and setsockopt() when referring to
       this option. */
    int GetCode() const {
      assert(this);
      return Code;
    }

    /* The human-readable name of this option. */
    const std::string &GetName() const {
      assert(this);
      return Name;
    }

    /* Get the option's value from the given socket and write to the given
       stream in a human-readable form, like this: "<option name>: <val>". */
    virtual void Dump(std::ostream &strm, int sock) const = 0;

    protected:
    /* Do-little. */
    template <typename TName>
    TAnyOption(TName &&name, int code)
        : Name(std::forward<TName>(name)), Code(code) {
    }

    private:
    /* See accessor. */
    const std::string Name;

    /* See accessor. */
    const int Code;
  };  // TAnyOption

  /* The base class for read-only and read-write socket options of a particular
     type.  The selector argument is used to pick which Conv<> overload to use
     when getting and setting the option.  The Conv<> overload, in turn, picks
     the C++ type used by the option. */
  template <typename TSelector>
  class TOption : public TAnyOption {
    public:
    /* The type taken by our operator() overloads.*/
    using TVal = typename Conv<TSelector>::TVal;

    /* The option's value for the given socket, returned by value. */
    TVal Get(int sock) const {
      assert(this);
      TVal val;
      Get(sock, val);
      return val;
    }

    /* The option's value for the given socket, returned via out-parameter. */
    void Get(int sock, TVal &val) const {
      assert(this);
      Conv<TSelector>::GetSockOpt(sock, TAnyOption::GetCode(), val);
    }

    /* See base class. */
    virtual void Dump(std::ostream &strm, int sock) const override final {
      assert(this);
      assert(&strm);
      strm << TAnyOption::GetName() << ": ";
      TVal val;
      Get(sock, val);
      Format<TVal>::Dump(strm, val);
    }

    protected:
    /* Do-little. */
    template <typename TName>
    TOption(TName &&name, int code)
        : TAnyOption(std::forward<TName>(name), code) {
    }
  };  // TOption<TSelector>

  /* A read-only socket option of a particular type. */
  template <typename TSelector>
  class TRoOption final : public TOption<TSelector> {
    public:
    /* Do-little. */
    template <typename TName>
    TRoOption(TName &&name, int code)
        : TOption<TSelector>(std::forward<TName>(name), code) {
    }
  };  // TRoOption<TSelector>

  /* A read-write socket option of a particular type. */
  template <typename TSelector>
  class TRwOption final : public TOption<TSelector> {
    public:
    /* The type taken by our operator() overload.*/
    using TVal = typename Conv<TSelector>::TVal;

    /* Do-little. */
    template <typename TName>
    TRwOption(TName &&name, int code)
        : TOption<TSelector>(std::forward<TName>(name), code) {
    }

    /* Set the option's value for the given socket. */
    void Set(int sock, const TVal &val) const {
      Conv<TSelector>::SetSockOpt(sock, TAnyOption::GetCode(), val);
    }
  };  // TRwOption<TSelector>

  /**
   *  All the standard socket options, taken collectively.
   **/

  /* Pointers to all the currently defined socket options, terminated by a null
     pointer.
     NOTE: BindToDevice, RcvBufForce, and SndBufForce are omitted from this
     collection because they require root privilege, and PeerCred is omitted
     from this collection because it is only defined for AF_UNIX sockets. */
  extern const TAnyOption *AllOptions[];

  /* For each option in AllOptions, get the option's value from the given
     socket and write to the given stream in a human-readable form.  The result
     looks like this: "{ <option name>: <val>, <option name>: <val>, ... }". */
  void DumpAllOptions(std::ostream &strm, int sock);

  /* Just like DumpAllOptions(), but the output is sent to the system logger at
     the given logging level. */
  void LogAllOptions(int log_level, int sock);

  /**
   *  The standard socket options.  These are drawn from the socket(7) man
   *  page, q.v, and appear here in the same order as they do there.
   **/

  /* Returns a value indicating whether or not this socket has been marked to
     accept connections with listen(2). */
  extern const TRoOption<bool> AcceptConn;

  /* Bind this socket to a particular device like "eth0", as specified in the
     passed interface name. If the name is an empty string or the option length
     is zero, the socket device binding is removed. The passed option is a
     variable-length null-terminated interface name string with the maximum
     size of IFNAMSIZ. If a socket is bound to an interface, only packets
     received from that particular interface are processed by the socket. Note
     that this only works for some socket types, particularly AF_INET sockets.
     It is not supported for packet sockets (use normal bind(2) there). */
  extern const TRwOption<Str<IFNAMSIZ>> BindToDevice;

  /* Set or get the broadcast flag. When enabled, datagram sockets receive
     packets sent to a broadcast address and they are allowed to send packets
     to a broadcast address. This option has no effect on stream-oriented
     sockets. */
  extern const TRwOption<bool> Broadcast;

  /* Enable BSD bug-to-bug compatibility. This is used by the UDP protocol
     module in Linux 2.0 and 2.2. If enabled ICMP errors received for a UDP
     socket will not be passed to the user program. In later kernel versions,
     support for this option has been phased out: Linux 2.4 silently ignores
     it, and Linux 2.6 generates a kernel warning (printk()) if a program uses
     this option. Linux 2.0 also enabled BSD bug-to-bug compatibility options
     (random header changing, skipping of the broadcast flag) for raw sockets
     with this option, but that was removed in Linux 2.2. */
  extern const TRwOption<bool> BsdCompat;

  /* Enable socket debugging. Only allowed for processes with the CAP_NET_ADMIN
     capability or an effective user ID of 0. */
  extern const TRwOption<bool> Debug;

  /* Retrieves the socket domain as an integer, returning a value such as
     AF_INET6. See socket(2) for details. */
  extern const TRoOption<int> Domain;

  /* Get and clear the pending socket error. */
  extern const TRoOption<int> Error;

  /* Don't send via a gateway, only send to directly connected hosts. The same
     effect can be achieved by setting the MSG_DONTROUTE flag on a socket
     send(2) operation. Expects an integer boolean flag. */
  extern const TRwOption<bool> DontRoute;

  /* Enable sending of keep-alive messages on connection-oriented sockets. */
  extern const TRwOption<bool> KeepAlive;

  /* When enabled, a close(2) or shutdown(2) will not return until all queued
     messages for the socket have been successfully sent or the linger timeout
     has been reached. Otherwise, the call returns immediately and the closing
     is done in the background. When the socket is closed as part of exit(2),
     it always lingers in the background. */
  extern const TRwOption<TLinger> Linger;

  /* If this option is enabled, out-of-band data is directly placed into the
     receive data stream. Otherwise out-of-band data is only passed when the
     MSG_OOB flag is set during receiving. */
  extern const TRwOption<bool> OobInline;

  /* Enable or disable the receiving of the SCM_CREDENTIALS control message.
     For more information see unix(7). */
  extern const TRwOption<bool> PassCred;

  /* Return the credentials of the foreign process connected to this socket.
     This is only possible for connected AF_UNIX stream sockets and AF_UNIX
     stream and datagram socket pairs created using socketpair(2); see unix(7).
     The returned credentials are those that were in effect at the time of the
     call to connect(2) or socketpair(2).  Argument is a ucred structure. */
  extern const TRoOption<ucred> PeerCred;

  /* Set the protocol-defined priority for all packets to be sent on this
     socket. Linux uses this value to order the networking queues: packets with
     a higher priority may be processed first depending on the selected device
     queueing discipline. For ip(7), this also sets the IP type-of-service
     (TOS) field for outgoing packets. Setting a priority outside the range 0
     to 6 requires the CAP_NET_ADMIN capability. */
  extern const TRwOption<int> Priority;

  /* Retrieves the socket protocol as an integer, returning a value such as
     IPPROTO_SCTP. See socket(2) for details. */
  extern const TRoOption<int> Protocol;

  /* Sets or gets the maximum socket receive buffer in bytes. The kernel
     doubles this value (to allow space for bookkeeping overhead) when it is
     set using setsockopt(2), and this doubled value is returned by
     getsockopt(2). The default value is set by the
     /proc/sys/net/core/rmem_default file, and the maximum allowed value is set
     by the /proc/sys/net/core/rmem_max file. The minimum (doubled) value for
     this option is 256. */
  extern const TRwOption<int> RcvBuf;

  /* Using this socket option, a privileged (CAP_NET_ADMIN) process can perform
     the same task as SO_RCVBUF, but the rmem_max limit can be overridden. */
  extern const TRwOption<int> RcvBufForce;

  /* Specify the minimum number of bytes in the buffer until the socket layer
     will pass the data to the protocol (SO_SNDLOWAT) or the user on receiving
     (SO_RCVLOWAT). These two values are initialized to 1. SO_SNDLOWAT is not
     changeable on Linux (setsockopt(2) fails with the error ENOPROTOOPT).
     SO_RCVLOWAT is changeable only since Linux 2.4. The select(2) and poll(2)
     system calls currently do not respect the SO_RCVLOWAT setting on Linux,
     and mark a socket readable when even a single byte of data is available. A
     subsequent read from the socket will block until SO_RCVLOWAT bytes are
     available. */
  extern const TRwOption<int> RcvLowAt;
  extern const TRwOption<int> SndLowAt;

  /* Specify the receiving or sending timeouts until reporting an error. The
     argument is a struct timeval. If an input or output function blocks for
     this period of time, and data has been sent or received, the return value
     of that function will be the amount of data transferred; if no data has
     been transferred and the timeout has been reached then -1 is returned with
     errno set to EAGAIN or EWOULDBLOCK just as if the socket was specified to
     be nonblocking. If the timeout is set to zero (the default) then the
     operation will never timeout. Timeouts only have effect for system calls
     that perform socket I/O (e.g., read(2), recvmsg(2), send(2), sendmsg(2));
     timeouts have no effect for select(2), poll(2), epoll_wait(2), etc. */
  extern const TRwOption<TTimeout> RcvTimeo;
  extern const TRwOption<TTimeout> SndTimeo;

  /* Indicates that the rules used in validating addresses supplied in a
     bind(2) call should allow reuse of local addresses.  For AF_INET sockets
     this means that a socket may bind, except when there is an active
     listening socket bound to the address.  When the listening socket is bound
     to INADDR_ANY with a specific port then it is not possible to bind to this
     port for any local address. */
  extern const TRwOption<bool> ReuseAddr;

  /* Sets or gets the maximum socket send buffer in bytes. The kernel doubles
     this value (to allow space for bookkeeping overhead) when it is set using
     setsockopt(2), and this doubled value is returned by getsockopt(2). The
     default value is set by the /proc/sys/net/core/wmem_default file and the
     maximum allowed value is set by the /proc/sys/net/core/wmem_max file. The
     minimum (doubled) value for this option is 2048. */
  extern const TRwOption<int> SndBuf;

  /* Using this socket option, a privileged (CAP_NET_ADMIN) process can perform
     the same task as SO_SNDBUF, but the wmem_max limit can be overridden. */
  extern const TRwOption<int> SndBufForce;

  /* Enable or disable the receiving of the SO_TIMESTAMP control message. The
     timestamp control message is sent with level SOL_SOCKET and the cmsg_data
     field is a struct timeval indicating the reception time of the last packet
     passed to the user in this call.  See cmsg(3) for details on control
     messages. */
  extern const TRwOption<bool> TimeStamp;

  /* Gets the socket type as an integer (e.g., SOCK_STREAM). */
  extern const TRoOption<int> Type;
}  // Socket
