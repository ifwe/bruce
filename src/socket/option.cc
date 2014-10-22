/* <socket/option.cc>

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

   Implements <socket/option.h>.
 */

#include <socket/option.h>

#include <sstream>

#include <syslog.h>

using namespace std;
using namespace Socket;

TAnyOption::~TAnyOption() {
}

const TAnyOption *Socket::AllOptions[] = {
  &AcceptConn,
  &Broadcast,
  &BsdCompat,
  &Debug,
  &Domain,
  &Error,
  &DontRoute,
  &KeepAlive,
  &Linger,
  &OobInline,
  &PassCred,
  &Priority,
  &Protocol,
  &RcvBuf,
  &RcvLowAt,
  &SndLowAt,
  &RcvTimeo,
  &SndTimeo,
  &ReuseAddr,
  &SndBuf,
  &TimeStamp,
  &Type,
  nullptr
};

void Socket::DumpAllOptions(ostream &strm, int sock) {
  assert(&strm);
  strm << "socket_ops: {";
  bool has_written = false;

  for (const TAnyOption *const *csr = AllOptions; *csr; ++csr) {
    strm << (has_written ? ", " : " ");
    (*csr)->Dump(strm, sock);
    has_written = true;
  }

  strm << (has_written ? " }" : "}");
}

void Socket::LogAllOptions(int log_level, int sock) {
  ostringstream strm;
  DumpAllOptions(strm, sock);
  syslog(log_level, "%s", strm.str().c_str());
}

const TRoOption<bool> Socket::AcceptConn("accept_conn",SO_ACCEPTCONN);

const TRwOption<Str<IFNAMSIZ>>
Socket::BindToDevice("bind_to_device", SO_BINDTODEVICE);

const TRwOption<bool> Socket::Broadcast("broadcast", SO_BROADCAST);

const TRwOption<bool> Socket::BsdCompat("bsd_compat", SO_BSDCOMPAT);

const TRwOption<bool> Socket::Debug("debug", SO_DEBUG);

const TRoOption<int> Socket::Domain("domain", SO_DOMAIN);

const TRoOption<int> Socket::Error("error", SO_ERROR);

const TRwOption<bool> Socket::DontRoute("dont_route", SO_DONTROUTE);

const TRwOption<bool> Socket::KeepAlive("keep_alive", SO_KEEPALIVE);

const TRwOption<TLinger> Socket::Linger("linger", SO_LINGER);

const TRwOption<bool> Socket::OobInline("oob_inline", SO_OOBINLINE);

const TRwOption<bool> Socket::PassCred("pass_cred", SO_PASSCRED);

const TRoOption<ucred> Socket::PeerCred("peer_cred", SO_PEERCRED);

const TRwOption<int> Socket::Priority("priority", SO_PRIORITY);

const TRoOption<int> Socket::Protocol("protocol", SO_PROTOCOL);

const TRwOption<int> Socket::RcvBuf("rcv_buf", SO_RCVBUF);

const TRwOption<int> Socket::RcvBufForce("rcv_buf_force", SO_RCVBUFFORCE);

const TRwOption<int> Socket::RcvLowAt("rcv_low_at", SO_RCVLOWAT);

const TRwOption<int> Socket::SndLowAt("snd_low_at", SO_SNDLOWAT);

const TRwOption<TTimeout> Socket::RcvTimeo("rcv_timeo", SO_RCVTIMEO);

const TRwOption<TTimeout> Socket::SndTimeo("snd_timeo", SO_SNDTIMEO);

const TRwOption<bool> Socket::ReuseAddr("reuse_addr", SO_REUSEADDR);

const TRwOption<int> Socket::SndBuf("snd_buf", SO_SNDBUF);

const TRwOption<int> Socket::SndBufForce("snd_buf_force", SO_SNDBUFFORCE);

const TRwOption<bool> Socket::TimeStamp("time_stamp", SO_TIMESTAMP);

const TRoOption<int> Socket::Type("type", SO_TYPE);
