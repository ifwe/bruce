/* <rpc/transceiver.h>

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

   Provides scatter-reads and gather-writes over a socket.

   The transceiver object manages the vector array used in scatter/gather I/O.
   Call GetIoVecs() to get an array of sufficient size, then fill in the
   elements of the array to point to your data.  Then call Send() or Recv()
   repeatedly until the transfer is complete.

   Here's an example:

      TTranceiver xver;
      auto *vecs = xver.GetIoVecs(2);
      vecs[0].iov_base = "Hello, ";
      vecs[0].iov_len  = 7;
      vecs[1].iov_base = "world!";
      vecs[1].iov_len  = 6;

      for (size_t part = 0; xver; xver += part) {
        part = xver.Send(sock_fd);
      }
 */

#pragma once

#include <cstddef>
#include <stdexcept>

#include <sys/socket.h>

#include <base/no_copy_semantics.h>

namespace Rpc {

  /* Provides scatter-reads and gather-writes over a socket. */
  class TTransceiver final {
    NO_COPY_SEMANTICS(TTransceiver);

    public:
    /* Thrown when a Send() or Recv() fails because our peer has hung up. */
    class TDisconnected : public std::runtime_error {
      public:
      /* Do-little. */
      TDisconnected();
    };  // TTransceiver::TDisconnected

    /* Thrown by operator+= when advancing beyond the end of data. */
    class TPastEnd : public std::logic_error {
      public:
      /* Do-little. */
      TPastEnd();
    };  // TTransceiver::TPastEnd

    /* Start with no data to transfer. */
    TTransceiver();

    /* Frees our array of iovecs. */
    ~TTransceiver();

    /* True iff. we have data to transfer; that is, when we have at least one
       iovec with a non-empty buffer. */
    operator bool() const noexcept;

    /* Update our iovecs to skip past the given number of bytes.
       Return true iff. there still data left in the msghdr afterward. */
    TTransceiver &operator+=(size_t size);

    /* Return a pointer to the start of an array of iovecs containing as many
       elements as requested.  The array is owned by the tranceiver object, so
       don't delete it.  The contents of the array are maintained even if you
       call this function multiple times, so you can expand the array if you
       need to. */
    iovec *GetIoVecs(size_t size);

    /* Scatter-read into the buffers currently in the iovec array.  The fd must
       be an open socket.  Flags are as for recvmsg().  If there is an I/O
       error, or if our peer hangs up, we throw. */
    size_t Recv(int sock_fd, int flags = 0);

    /* Gather-write out of the buffers currently in the iovec array.  The fd
       must be an open socket.  Flags are as for recvmsg().  If there is an I/O
       error, or if our peer hangs up, we throw.
       NOTE: This function always sets the MSG_NOSIGNAL. */
    size_t Send(int sock_fd, int flags = 0);

    protected:
    /* Initialze a msghdr structure to point at our array of initialized
       iovecs. */
    void InitHdr(msghdr &hdr) const noexcept;

    /* Takes the code returned by sendmsg() or recvmsg() and returns the actual
       number of bytes transferred.  If the result indicates an error, or if no
       bytes were transferred, this function will throw appropriately. */
    static size_t GetActualIoSize(ssize_t io_result);

    /* The start and limit of our array of iovecs, or nulls if we haven't
       allocated the array yet.  Always true: AvailStart <= AvailLimit. */
    iovec *AvailStart, *AvailLimit;

    /* The start and limit of the portion of the iovec array above which we
       have yet to transfer via I/O.
       Always true: AvailStart <= DataStart <= DataLimit <= AvailLimit. */
    iovec *DataStart, *DataLimit;
  };  // TTransceiver

}  // Rpc
