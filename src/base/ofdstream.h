/* <base/ofdstream.h>

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

   Convenience class that facilitates using file descriptors with iostreams.
 */

#include <ostream>

#include <ext/stdio_filebuf.h>

namespace Base {

  /* Warning: This is somewhat of a hack, and depends on nonportable
     gcc-specific functionality.  It can probably be made much better.

     Intended usage:

       int fd = <some file descriptor that can be written to>;
       TOFdStream fds(fd);
       fds.Get() << "hello world" << std::endl;

     Note: The TOFdStream object takes ownership of the file descriptor passed
     in, and will close the file descriptor on destruction.
   */
  class TOFdStream {

    public:

    explicit TOFdStream(int fd)
        : Filebuf(fd, std::ios::out),
          Os(&Filebuf) {
    }

    std::ostream &Get() {
      return Os;
    }

    private:
    __gnu_cxx::stdio_filebuf<char> Filebuf;

    std::ostream Os;
  };  // TOFdStream

}  // Base
