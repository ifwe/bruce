/* <base/dir_iter.h>

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

   An iterator over the contents of a directory.

   This iterator always skips the "." and ".." directory entries.

   The entries are in the order in which they appear in the directory stucture,
   which isn't something you should rely on.

   See <base/path_utils.test.cc> for tests of this unit.
 */

#pragma once

#include <cassert>

#include <dirent.h>

#include <base/no_copy_semantics.h>
#include <base/thrower.h>

namespace Base {

  /* An iterator over the contents of a directory. */
  class TDirIter final {
    NO_COPY_SEMANTICS(TDirIter);
    public:

    /* What kind of directory entry have we? */
    enum TKind {

      /* This is a block device. */
      BlockDev = DT_BLK,

      /* This is a character device. */
      CharDev = DT_CHR,

      /* This is a directory. */
      Dir = DT_DIR,

      /* This is a normal file. */
      File = DT_REG,

      /* This is a named pipe. */
      NamedPipe = DT_FIFO,

      /* This is a UNIX domain socket. */
      Socket = DT_SOCK,

      /* This is a symbolic link. */
      SymLink = DT_LNK,

      /* We have no idea what kind of entry this or, or the file system
         doesn't support the concept of kinds of entries. */
      Unknown = DT_UNKNOWN
    };  // TDirIter::TKind

    /* Thrown by Refresh() when we try to go past the end of the directory. */
    DEFINE_ERROR(TPastEnd, std::logic_error, "past end of directory");

    /* Starts at the beginning of the given directory. */
    explicit TDirIter(const char *dir);

    /* Cleans up our OS stuff. */
    ~TDirIter();

    /* True until we reach then end of the directory. */
    operator bool() const {
      assert(this);
      return TryRefresh();
    }

    /* Advance to the next entry in the directory.
       Calling this function when we've already reached the end of the
       directory is a logic error. */
    TDirIter &operator++() {
      assert(this);
      Refresh();
      Pos = NotFresh;
      return *this;
    }

    /* The kind of the current directory entry, which can be unknown for some
       file systems.
       Calling this function when we've already reached the end of the
       directory is a logic error. */
    TKind GetKind() const {
      assert(this);
      Refresh();
      return static_cast<TKind>(DirEnt.d_type);
    }

    /* The name of the current directory entry.  Never null.
       Calling this function when we've already reached the end of the
       directory is a logic error. */
    const char *GetName() const {
      assert(this);
      Refresh();
      return DirEnt.d_name;
    }

    /* Go back to the start of the directory. */
    void Rewind();

    private:
    /* What we know about our current position within the directory we're
       iterating. */
    enum TPos {
      /* We don't know our position. */
      NotFresh,

      /* We know we're positioned at a valid directory entry. */
      AtEntry,

      /* We know we're positioned at the end of the directory, where there are
         no more entries. */
      AtEnd
    };  // TDirIter::TPos

    /* Like TryRefresh(), but we throw if we're at the end. */
    void Refresh() const {
      assert(this);

      if (!TryRefresh()) {
        THROW_ERROR(TPastEnd);
      }
    }

    /* If Pos says we're already positioned at directory entry, do nothing and
       return true.
       If not, try to position to the next entry, update Pos, and return
       success/failure. */
    bool TryRefresh() const;

    /* The return of opendir().  Never null. */
    DIR *Handle;

    /* See TPos, above. */
    mutable TPos Pos;

    /* Our current entry.  Only valid when Pos == AtEntry. */
    mutable dirent DirEnt;
  };  // TDirIter

}  // Base
