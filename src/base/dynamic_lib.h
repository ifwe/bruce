/* <base/dynamic_lib.h>

   ----------------------------------------------------------------------------
   Copyright 2014 if(we)

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

   Dynamic libaray class.
 */

#include <cassert>
#include <stdexcept>
#include <string>

#include <dlfcn.h>

#include <base/no_copy_semantics.h>

namespace Base {

  /* Dynamic library class.  This acts as a thin wrapper around dlopen(),
     dlsym(), and dlclose(), and is used as a base class.  To create a wrapper
     for a specific library, derive from this class. */
  class TDynamicLib {
    NO_COPY_SEMANTICS(TDynamicLib);

    public:
    /* Exception base class. */
    class TErrorBase : public std::runtime_error {
      public:
      virtual const char *what() const noexcept {
        assert(this);
        return Msg.c_str();
      }

      protected:
      explicit TErrorBase(std::string &&msg)
          : std::runtime_error(msg.c_str()),
            Msg(std::move(msg)) {
      }

      private:
      std::string Msg;
    };  // TErrorBase

    /* Thrown when dlopen() fails. */
    class TLibLoadError : public TErrorBase {
      public:
      explicit TLibLoadError(const char *libname)
          : TErrorBase(CreateMsg(libname)) {
      }

      private:
      static std::string CreateMsg(const char *libname);
    };  // TLibLoadError

    /* Thrown when dlsym() fails. */
    class TSymLoadError : public TErrorBase {
      public:
      explicit TSymLoadError(const char *libname, const char *symname)
          : TErrorBase(CreateMsg(libname, symname)) {
      }

      private:
      static std::string CreateMsg(const char *libname, const char *symname);
    };  // TSymLoadError

    virtual ~TDynamicLib() noexcept {
      int ret = dlclose(Handle);
      assert(ret == 0);
    }

    protected:
    /* Constructor parameters are passed directly to dlopen().  Throws
       TLibLoadError if dlopen() fails. */
    TDynamicLib(const char *libname, int flags);

    /* Attempt to load the given symbol.  Return the loaded symbol on success
       or nullptr on failure.  Template parameter 'T' specifies a pointer type
       to cast the returned symbol to.  If the symbol represents a function,
       you will want to specify 'T' as a function pointer type that matches the
       function signature. */
    template <typename T>
    T LoadSymNoexcept(const char *symname) noexcept {
      assert(this);
      return reinterpret_cast<T>(dlsym(Handle, symname));
    }

    /* Same as LoadSymNoexcept(), but throws TSymLoadError on dlsym() failure.
     */
    template <typename T>
    T LoadSym(const char *symname) {
      assert(this);
      T sym = reinterpret_cast<T>(dlsym(Handle, symname));

      if (sym == nullptr) {
        throw TSymLoadError(LibName.c_str(), symname);
      }

      return sym;
    }

    private:
    /* Library name passed to dlopen(). */
    std::string LibName;

    /* Handle returned by dlopen(). */
    void *Handle;
  };  //  TDynamicLib

}  // Base
