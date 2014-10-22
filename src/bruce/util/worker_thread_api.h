/* <bruce/util/worker_thread_api.h>

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

   API definition for worker thread abstraction for Bruce daemon.
 */

#pragma once

#include <exception>
#include <stdexcept>
#include <string>

#include <base/event_semaphore.h>
#include <base/fd.h>
#include <base/no_copy_semantics.h>
#include <base/thrower.h>

namespace Bruce {

  namespace Util {

    /* Abstract class defining API for worker thread mechanism. */
    class TWorkerThreadApi {
      NO_COPY_SEMANTICS(TWorkerThreadApi);

      public:
      DEFINE_ERROR(TThreadAlreadyStarted, std::runtime_error,
                   "Worker thread is already started");

      DEFINE_ERROR(TThreadAlreadyShutDown, std::runtime_error,
                   "Cannot request shutdown on nonexistent worker thread");

      DEFINE_ERROR(TThreadThrewUnknownException, std::runtime_error,
                   "Worker thread threw unknown exception");

      DEFINE_ERROR(TCannotJoinNonexistentThread, std::runtime_error,
                   "Cannot join nonexistent worker thread");

      class TThreadThrewStdException final : public std::runtime_error {
        public:
        TThreadThrewStdException(const char *what_msg)
            : std::runtime_error(MakeWhatMsg(what_msg)) {
        }

        TThreadThrewStdException(const TThreadThrewStdException &) = default;

        virtual ~TThreadThrewStdException() noexcept { }

        TThreadThrewStdException &
        operator=(const TThreadThrewStdException &) = default;

        private:
        static std::string MakeWhatMsg(const char *msg);
      };  // TThreadThrewStdException

      virtual ~TWorkerThreadApi() noexcept { }

      virtual void Start() = 0;

      virtual bool IsStarted() const = 0;

      virtual void RequestShutdown() = 0;

      virtual const Base::TFd &GetShutdownWaitFd() const = 0;

      virtual void Join() = 0;

      protected:
      TWorkerThreadApi() = default;
    };  // TWorkerThreadApi

  }  // Util

}  // Bruce
