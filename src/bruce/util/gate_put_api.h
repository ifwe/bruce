/* <bruce/util/gate_put_api.h>

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

   "Put" API for TGate mechanism.
 */

#pragma once

#include <list>

#include <base/no_copy_semantics.h>

namespace Bruce {

  namespace Util {

    template <typename TMsgType>
    class TGatePutApi {
      NO_COPY_SEMANTICS(TGatePutApi);

      public:
      TGatePutApi() = default;

      virtual ~TGatePutApi() noexcept { }

      virtual void Put(std::list<TMsgType> &&put_list) = 0;

      virtual void Put(TMsgType &&put_item) = 0;
    };  // TGatePutApi

  }  // Util

}  // Bruce
