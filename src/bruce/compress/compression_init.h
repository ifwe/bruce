/* <bruce/compress/compression_init.h>

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

   Compression initialization code.
 */

#pragma once

#include <bruce/conf/compression_conf.h>

namespace Bruce {

  namespace Compress {

    /* Load all compression libraries that are needed, according to information
       from the config file.  This should be called before starting Bruce.  The
       idea is to fail early in the case where a compression library fails to
       load. */
    void CompressionInit(const Conf::TCompressionConf &conf);

    /* Same as above, but don't be selective about which libraries we load.
       The mock Kafka server uses this, since it must be prepared to handle all
       supported compression types. */
    void CompressionInit();

  }  // Compress

}  // Bruce
