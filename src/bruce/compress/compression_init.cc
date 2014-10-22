/* <bruce/compress/compression_init.cc>

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

   Implements <bruce/compress/compression_init.h>.
 */

#include <bruce/compress/compression_init.h>

#include <set>

#include <bruce/compress/get_compression_codec.h>
#include <bruce/compress/snappy/snappy_codec.h>
#include <bruce/conf/compression_type.h>

using namespace Bruce;
using namespace Bruce::Compress;
using namespace Bruce::Compress::Snappy;
using namespace Bruce::Conf;

void Bruce::Compress::CompressionInit(const TCompressionConf &conf) {
  std::set<TCompressionType> all_types;
  all_types.insert(conf.GetDefaultTopicConfig().Type);
  const TCompressionConf::TTopicMap &topic_map = conf.GetTopicConfigs();

  for (const auto &item : topic_map) {
    all_types.insert(item.second.Type);
  }

  /* For each needed compression type, force the associated compression library
     to load.  This will throw if there is an error loading a library. */
  for (TCompressionType type : all_types) {
    GetCompressionCodec(type);
  }
}

void Bruce::Compress::CompressionInit() {
  /* Force all supported compression libraries to load.  This will throw if
     there is an error loading a library. */
  TSnappyCodec::The();
}
