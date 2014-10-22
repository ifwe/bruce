/* <io/output_producer.cc>

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

   Implements <io/output_producer.h>.
 */

#include <io/output_producer.h>

#include <syslog.h>

using namespace std;
using namespace Io;

TOutputProducer::~TOutputProducer() {
  assert(this);

  try {
    Flush();
  } catch (const exception &ex) {
    syslog(LOG_INFO, "discarding unsent data; %s", ex.what());
  } catch (...) {
    syslog(LOG_INFO, "discarding unsent data due to non-standard exception");
  }
}

void TOutputProducer::Flush() {
  assert(this);

  if (CurrentChunk) {
    if (OutputConsumer) {
      OutputConsumer->ConsumeOutput(CurrentChunk);
    }

    CurrentChunk.reset();
  }
}

void TOutputProducer::WriteExactly(const void *buf, size_t size) {
  assert(this);
  const char *csr = static_cast<const char *>(buf);

  while (size) {
    if (!CurrentChunk) {
      CurrentChunk = Pool->AcquireChunk();
    }

    if (!CurrentChunk->Store(csr, size)) {
      Flush();
    }
  }
}
