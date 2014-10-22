/* <bruce/mock_kafka_server/port_map.cc>

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

   Implements <bruce/mock_kafka_server/port_map.h>.
 */

#include <bruce/mock_kafka_server/port_map.h>

#include <cassert>
#include <stdexcept>

using namespace Bruce;
using namespace Bruce::MockKafkaServer;

void TPortMap::AddMapping(in_port_t virtual_port, in_port_t physical_port) {
  assert(this);
  auto result1 = VToPMap.insert(std::make_pair(virtual_port, physical_port));

  if (!result1.second) {
    throw std::logic_error("virtual to physical port mapping already present");
  }

  auto result2 = PToVMap.insert(std::make_pair(physical_port, virtual_port));

  if (!result2.second) {
    throw std::logic_error("physical to virtual port mapping already present");
  }
}

in_port_t TPortMap::VirtualPortToPhys(in_port_t v_port) const {
  assert(this);
  auto iter = VToPMap.find(v_port);
  return (iter == VToPMap.end()) ? 0 : iter->second;
}

in_port_t TPortMap::PhysicalPortToVirt(in_port_t p_port) const {
  assert(this);
  auto iter = PToVMap.find(p_port);
  return (iter == PToVMap.end()) ? 0 : iter->second;
}
