/* <bruce/mock_kafka_server/port_map.h>

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

   Mock Kafka server class for keeping track of mappings between virtual and
   physical ports.
 */

#pragma once

#include <map>

#include <netinet/in.h>

namespace Bruce {

  namespace MockKafkaServer {

    /* This is somewhat of a quick and dirty hack.  When initially developed,
       the mock Kafka server was just a standalone executable that simulated
       multiple Kafka brokers by listening on a consecutive range of ports,
       where the setup file specifies the first port in the range and the
       number of ports.  Each port is a simulated broker.

       Then support was added for running the mock Kafka server as a bunch of
       threads as part of a unit test process.  With this change, there is some
       benefit in having the server bind() to ephemeral ports, so unit tests
       will not fail due to ports already being in use.  To avoid having to
       rework a bunch of code based on the assumption that the ports are always
       consecutive, I introduced the notion of virtual vs. physical ports.

       The setup file and most of the server logic deals with virtual ports,
       which are always consecutive.  Each simulated broker is then represented
       by a virtual port.  When bind() is called for a broker with virtual port
       p1, a port of 0 is specified so the kernel chooses an ephemeral port p2.
       Then the server calls getsockname() to find the value of p2 (the
       physical port), and stores a mapping between p1 and p2.  bruce only
       deals with physical ports, so the server sends physical ports in its
       metadata responses.  The server binds to ephemeral ports only when
       running as part of a unit test.  Therefore, when running in standalone
       executable mode, the physical ports are always equal to the virtual
       ports. */
    class TPortMap final {

      public:
      /* Store the correspondence between the given virtual and physical ports.
       */
      void AddMapping(in_port_t virtual_port, in_port_t physical_port);

      /* Return the physical port corresponding to the given virtual port.  A
         return value of 0 means "mapping not found". */
      in_port_t VirtualPortToPhys(in_port_t v_port) const;

      /* Return the virtual port corresponding to the given physical port.  A
         return value of 0 means "mapping not found". */
      in_port_t PhysicalPortToVirt(in_port_t p_port) const;

      private:
      /* Keys are virtual ports and values are physical ports. */
      std::map<in_port_t, in_port_t> VToPMap;

      /* Keys are physical ports and values are virtual ports. */
      std::map<in_port_t, in_port_t> PToVMap;
    };  // TPortMap

  }  // MockKafkaServer

}  // Bruce
