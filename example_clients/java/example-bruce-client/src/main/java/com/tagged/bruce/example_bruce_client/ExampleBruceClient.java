/* ----------------------------------------------------------------------------
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

    This is an example Java program that sends messages to Bruce.
 */

package com.tagged.bruce.example_bruce_client;

import java.io.IOException;
import java.nio.charset.Charset;

import com.etsy.net.JUDS;
import com.etsy.net.UnixDomainSocketClient;

/**
 * Example Bruce client.
 *
 */
public class ExampleBruceClient 
{
    public static void main( String[] args )
    {
        DatagramCreator dgc = new DatagramCreator();
        String brucePath = "/path/to/bruce/socket";
        String topic = "some topic";  // Kafka topic
        long timestamp = 12345;  // should be milliseconds since the epoch
        String value = "hello world";
        byte[] valueBytes = value.getBytes(Charset.forName("UTF-8"));
        byte[] datagram1 = null, datagram2 = null;

        try {
            // create AnyPartition message
            datagram1 = dgc.createAnyPartitionDatagram(topic, timestamp, null,
                    valueBytes);

            // create PartitionKey message
            datagram2 = dgc.createPartitionKeyDatagram(1, topic, timestamp,
                    null, valueBytes);
        } catch (DatagramCreator.TopicTooLong x) {
            System.err.println(
                    "Failed to create datagram because topic is too long");
            System.exit(1);
        } catch (DatagramCreator.DatagramTooLarge x) {
            System.err.println("Failed to create datagram because its size " +
                    "would exceed the maximum");
            System.exit(1);
        }

        UnixDomainSocketClient unixSocket = null;

        /* Here we use a third party library that uses JNI to write to Bruce's
           UNIX domain datagram socket.  See the following links:

               https://github.com/caprica/juds
               http://mvnrepository.com/artifact/uk.co.caprica/juds/0.94.1

           Alternatively, you can write your own JNI code that uses Bruce's
           client C library. */
        try {
            // create socket for sending to Bruce
            unixSocket = new UnixDomainSocketClient(brucePath,
                    JUDS.SOCK_DGRAM);

            // send AnyPartition message to Bruce
            unixSocket.getOutputStream().write(datagram1);

            // send PartitionKey message to Bruce
            unixSocket.getOutputStream().write(datagram2);
        } catch (IOException x) {
            System.err.println("IOException on attempt to send to Bruce: " +
                x.getMessage());
            System.exit(1);
        } finally {
            if (unixSocket != null) {
                unixSocket.close();
            }
        }
    }
}
