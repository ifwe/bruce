package com.tagged.bruce.example_bruce_client;

import java.io.IOException;
import java.nio.charset.Charset;

import com.etsy.net.JUDS;
import com.etsy.net.UnixDomainSocketClient;

/**
 * example Bruce client
 *
 */
public class ExampleBruceClient 
{
    private static UnixDomainSocketClient unixSocket;

    public static void main( String[] args )
    {
        DatagramCreator dgc = new DatagramCreator();
        String topic = "blah";  // Kafka topic
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
        } catch (DatagramCreator.ErrorBase x) {
            System.err.println(
                    "Failed to create datagram because topic is too long");
            System.exit(1);
        }

        try {
            // create socket for sending to Bruce
            unixSocket = new UnixDomainSocketClient("/tmp/bruce_socket",
                    JUDS.SOCK_DGRAM);

            // send AnyPartition message to Bruce
            unixSocket.getOutputStream().write(datagram1);

            // send PartitionKey message to Bruce
            unixSocket.getOutputStream().write(datagram2);
        } catch (IOException x) {
            System.err.println("IOException on attempt to send to Bruce: " +
                x.getMessage());
        }
    }
}
