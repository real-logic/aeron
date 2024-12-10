package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class Main
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40123|alias=banana";
    private static final int STREAM_ID = 10;

    public static void main(String[] args)
    {
        // Start the embedded MediaDriver
        MediaDriver.Context driverContext = new MediaDriver.Context();
        driverContext.conductorIdleStrategy(new SleepingIdleStrategy(10));
        driverContext.senderIdleStrategy(new SleepingIdleStrategy(10));
        driverContext.receiverIdleStrategy(new SleepingIdleStrategy(10));
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(driverContext);

        // Create the Aeron Context
        Aeron.Context aeronContext = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());

        try (Aeron aeron = Aeron.connect(aeronContext))
        {
            // Start a subscriber thread
            Thread subscriberThread = new Thread(() -> runSubscriber(aeron));
            subscriberThread.start();

            // Run the publisher in the main thread
            runPublisher(aeron);

            // Wait for subscriber thread to finish (it won't in this example)
            subscriberThread.join();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            mediaDriver.close();
        }
    }


    private static void runPublisher(Aeron aeron)
    {
        try (Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            // Allocate a direct ByteBuffer and wrap it in an UnsafeBuffer
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024);
            UnsafeBuffer unsafeBuffer = new UnsafeBuffer(directBuffer);

            for (int i = 0; i < 10; i++)
            {
                String message = "Hello, Aeron! Message #" + i;
                byte[] messageBytes = message.getBytes();

                // Put the message into the UnsafeBuffer
                unsafeBuffer.putBytes(0, messageBytes);

                System.out.println("Publishing: " + message);

                // Offer the buffer to the Aeron publication
                long result = publication.offer(unsafeBuffer, 0, messageBytes.length);
                if (result < 0)
                {
                    if (result == Publication.BACK_PRESSURED)
                    {
                        System.out.println("Offer failed due to back pressure");
                    }
                    else
                    {
                        System.out.println("Offer failed with result: " + result);
                    }
                }
                else
                {
                    System.out.println("Message sent successfully!");
                }

                Thread.sleep(100); // Simulate a delay between messages
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static void runSubscriber(Aeron aeron)
    {
        try (Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            System.out.println("Subscriber is ready and waiting for messages...");

            FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
            {
                byte[] data = new byte[length];
                buffer.getBytes(offset, data);
                System.out.println("Received: " + new String(data));
            };

            while (true)
            {
                int fragments = subscription.poll(fragmentHandler, 10);
                if (fragments == 0)
                {
                    Thread.yield(); // Avoid busy spinning
                }
            }
        }
    }
}

