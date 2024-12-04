package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.nio.ByteBuffer;

public class Main
{
    public static void main(String[] args)
    {
        MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        //  mediaDriverContext.printConfigurationOnStart(true);
        MediaDriver driver = MediaDriver.launchEmbedded(mediaDriverContext);

        // Configure the Aeron context to use the embedded Media Driver
        Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(driver.aeronDirectoryName());

        // Define the URI and stream ID
        String inputUri = "aeron:udp?endpoint=localhost:40123";
        int streamId = 1;

        // Connect to the embedded Media Driver
        try (Aeron aeron = Aeron.connect(ctx))
        {
            // Create a publication and subscription
            try (Publication publication = aeron.addPublication(inputUri, streamId);
                 Subscription subscription = aeron.addSubscription(inputUri, streamId))
            {
                // Retrieve the resolved channel URI
                System.out.println("Resolved Channel URI: " + publication.channel());

                // Wait for the publication to be connected
                IdleStrategy idleStrategy = new SleepingIdleStrategy(100);
                System.out.println("Waiting for the publication to connect to subscribers...");
                while (!publication.isConnected())
                {
                    idleStrategy.idle();
                }
                System.out.println("Publication is now connected to subscribers.");

                // Send a message
                String message = "Hello, Aeron!";
                byte[] messageBytes = message.getBytes();
                UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(messageBytes.length));
                buffer.putBytes(0, messageBytes);

                // Publish the message
                long result;
                do
                {
                    result = publication.offer(buffer, 0, messageBytes.length);
                    if (result < 0)
                    {
                        System.out.println("Offer failed, retrying... Result: " + result);
                        idleStrategy.idle();
                    }
                }
                while (result < 0);

                System.out.println("Message sent successfully!");

                // Poll for messages in the subscription
                idleStrategy.reset();
                int fragmentsRead = 0;
                while (fragmentsRead == 0)
                {
                    fragmentsRead = subscription.poll((buffer1, offset, length, header) ->
                    {
                        byte[] data = new byte[length];
                        buffer1.getBytes(offset, data);
                        System.out.println("Received message: " + new String(data));
                    }, 1);
                    idleStrategy.idle(fragmentsRead);
                }
            }
        }
        catch (Exception e)
        {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        finally
        {
            driver.close();
        }
    }
}
