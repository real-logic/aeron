package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;

public class Main {

    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40123";
    private static final int STREAM_ID = 1001;
    private static final String MESSAGE = "Hello, Aeron!";

    public static void main(String[] args) {
        // Start an embedded Media Driver
        MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverContext);

        Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());
        try (Aeron aeron = Aeron.connect(ctx)) {
            // Publisher thread
            Thread publisherThread = new Thread(() -> runPublisher(aeron), "Publisher");

            // Subscriber thread
            Thread subscriberThread = new Thread(() -> runSubscriber(aeron), "Subscriber");

            publisherThread.start();
            subscriberThread.start();

            publisherThread.join();
            subscriberThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } finally {
            CloseHelper.quietClose(mediaDriver);
        }
    }

    private static void runPublisher(Aeron aeron) {
        try (Publication publication = aeron.addPublication(CHANNEL, STREAM_ID)) {
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
            buffer.putStringWithoutLengthAscii(0, MESSAGE);

            while (!publication.isConnected()) {
                System.out.println("Waiting for subscriber to connect...");
                Thread.sleep(100);
            }

            long result;
            do {
                result = publication.offer(buffer);
                if (result < 0) {
                    System.out.println("Offer failed: " + result);
                    Thread.sleep(10);
                }
            } while (result < 0);

            System.out.println("Message published: " + MESSAGE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runSubscriber(Aeron aeron) {
        IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1, 1);
        try (Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID)) {
            while (!subscription.isConnected()) {
                System.out.println("Waiting for publisher to connect...");
                Thread.sleep(100);
            }

            while (true) {
                int fragmentsRead = subscription.poll((buffer, offset, length, header) -> {
                    byte[] data = new byte[length];
                    buffer.getBytes(offset, data);
                    String receivedMessage = new String(data, StandardCharsets.US_ASCII);
                    System.out.println("Received message: " + receivedMessage);
                }, 10);

                idleStrategy.idle(fragmentsRead);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

