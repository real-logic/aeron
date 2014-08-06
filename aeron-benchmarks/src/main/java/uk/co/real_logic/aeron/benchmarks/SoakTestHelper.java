package uk.co.real_logic.aeron.benchmarks;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.DataHandler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.ByteOrder.nativeOrder;
import static uk.co.real_logic.aeron.common.CommonContext.ADMIN_DIR_PROP_NAME;
import static uk.co.real_logic.aeron.common.CommonContext.COUNTERS_DIR_PROP_NAME;
import static uk.co.real_logic.aeron.common.CommonContext.DATA_DIR_PROP_NAME;

/**
 * .
 */
public class SoakTestHelper
{
    public static final String CHANNEL = "udp://localhost:40123";
    public static final int STREAM_ID = 10;
    public static final int MESSAGES_PER_RUN = 10;

    public static void useSharedMemoryOnLinux()
    {
        // use shared memory to avoid Disk I/O bottleneck
        if ("Linux".equals(System.getProperty("os.name")))
        {
            System.setProperty(ADMIN_DIR_PROP_NAME, "/dev/shm/aeron/conductor");
            System.setProperty(COUNTERS_DIR_PROP_NAME, "/dev/shm/aeron/counters");
            System.setProperty(DATA_DIR_PROP_NAME, "/dev/shm/aeron/data");
        }
    }

    public static void exchangeMessagesBetweenClients(final Aeron publishingClient, final Aeron consumingClient, final AtomicBuffer publishingBuffer)
    {
        publishingBuffer.setMemory(0, publishingBuffer.capacity(), (byte) 0);

        try (final Publication publication = publishingClient.addPublication(CHANNEL, STREAM_ID, 0))
        {
            AtomicInteger receivedCount = new AtomicInteger(0);

            final DataHandler handler =
                (buffer, offset, length, sessionId, flags) ->
                {
                    final int expectedValue = receivedCount.get();
                    final int value = buffer.getInt(offset, nativeOrder());
                    if (value != expectedValue)
                    {
                        System.err.println("Unexpected value: " + value);
                    }
                    receivedCount.incrementAndGet();
                };

            try (final Subscription subscription = consumingClient.addSubscription(CHANNEL, STREAM_ID, handler))
            {
                for (int i = 0; i < MESSAGES_PER_RUN; i++)
                {
                    publishingBuffer.putInt(0, i, nativeOrder());
                    if (!publication.offer(publishingBuffer))
                    {
                        System.err.println("Unable to send " + i);
                    }
                }

                int read = 0;
                do
                {
                    read += subscription.poll(MESSAGES_PER_RUN);
                }
                while (read < MESSAGES_PER_RUN);

                if (read > MESSAGES_PER_RUN)
                {
                    System.err.println("We've read too many messages: " + read);
                }

                if (receivedCount.get() != MESSAGES_PER_RUN)
                {
                    System.err.println("Data Handler has " + read + " messages");
                }
            }
        }
    }
}
